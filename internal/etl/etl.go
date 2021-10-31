package etl

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/livepeer/cdn-log-puller/internal/common"
	"github.com/livepeer/cdn-log-puller/internal/config"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	ErrForbidden = errors.New("forbidden")
)

const (
	aggregationDuration = time.Hour
	numParralelReaders  = 10
)

type (
	// Etl loads CDN usage data from files from GS bucket,
	// aggregates them by hour and sends to Livepeer API
	// to be put into database
	Etl struct {
		ctx            context.Context
		cfg            *config.Config
		gsClient       *storage.Client
		bucket         string
		staging        bool
		livepeerAPIKey string
		livepeerAPIUrl string
	}
)

var (
	errEmpty = errors.New("empty")
)

func NewEtl(ctx context.Context, cfg *config.Config, bucket, credentials string, staging bool,
	livepeerAPIKey string, livepeerAPIUrl *url.URL) (*Etl, error) {

	var opts []option.ClientOption
	if credentials != "" {
		opts = append(opts, option.WithCredentialsFile(credentials))
	} else {
		opts = append(opts, option.WithoutAuthentication())
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	// defer client.Close()

	// Check GS access rights
	rctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	it := client.Bucket(bucket).Objects(rctx, nil)
	if _, err = it.Next(); err != nil {
		return nil, err
	}
	livepeerAPIUrl.Path = ""
	lau := livepeerAPIUrl.String()

	etl := &Etl{
		ctx:            ctx,
		bucket:         bucket,
		gsClient:       client,
		cfg:            cfg,
		staging:        staging,
		livepeerAPIKey: livepeerAPIKey,
		livepeerAPIUrl: lau,
	}
	return etl, nil
}

// used for debugging
func (etl *Etl) printFile(file string) {
	fmt.Printf("Printing file %s\n", file)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
	defer cancel()
	rc, err := etl.gsClient.Bucket(etl.bucket).Object(file).NewReader(ctx)
	if err != nil {
		panic(fmt.Errorf("Object(%q).NewReader: %v", file, err))
	}
	defer rc.Close()

	reader, err := gzip.NewReader(rc)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	contents := bufio.NewScanner(reader)
	for contents.Scan() {
		line := contents.Text()
		fmt.Println(line)
	}
}

func (etl *Etl) Do() error {
	// list top-level dirs. Each dir corresponds to StackPath's 'site'
	topDirs, _, err := etl.getGSDirs("/", "")
	if err != nil {
		return err
	}
	glog.V(common.VVERBOSE).Infof("Got top dirs in GS: %+v", topDirs)
	/*
		glog.Infof("\n%s", strings.Join(topDirs, "\n"))
		// tempDirs, tempFiles, err := etl.getGSDirs("/", "k3c3y8z2/")
		// tempDirs, tempFiles, err := etl.getGSDirs("/", "k3c3y8z2/cds/2021/10/24/")
		tempDirs, tempFiles, err := etl.getGSDirs("/", "t8a6c4p8/cds/2021/10/24/")
		glog.Infof("Got temp dirs: %+v", tempDirs)
		glog.Infof("Got temp files: %+v", tempFiles)
		if len(tempFiles) > 0 {
			// etl.printFile(tempFiles[0])
			for _, fn := range tempFiles {
				etl.printFile(fn)
			}
		}
		panic("stop")
	*/

	for _, siteHash := range topDirs {
		cleanSiteHash := cleanLastSlash(siteHash)
		if regionName, ok := etl.cfg.Names[cleanSiteHash]; ok {
			isStagingRegion := strings.HasSuffix(regionName, "-monster")
			if etl.staging && !isStagingRegion {
				continue
			} else if !etl.staging && isStagingRegion {
				continue
			}
			glog.Infof("For hash %q found region %q", cleanSiteHash, regionName)
			// etl.getGSDirs("", siteHash+"cds/")
			startHour, fullFileName, err := etl.getStartHour(cleanSiteHash, regionName)
			glog.V(common.INSANE).Infof("--> start hour %s startFile=%s", startHour, fullFileName)
			if err == ErrForbidden {
				return err
			}

			if err != nil {
				glog.Errorf("Error getting start hour err=%v", err)
				return err
				// continue
			}
			glog.V(common.DEBUG).Infof("Start hour is %s, start fileName=%s", startHour, fullFileName)
			err = etl.doEtl(cleanSiteHash, startHour, fullFileName)
			if err != nil {
				glog.Errorf("Error processing data for siteHash=%s region=%s err=%v", cleanSiteHash, regionName, err)
				return err
			}
			// break
		}
	}

	return nil
}

// doEtl reads logs files that corresponds to startHour, aggregates it and
// pushes aggreagated data to Livepeer API
func (etl *Etl) doEtl(siteHash string, startHour time.Time, startFile string) error {
	for {
		endHour := startHour.Add(aggregationDuration)
		if endHour.After(time.Now()) {
			break
		}
		err := etl.doEtlHour(siteHash, startHour, startFile)
		if err != nil && err != errEmpty {
			return err
		}
		startFile = ""
		startHour = startHour.Add(aggregationDuration)
		// break
	}
	return nil
}

func (etl *Etl) doEtlHour(siteHash string, startHour time.Time, startFile string) error {
	regionName := etl.cfg.Names[siteHash]
	endHour := startHour.Add(aggregationDuration)
	started := time.Now()
	glog.Infof("Start processing data for hash=%s region=%s startHour=%s endHour=%s startFile=%s", siteHash, regionName, startHour, endHour, startFile)
	// get list of all files that needs to be processed
	query := storage.Query{
		EndOffset: constructFileNameFromTime(siteHash, endHour),
	}
	if startFile != "" {
		query.StartOffset = startFile
	} else {
		query.StartOffset = constructFileNameFromTime(siteHash, startHour)
	}
	glog.V(common.INSANE).Infof("Query %+v", query)
	_, fileNames, err := etl.getGSDirsWithQuery(&query, -1)
	if err != nil {
		return err
	}
	if len(fileNames) == 0 {
		glog.Errorf("no logs files found for hash=%s region=%s startHour=%s", siteHash, regionName, startHour)
		return errEmpty
	}
	glog.V(common.INSANE).Infof("Got files=%+v", fileNames)
	// now check that there is logs files exists in the next hour
	// (to make sure that all the files for startHour made their was to GS)
	/*
		query = storage.Query{
			StartOffset: constructFileNameFromTime(siteHash, endHour),
		}
		glog.V(common.INSANE).Infof("Query %+v", query)
		_, endHourFileNames, err := etl.getGSDirsWithQuery(&query, 1)
		if err != nil {
			return err
		}
		if len(endHourFileNames) == 0 {
			return fmt.Errorf("no logs files found for hour=%s", endHour)
		}
	*/
	agg := newAggregator(etl.ctx, etl.gsClient, etl.bucket, etl.livepeerAPIKey, etl.livepeerAPIUrl)

	datac := make(chan VideoStat)
	filesChan := make(chan string, 32)
	doneChan := make(chan struct{})

	go agg.incomingDataLoop(doneChan, datac)
	for i := 0; i < numParralelReaders; i++ {
		go agg.parseFileWorker(filesChan, doneChan, datac)
	}
	for _, fname := range fileNames {
		filesChan <- fname
	}
	close(filesChan)
	for i := 0; i < numParralelReaders; i++ {
		<-doneChan
	}
	close(datac)
	<-doneChan
	// data processing complete
	glog.Infof("Extract and transform of bucket=%s region=%s hour=%s complete in %s other traffic=%d bytes.",
		etl.bucket, regionName, startHour, time.Since(started), agg.otherTraffic)
	// agg.aggregate(regionName)
	glog.V(common.DEBUG).Infof("Parsed %d days (%+v)", len(agg.data), agg.data)
	if len(agg.data) == 0 {
		return nil
	}
	export := agg.flatten(regionName, startHour, fileNames[len(fileNames)-1])
	if err = agg.postToAPI(export); err != nil {
		glog.Errorf("Error posting data to api region=%s hour=%s err=%v", regionName, startHour, err)
	}

	return err
}

func constructFileNameFromTime(siteHash string, tm time.Time) string {
	return fmt.Sprintf("%s/cds/%s", siteHash, tm.Format("2006/01/02/cds_20060102-150405"))
}

type regionResp struct {
	FileName string `json:"fileName,omitempty"`
	Region   string `json:"region,omitempty"`
}

func (etl *Etl) getFileFromAPI(region string) (string, error) {
	uri := fmt.Sprintf("%s/api/cdn-data/region/%s", etl.livepeerAPIUrl, region)

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Authorization", "Bearer "+etl.livepeerAPIKey)
	req.Header.Add("Content-Type", "application/json")
	resp, err := defaultHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bin, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	glog.Infof("Get region get response=%s", string(bin))
	if resp.StatusCode == http.StatusNoContent {
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusForbidden {
			return "", ErrForbidden
		}
		panic("stop")
	}
	rr := &regionResp{}
	if err = json.Unmarshal(bin, rr); err != nil {
		return "", err
	}
	return rr.FileName, nil
}

func (etl *Etl) getStartHour(siteHash, regionName string) (time.Time, string, error) {
	fullFileName, err := etl.getFileFromAPI(regionName)
	if err != nil {
		glog.Errorf("Error contacting API err=%v", err)
		var tz time.Time
		return tz, "", err
	}
	if fullFileName != "" {
		tm, fileName, err := getTimeFromFullFileName(fullFileName, regionName)
		glog.V(common.DEBUG).Infof("Got from API for region=%s fileName=%s time=%s", regionName, fileName, tm)
		// if err != nil {
		return tm.Truncate(aggregationDuration), fullFileName, err
		// }
	}
	ts, fullFileName, err := etl.getTimestampFromFirstFile(siteHash)
	if err != nil {
		return time.Now(), "", err
	}
	return ts.Truncate(aggregationDuration), fullFileName, nil
}

func getTimeFromFullFileName(fullFileName, siteHash string) (time.Time, string, error) {
	_, fileName := filepath.Split(fullFileName)
	glog.Infof("Got first file for site %s file %s (name=%s)", siteHash, fullFileName, fileName)
	fileName = strings.TrimPrefix(fileName, "cds_")
	fnp := strings.Split(fileName, "-")
	var tm time.Time
	var err error
	if len(fnp) < 2 {
		return tm, fileName, errors.New("invalid file name")
	}

	tm, err = time.Parse("20060102150405", fnp[0]+fnp[1])
	return tm, fileName, err

}

func (etl *Etl) getTimestampFromFirstFile(siteHash string) (time.Time, string, error) {
	_, fns, err := etl.getGSDirsWithLimit("", siteHash+"/cds/", 1)
	rt := time.Now()
	if err != nil {
		return rt, "", err
	}
	if len(fns) != 1 {
		return rt, "", errEmpty
	}
	fullFileName := fns[0]
	tm, fileName, err := getTimeFromFullFileName(fullFileName, siteHash)

	if err != nil {
		return rt, fullFileName, err
	}
	glog.V(common.VVERBOSE).Infof("From fileName=%s parsed time=%s", fileName, tm)
	// var re1 *regexp.Regexp = regexp.MustCompile(`cds_(\d{4})20210828-222(w\d+)_`)
	// ms1 := re1.FindStringSubmatch(fileName)
	// glog.Infof("Got regexp matches: %+v", ms1)

	return tm, fullFileName, nil
}

func (etl *Etl) getGSDirs(delimiter, prefix string) ([]string, []string, error) {
	return etl.getGSDirsWithLimit(delimiter, prefix, -1)
}

func (etl *Etl) getGSDirsWithLimit(delimiter, prefix string, limit int) ([]string, []string, error) {
	query := storage.Query{
		Delimiter: delimiter,
		Prefix:    prefix,
	}
	return etl.getGSDirsWithQuery(&query, limit)
}

func (etl *Etl) getGSDirsWithQuery(query *storage.Query, limit int) ([]string, []string, error) {
	ctx, cancel := context.WithTimeout(etl.ctx, time.Second*15)
	// ctx, cancel := context.WithCancel(etl.ctx)
	defer cancel()
	it := etl.gsClient.Bucket(etl.bucket).Objects(ctx, query)
	var dirNames, filesNames []string
	var count int
	for {
		fi, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			glog.Errorf("Returning err=%v", err)
			return nil, nil, err
		}
		if limit > 0 && count >= limit {
			break
		}
		glog.V(common.INSANE2).Infof("==> Got file name=%q prefix=%q", fi.Name, fi.Prefix)
		if fi.Prefix != "" && fi.Name == "" {
			dirNames = append(dirNames, fi.Prefix)
			count++
		} else if fi.Prefix == "" && fi.Name != "" {
			filesNames = append(filesNames, fi.Name)
			count++
		}
	}
	return dirNames, filesNames, nil
}

func cleanLastSlash(val string) string {
	if len(val) > 0 && val[len(val)-1] == '/' {
		return val[:len(val)-1]
	}
	return val
}
