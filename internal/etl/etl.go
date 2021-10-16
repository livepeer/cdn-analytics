package etl

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/livepeer/cdn-log-analytics/internal/common"
	"github.com/livepeer/cdn-log-analytics/internal/config"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
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
		ctx      context.Context
		cfg      *config.Config
		gsClient *storage.Client
		bucket   string
		staging  bool
	}
)

var (
	errEmpty = errors.New("empty")
)

func NewEtl(ctx context.Context, cfg *config.Config, bucket, credentials string, staging bool) (*Etl, error) {
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

	etl := &Etl{
		ctx:      ctx,
		bucket:   bucket,
		gsClient: client,
		cfg:      cfg,
		staging:  staging,
	}
	return etl, nil
}

func (etl *Etl) Do() error {
	// list top-level dirs. Each dir corresponds to StackPath's 'site'
	topDirs, _, err := etl.getGSDirs("/", "")
	if err != nil {
		return err
	}
	glog.V(common.VVERBOSE).Infof("Got top dirs in GS: %+v", topDirs)
	glog.Infof("=>> fi %s", cleanLastSlash(topDirs[0]))
	for _, siteHash := range topDirs {
		cleanSiteHash := cleanLastSlash(siteHash)
		if regionName, ok := etl.cfg.Names[cleanSiteHash]; ok {
			isStagingRegion := strings.HasSuffix(regionName, "-monster")
			if etl.staging && !isStagingRegion {
				continue
			}
			glog.Infof("For hash %q found region %q", cleanSiteHash, regionName)
			// etl.getGSDirs("", siteHash+"cds/")
			startHour, err := etl.getStartHour(cleanSiteHash)
			if err != nil {
				glog.Errorf("Error getting start hour err=%v", err)
				continue
			}
			glog.V(common.DEBUG).Infof("Start hour is %s", startHour)
			err = etl.doEtl(cleanSiteHash, startHour)
			if err != nil {
				glog.Errorf("Error processing data for siteHash=%s region=%s err=%v", cleanSiteHash, regionName, err)
			}
			// break
		}
	}

	return nil
}

// doEtl reads logs files that corresponds to startHour, aggregates it and
// pushes aggreagated data to Livepeer API
func (etl *Etl) doEtl(siteHash string, startHour time.Time) error {
	for {
		endHour := startHour.Add(aggregationDuration)
		if endHour.After(time.Now()) {
			break
		}
		err := etl.doEtlHour(siteHash, startHour)
		if err != nil {
			return err
		}
		startHour = startHour.Add(aggregationDuration)
		// break
	}
	return nil
}

func (etl *Etl) doEtlHour(siteHash string, startHour time.Time) error {
	regionName := etl.cfg.Names[siteHash]
	endHour := startHour.Add(aggregationDuration)
	started := time.Now()
	glog.Infof("Start processing data for hash=%s region=%s startHour=%s endHour=%s", siteHash, regionName, startHour, endHour)
	// get list of all files that needs to be processed
	query := storage.Query{
		StartOffset: constructFileNameFromTime(siteHash, startHour),
		EndOffset:   constructFileNameFromTime(siteHash, endHour),
	}
	glog.V(common.INSANE).Infof("Query %+v", query)
	_, fileNames, err := etl.getGSDirsWithQuery(&query, -1)
	if err != nil {
		return err
	}
	if len(fileNames) == 0 {
		return fmt.Errorf("no logs files found for hash=%s region=%s startHour=%s", siteHash, regionName, startHour)
	}
	glog.V(common.INSANE).Infof("Got files=%+v", fileNames)
	// now check that there is logs files exists in the next hour
	// (to make sure that all the files for startHour made their was to GS)
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
	agg := newAggregator(etl.ctx, etl.gsClient, etl.bucket)

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
	export := agg.flatten(regionName, startHour)
	if err = agg.postToAPI(export); err != nil {
		glog.Errorf("Error posting data to api region=%s hour=%s err=%v", regionName, startHour, err)
	}

	return nil
}

func constructFileNameFromTime(siteHash string, tm time.Time) string {
	return fmt.Sprintf("%s/cds/%s", siteHash, tm.Format("2006/01/02/cds_20060102-150405"))
}

func (etl *Etl) getStartHour(siteHash string) (time.Time, error) {
	const askedAPI = false
	if askedAPI {
		// todo: get last processed hour from API, add one hour
		return time.Now(), nil
	}
	ts, err := etl.getTimestampFromFirstFile(siteHash)
	if err != nil {
		return time.Now(), err
	}
	return ts.Truncate(aggregationDuration), nil
}

func (etl *Etl) getTimestampFromFirstFile(siteHash string) (time.Time, error) {
	_, fns, err := etl.getGSDirsWithLimit("", siteHash+"/cds/", 1)
	rt := time.Now()
	if err != nil {
		return rt, err
	}
	if len(fns) != 1 {
		return rt, errEmpty
	}
	_, fileName := filepath.Split(fns[0])
	glog.Infof("Got first file for site %s file %s (name=%s)", siteHash, fns[0], fileName)
	fileName = strings.TrimPrefix(fileName, "cds_")
	fnp := strings.Split(fileName, "-")
	if len(fnp) < 2 {
		return rt, errors.New("invalid file name")
	}

	tm, err := time.Parse("20060102150405", fnp[0]+fnp[1])
	if err != nil {
		return rt, err
	}
	glog.V(common.VVERBOSE).Infof("From fileName=%s parsed time=%s", fileName, tm)
	// var re1 *regexp.Regexp = regexp.MustCompile(`cds_(\d{4})20210828-222(w\d+)_`)
	// ms1 := re1.FindStringSubmatch(fileName)
	// glog.Infof("Got regexp matches: %+v", ms1)

	return tm, nil
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
