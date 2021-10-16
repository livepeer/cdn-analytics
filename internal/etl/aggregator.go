package etl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/livepeer/cdn-log-analytics/internal/common"
	"github.com/livepeer/cdn-log-analytics/internal/utils"
)

type (
	VideoStats struct {
		IPs           []string `json:"i_ps,omitempty"`
		TotalFilesize int64    `json:"total_filesize,omitempty"`
		TotalCsBytes  int64    `json:"total_cs_bytes,omitempty"`
		TotalScBytes  int64    `json:"total_sc_bytes,omitempty"`
		Count         int      `json:"count,omitempty"`
	}

	VideoStatsExt struct {
		StreamID      string `json:"stream_id"`
		PlaybackID    string `json:"playback_id"`
		UniqueUsers   int    `json:"unique_users"`
		TotalFilesize int64  `json:"total_filesize"`
		TotalCsBytes  int64  `json:"total_cs_bytes"`
		TotalScBytes  int64  `json:"total_sc_bytes"`
		Count         int    `json:"count"`
	}

	SendData struct {
		Date   int64            `json:"date"` // hour in Unix epoch
		Region string           `json:"region"`
		Data   []*VideoStatsExt `json:"data"`
	}

	VideoStat struct {
		date     string
		streamId string
		itemType utils.IDType
		IP       string
		Filesize int64
		CsBytes  int64
		ScBytes  int64
		httpCode string
	}

	aggregator struct {
		ctx          context.Context
		cancel       context.CancelFunc
		gsClient     *storage.Client
		bucket       string
		data         map[string]map[utils.IDType]map[string]map[string]*VideoStats
		otherTraffic int64 // traffic sent from CDN to clients not related to video streaming
	}
)

func newAggregator(gctx context.Context, gsClient *storage.Client, bucket string) *aggregator {
	ctx, cancel := context.WithCancel(gctx)
	return &aggregator{
		ctx:      ctx,
		cancel:   cancel,
		gsClient: gsClient,
		bucket:   bucket,
		data:     make(map[string]map[utils.IDType]map[string]map[string]*VideoStats), // date:IdType:streamId:httpCode
	}
}

func (ag *aggregator) incomingDataLoop(doneC chan struct{}, c chan VideoStat) {
	for chainVideoStat := range c {
		if chainVideoStat.httpCode == "other" {
			ag.otherTraffic += chainVideoStat.ScBytes
			continue
		}
		if chainVideoStat.httpCode == "-" {
			// StackPath sometimes return '-' instead of correct HTTP response code.
			// In that case ScBytes in 0, so just skipping
			continue
		}
		// treat all codes in the same way
		chainVideoStat.httpCode = "200"
		glog.Infof("~~~ inserting line for date %s stream id %s", chainVideoStat.date, chainVideoStat.streamId)
		byDate := ag.data[chainVideoStat.date]
		if byDate == nil {
			byDate = make(map[utils.IDType]map[string]map[string]*VideoStats)
			ag.data[chainVideoStat.date] = byDate
		}
		byType := byDate[chainVideoStat.itemType]
		if byType == nil {
			byType = make(map[string]map[string]*VideoStats)
			byDate[chainVideoStat.itemType] = byType
		}
		byStreamID := byType[chainVideoStat.streamId]
		if byStreamID == nil {
			byStreamID = make(map[string]*VideoStats)
			byType[chainVideoStat.streamId] = byStreamID
		}
		if stats, ok := byStreamID[chainVideoStat.httpCode]; ok {
			if !utils.Includes(stats.IPs, chainVideoStat.IP) {
				stats.IPs = append(stats.IPs, chainVideoStat.IP)
			}
			stats.Count++
			stats.TotalFilesize += chainVideoStat.Filesize
			stats.TotalCsBytes += chainVideoStat.CsBytes
			stats.TotalScBytes += chainVideoStat.ScBytes
		} else {
			byStreamID[chainVideoStat.httpCode] = &VideoStats{
				IPs:           []string{chainVideoStat.IP},
				TotalFilesize: chainVideoStat.Filesize,
				TotalCsBytes:  chainVideoStat.CsBytes,
				TotalScBytes:  chainVideoStat.ScBytes,
				Count:         1,
			}
		}
	}
	doneC <- struct{}{}
}

func (ag *aggregator) flatten(region string, startHour time.Time) *SendData {
	sd := &SendData{
		Region: region,
		Date:   startHour.Unix(),
	}
	for date, val := range ag.data {
		glog.Infof("--> date: %s", date)
		for itemType, val1 := range val {
			glog.Infof("--> item type %q", itemType)
			for stream, val2 := range val1 {
				glog.Infof("## stream %s", stream)
				for status, details := range val2 {
					// glog.Infof("---> status: %s", status)
					if status != "200" {
						panic("xstop")
					}
					if stream == "bad0aqlrxtr9cvuv" {
						glog.Infof("-> itemType %s stream %s dat %+v", itemType, stream, details)
					}
					vstat := &VideoStatsExt{
						Count:         details.Count,
						TotalFilesize: details.TotalFilesize,
						TotalCsBytes:  details.TotalCsBytes,
						TotalScBytes:  details.TotalScBytes,
						UniqueUsers:   len(details.IPs),
					}
					switch itemType {
					case utils.IDTypeManifestID:
						vstat.PlaybackID = stream
					case utils.IDTypeStreamID:
						vstat.StreamID = stream
					default:
						panic("shouldn't happen")
					}
					sd.Data = append(sd.Data, vstat)

				}
			}
		}
	}
	return sd
}

const httpTimeout = 4 * time.Second
const setActiveTimeout = 1500 * time.Millisecond

var defaultHTTPClient = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	// Transport: &http2.Transport{AllowHTTP: true},
	Timeout: httpTimeout,
}

func (ag *aggregator) postToAPI(data *SendData) error {
	uri := fmt.Sprintf("http://localhost:3004/api/cdn-data")
	bin, err := json.Marshal(data)
	if err != nil {
		glog.Errorf("Error mashalling SendData err=%v", err)
		return err
	}

	req, err := http.NewRequest("POST", uri, bytes.NewBuffer(bin))
	if err != nil {
		return err
	}
	// req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	req.Header.Add("Content-Type", "application/json")
	resp, err := defaultHTTPClient.Do(req)
	if err != nil {
		panic("stop1")
		return err
	}
	defer resp.Body.Close()
	bin, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	glog.Infof("SendData post response=%s", string(bin))
	if resp.StatusCode != http.StatusOK {
		panic("stop")
	}
	return nil
}

func (ag *aggregator) aggregate(region string) {
	glog.V(common.DEBUG).Infof("Create output file region=%s", region)
	// print results
	// file, err := os.OpenFile("zsup.csv", os.O_CREATE|os.O_WRONLY, 0644)
	file, err := os.OpenFile("zsup3.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	if err != nil {
		panic(fmt.Errorf("failed creating file: %s", err))
		// return fmt.Errorf("failed creating file: %s", err)
	}
	defer file.Close()

	datawriter := bufio.NewWriter(file)
	bufString := getCsvHeader()
	datawriter.WriteString(fmt.Sprintf("region=%s\n", region))
	_, err = datawriter.WriteString(bufString + "\n")
	if err != nil {
		panic(err)
	}

	format := "csv"
	for date, val := range ag.data {
		for itemType, val1 := range val {
			for stream, val2 := range val1 {
				for httpCode, details := range val2 {
					bufString := ""
					switch format {
					case "csv":
						switch itemType {
						case "manifest_id":
							bufString = getCsvLine(date, "", stream, "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScBytes, details.TotalFilesize, httpCode)
						case "stream_id":
							bufString = getCsvLine(date, stream, "", "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScBytes, details.TotalFilesize, httpCode)
						case "stream_name":
							bufString = getCsvLine(date, "", "", stream, len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScBytes, details.TotalFilesize, httpCode)
						default:
						}

					case "sql":
						// switch itemType {
						// case "manifest_id":
						// 	bufString = getSqlLine(date, "", stream, "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, itemType, httpCode)
						// case "stream_id":
						// 	bufString = getSqlLine(date, stream, "", "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, itemType, httpCode)
						// case "stream_name":
						// 	bufString = getSqlLine(date, "", "", stream, len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, itemType, httpCode)
						// default:
						// }
					default:
						// return fmt.Errorf("invalid output format %s, valid format are csv and sql", format)
					}

					_, err = datawriter.WriteString(bufString + "\n")

					if err != nil {
						panic(fmt.Errorf("failed writing line %s to file: %s", bufString, err))
						// return fmt.Errorf("failed writing line %s to file: %s", bufString, err)
					}
				}
			}
		}
	}
	datawriter.Flush()
}

func (ag *aggregator) Done() <-chan struct{} {
	return ag.ctx.Done()
}

func (ag *aggregator) parseFileWorker(fileNameChan chan string, doneC chan struct{}, c chan VideoStat) {
	for fileName := range fileNameChan {
		glog.V(common.DEBUG).Infof("Got file=%s to process", fileName)
		err := parseFile(ag.ctx, ag.gsClient, ag.bucket, fileName, c)
		if err != nil {
			glog.Errorf("Error processing file=%s err=%v", fileName, err)
		}
	}
	doneC <- struct{}{}
}

func parseFile(ctx context.Context, gsClient *storage.Client, bucket, file string, c chan VideoStat) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	defer cancel()
	started := time.Now()
	defer func(s time.Time) {
		glog.V(common.VERBOSE).Infof("End parsing file bucket=%s file=%s took=%s", bucket, file, time.Since(s))
	}(started)

	rc, err := gsClient.Bucket(bucket).Object(file).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %v", file, err)
	}
	defer rc.Close()

	reader, err := gzip.NewReader(rc)
	if err != nil {
		return err
	}
	defer reader.Close()

	contents := bufio.NewScanner(reader)
	for contents.Scan() {
		line := contents.Text()
		if utils.IsCommentLine(line) || utils.IsEmptyLine(line) {
			continue
		}
		parseLine(line, c)
	}
	return contents.Err()
}

func parseLine(line string, c chan VideoStat) {
	toks := strings.Split(line, "\t")
	glog.Infof("Parsing line:%s", line)

	if len(toks) < 17 {
		glog.V(common.DEBUG).Infof("Warning: line is not following the log standard. line=%q", line)
		return
	}

	date := toks[0]
	fileSize := toks[7]
	csBytes := toks[8]
	scBytes := toks[9]
	url := toks[14]
	// add hour
	date += strings.Split(toks[1], ":")[0]

	streamId, streamType, err := utils.GetStreamId(url)
	if err != nil {
		glog.V(common.VVERBOSE).Infof("Warning: invalid URL format: '%s'. line=%q", url, line)
		scBytesInt, err := strconv.ParseInt(scBytes, 10, 64)
		if err != nil {
			return
		}
		c <- VideoStat{
			httpCode: "other",
			ScBytes:  scBytesInt,
		}
		return
	}

	if date == "" || streamId == "" {
		glog.Warningf("Warning: Invalid line: %s", line)
	}

	csBytesInt, err := strconv.ParseInt(csBytes, 10, 64)
	if err != nil {
		glog.Warningf("Error: invalid int conversion format: '%s'", csBytes)
	}

	scBytesInt, err := strconv.ParseInt(scBytes, 10, 64)
	if err != nil {
		glog.Warningf("Error: invalid int conversion format: '%s'", scBytes)
	}

	fileSizeInt, err := strconv.ParseInt(fileSize, 10, 64)
	if err != nil {
		glog.Warningf("Error: invalid int conversion format: '%s'", fileSize)
	}
	var tempVideoStat VideoStat
	tempVideoStat.IP = toks[3]
	tempVideoStat.Filesize = fileSizeInt
	tempVideoStat.CsBytes = csBytesInt
	tempVideoStat.ScBytes = scBytesInt
	tempVideoStat.date = date
	tempVideoStat.streamId = streamId
	tempVideoStat.itemType = streamType
	tempVideoStat.httpCode = toks[12]
	// if tempVideoStat.httpCode == "-" {
	// 	glog.Infof("==============> %q", line)
	// }

	c <- tempVideoStat
}

func getCsvLine(date string, streamId string, manifestId string, manifestName string, countUniqueIPs int, contIPs int, totalCsBytes int64, totalScyBytes int64, totalFilesize int64, httpCode string) string {
	return fmt.Sprintf("%s,%s,%s,%s,%d,%d,%d,%d,%d,%s", date, streamId, manifestId, manifestName, countUniqueIPs, contIPs, totalCsBytes, totalScyBytes, totalFilesize, httpCode)
}

func getCsvHeader() string {
	return "date,stream_id,manifest_id,stream_name,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size,httpCode"
}
