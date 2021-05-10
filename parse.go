package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/profile"
)

type VideoStats struct {
	IPs           []string
	TotalFilesize int64
	TotalCsBytes  int64
	TotalScyBytes int64
	Count         int
}

type VideoStat struct {
	date     string
	streamId string
	itemType string
	IP       string
	Filesize int64
	CsBytes  int64
	ScyBytes int64
	httpCode string
}

const (
	fieldSeparator = ","
	topLoad        = 10
)

func validateParseParameters(folder string, output string, format string) error {
	// check if folder is a valid path
	if _, err := os.Stat(folder); err != nil {
		return fmt.Errorf("%s is an invalid path. Error: %+v", folder, err)
	}

	// check if output path is valid
	dir := filepath.Dir(output)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("%s is an invalid folder. Error: %+v", dir, err)
	}

	if !(format == "csv" || format == "sql") {
		return fmt.Errorf("Invalid format %s. Valid format are csv and sql.", format)
	}

	return nil
}

func parseFiles(folder string, output string, format string) error {
	defer profile.Start(profile.MemProfile).Stop()

	arrDetails := make(map[string]map[string]map[string]map[string]*VideoStats)
	// get file list
	var wg sync.WaitGroup
	c := make(chan VideoStat)

	go func() {
		for chainVideoStat := range c {
			var tempVideoStat VideoStats
			if arrDetails[chainVideoStat.date] == nil {
				arrDetails[chainVideoStat.date] = make(map[string]map[string]map[string]*VideoStats)
			}
			if arrDetails[chainVideoStat.date][chainVideoStat.itemType] == nil {
				arrDetails[chainVideoStat.date][chainVideoStat.itemType] = make(map[string]map[string]*VideoStats)
			}
			if arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId] == nil {
				arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId] = make(map[string]*VideoStats)
			}

			if arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode] != nil {
				if !find(arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode].IPs, chainVideoStat.IP) {
					tempVideoStat.IPs = append(arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode].IPs, chainVideoStat.IP)
				} else {
					tempVideoStat.IPs = arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode].IPs
				}
				tempVideoStat.Count = arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode].Count + 1
				tempVideoStat.TotalFilesize = arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode].TotalFilesize + chainVideoStat.Filesize
				tempVideoStat.TotalCsBytes = arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode].TotalCsBytes + chainVideoStat.CsBytes
				tempVideoStat.TotalScyBytes = arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode].TotalScyBytes + chainVideoStat.ScyBytes
			} else {
				tempVideoStat.IPs = []string{chainVideoStat.IP}
				tempVideoStat.TotalFilesize = chainVideoStat.Filesize
				tempVideoStat.TotalCsBytes = chainVideoStat.CsBytes
				tempVideoStat.TotalScyBytes = chainVideoStat.ScyBytes
				tempVideoStat.Count = 1
			}
			arrDetails[chainVideoStat.date][chainVideoStat.itemType][chainVideoStat.streamId][chainVideoStat.httpCode] = &tempVideoStat
		}
	}()

	err := filepath.Walk(folder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if isValidFile(path) {
				wg.Add(1)
				go func() {
					if verbose {
						log.Println("Parse file: ", path)
					}
					err = parseFile(path, c)

					if verbose {
						log.Println("End parse file: ", path)
					}
					wg.Done()
				}()
			}

			if err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		return err
	}
	log.Println("Wait for goroutine to finish")

	wg.Wait()
	close(c)
	log.Println("Create output file")
	// print results
	file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()

	if err != nil {
		return fmt.Errorf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)

	bufString := ""
	switch format {
	case "csv":
		bufString = getCsvHeader()
	case "sql":
		bufString = getSqlHeader()
	default:
		return fmt.Errorf("Invalid output format %s, valid format are csv and sql.", format)
	}
	_, err = datawriter.WriteString(bufString + "\n")

	if err != nil {
		return fmt.Errorf("failed writing line %s to file: %s", bufString, err)
	}

	for date, val := range arrDetails {
		for itemType, val1 := range val {
			for stream, val2 := range val1 {
				for httpCode, details := range val2 {
					bufString := ""
					switch format {
					case "csv":
						switch itemType {
						case "manifest_id":
							bufString = getCsvLine(date, "", stream, "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, httpCode)
						case "stream_id":
							bufString = getCsvLine(date, stream, "", "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, httpCode)
						case "stream_name":
							bufString = getCsvLine(date, "", "", stream, len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, httpCode)
						default:
						}

					case "sql":
						switch itemType {
						case "manifest_id":
							bufString = getSqlLine(date, "", stream, "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, itemType, httpCode)
						case "stream_id":
							bufString = getSqlLine(date, stream, "", "", len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, itemType, httpCode)
						case "stream_name":
							bufString = getSqlLine(date, "", "", stream, len(details.IPs), details.Count, details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize, itemType, httpCode)
						default:
						}
					default:
						return fmt.Errorf("Invalid output format %s, valid format are csv and sql.", format)
					}

					_, err = datawriter.WriteString(bufString + "\n")

					if err != nil {
						return fmt.Errorf("failed writing line %s to file: %s", bufString, err)
					}
				}
			}
		}
	}
	datawriter.Flush()

	return nil
}

func parseFile(file string, c chan VideoStat) error {
	// Create new reader to decompress gzip.
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	reader, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer reader.Close()

	contents := bufio.NewScanner(reader)
	for contents.Scan() {
		if isCommentLine(contents.Text()) || isEmptyLine(contents.Text()) {
			continue
		}

		parseLine(contents.Text(), c)
	}
	f.Close()
	if verbose {
		log.Println("End file: ", file)
	}
	return nil
}

func parseLine(line string, c chan VideoStat) {
	toks := strings.Split(line, "\t")

	if len(toks) < 17 {
		if verbose {
			log.Printf("Warning: line is not following the log standard. '%s'", line)
		}
		return
	}

	date := toks[0]
	fileSize := toks[7]
	csBytes := toks[8]
	scBytes := toks[9]
	url := toks[14]

	streamId, streamType, err := getStreamId(url)
	if err != nil {
		log.Printf("Warning: invalid URL format: '%s'.", url)
		return
	}

	if date == "" || streamId == "" {
		log.Printf("Warning: Invalid line: %s", line)
	}

	csBytesInt, err := strconv.ParseInt(csBytes, 10, 64)
	if err != nil {
		log.Printf("Error: invalid int conversion format: '%s'", csBytes)
	}

	scBytesInt, err := strconv.ParseInt(scBytes, 10, 64)
	if err != nil {
		log.Printf("Error: invalid int conversion format: '%s'", scBytes)
	}

	fileSizeInt, err := strconv.ParseInt(fileSize, 10, 64)
	if err != nil {
		log.Printf("Error: invalid int conversion format: '%s'", fileSize)
	}
	var tempVideoStat VideoStat
	tempVideoStat.IP = toks[3]
	tempVideoStat.Filesize = fileSizeInt
	tempVideoStat.CsBytes = csBytesInt
	tempVideoStat.ScyBytes = scBytesInt
	tempVideoStat.date = date
	tempVideoStat.streamId = streamId
	tempVideoStat.itemType = streamType
	tempVideoStat.httpCode = toks[12]

	c <- tempVideoStat

	return
}

func getStreamId(url string) (string, string, error) {
	toks := strings.Split(url, "/")
	lenght := len(toks)
	idType := ""
	if lenght < 4 || lenght > 5 {
		return "", "", errors.New("invalid URL format")
	}

	switch toks[1] {
	case "hls":
		idType = "manifest_id"
	case "recordings":
		idType = "stream_id"
	case "live":
		idType = "manifest_id"
	default:
		return "", "", errors.New("invalid URL format: first token should be one of hls, recording or live")
	}

	if !strings.HasSuffix(toks[lenght-1], ".m3u8") && !strings.HasSuffix(toks[lenght-1], ".ts") {
		return "", "", errors.New("invalid URL format - url not ending with index.m3u8 or .ts")
	}

	if strings.HasPrefix(toks[2], "video+") {
		return toks[2][6:], "stream_name", nil
	}

	return toks[2], idType, nil
}

func isCommentLine(line string) bool {
	return strings.HasPrefix(line, "#")
}

func isEmptyLine(line string) bool {
	return line == ""
}

func isValidFile(path string) bool {
	extension := filepath.Ext(path)
	if verbose {
		log.Println("extension ", extension)
	}
	if extension == ".gz" {
		return true
	}

	return false
}

func countUnique(slice []string) int {
	// create a map with all the values as key
	uniqMap := make(map[string]struct{})
	for _, v := range slice {
		uniqMap[v] = struct{}{}
	}

	// turn the map keys into a slice
	uniqSlice := make([]string, 0, len(uniqMap))
	for v := range uniqMap {
		uniqSlice = append(uniqSlice, v)
	}
	return len(uniqSlice)
}

func getCsvLine(date string, streamId string, manifestId string, manifestName string, countUniqueIPs int, contIPs int, totalCsBytes int64, totalScyBytes int64, totalFilesize int64, httpCode string) string {
	return fmt.Sprintf("%s,%s,%s,%s,%d,%d,%d,%d,%d,%s", date, streamId, manifestId, manifestName, countUniqueIPs, contIPs, totalCsBytes, totalScyBytes, totalFilesize, httpCode)
}

func getSqlLine(date string, streamId string, manifestId string, streamName string, countUniqueIPs int, contIPs int, totalCsBytes int64, totalScyBytes int64, totalFilesize int64, itemType string, httpCode string) string {
	template := `INSERT INTO cdn_stats (id, date,stream_id,manifest_id,stream_name,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size,http_code) 
		VALUES ('%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %d, '%s')
		ON CONFLICT (id) DO UPDATE 
		SET date = '%s', 
			stream_id = '%s',
			manifest_id = '%s',
			stream_name = '%s',
			unique_users = %d,
			total_views = %d,
			total_cs_bytes = %d,
			total_sc_bytes = %d,
			total_file_size = %d,
			http_code = %s;`
	id := date + "_" + itemType + "_" + streamId + manifestId + streamName
	return fmt.Sprintf(template, id, date, streamId, manifestId, streamName, countUniqueIPs, contIPs, totalCsBytes, totalScyBytes, totalFilesize, httpCode, date, streamId, manifestId, streamName, countUniqueIPs, contIPs, totalCsBytes, totalScyBytes, totalFilesize, httpCode)
}

func getCsvHeader() string {
	return "date,stream_id,manifest_id,stream_name,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size,httpCode"
}

func getSqlHeader() string {
	return `CREATE TABLE IF NOT EXISTS cdn_stats (
		id text PRIMARY KEY,
		date text,
		stream_id text,
		manifest_id text,
		stream_name text,
		unique_users integer,
		total_views integer,
		total_cs_bytes integer,
		total_sc_bytes integer,
		total_file_size integer,
		http_code text
	 );`
}

func find(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
