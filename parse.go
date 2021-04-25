package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type VideoStats struct {
	IPs           []string
	TotalFilesize int64
	TotalCsBytes  int64
	TotalScyBytes int64
}

const (
	fieldSeparator = ","
)

var arrDetails map[string]map[string]*VideoStats
var lock = sync.Mutex{}

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
	arrDetails = make(map[string]map[string]*VideoStats)
	// get file list
	var wg sync.WaitGroup
	err := filepath.Walk(folder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if isValidFile(path) {
				wg.Add(1)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					if verbose {
						log.Println("Parse file: ", path)
					}
					err = parseFile(path)
				}(&wg)
			}

			if err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		return err
	}
	wg.Wait()
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
		for stream, details := range val {
			countUniqueIPs := countUnique(details.IPs)

			bufString := ""
			switch format {
			case "csv":
				bufString = getCsvLine(date, stream, countUniqueIPs, len(details.IPs), details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize)
			case "sql":
				bufString = getSqlLine(date, stream, countUniqueIPs, len(details.IPs), details.TotalCsBytes, details.TotalScyBytes, details.TotalFilesize)
			default:
				return fmt.Errorf("Invalid output format %s, valid format are csv and sql.", format)
			}

			_, err = datawriter.WriteString(bufString + "\n")

			if err != nil {
				return fmt.Errorf("failed writing line %s to file: %s", bufString, err)
			}

		}
	}
	datawriter.Flush()

	return nil
}

func createTempDirectory() (string, error) {
	dir, err := ioutil.TempDir("", "log-parser")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)

	return dir, nil
}

func parseFile(file string) error {
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

		parseLine(contents.Text())
	}
	if verbose {
		log.Println("End file: ", file)
	}
	return nil
}

func parseLine(line string) {
	toks := strings.Split(line, "\t")

	if len(toks) < 17 {
		if verbose {
			log.Printf("Warning: line is not following the log standard. '%s'", line)
		}
		return
	}

	date := toks[0]
	ip := toks[3]
	fileSize := toks[7]
	csBytes := toks[8]
	scBytes := toks[9]
	url := toks[14]
	streamId, err := getStreamId(url)
	if err != nil {
		log.Printf("Warning: invalid URL format: '%s'", url)
	}

	if verbose {
		log.Printf("%s %s %s %s %s %s", date, ip, fileSize, csBytes, scBytes, streamId)
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
	var tempVideoStat VideoStats

	lock.Lock()
	if arrDetails[date] == nil {
		arrDetails[date] = make(map[string]*VideoStats)
	}
	if arrDetails[date][streamId] == nil {
		tempVideoStat.IPs = []string{ip}
		tempVideoStat.TotalFilesize = fileSizeInt
		tempVideoStat.TotalCsBytes = csBytesInt
		tempVideoStat.TotalScyBytes = scBytesInt
	} else {
		tempVideoStat.IPs = append(arrDetails[date][streamId].IPs, ip)
		tempVideoStat.TotalFilesize = arrDetails[date][streamId].TotalFilesize + fileSizeInt
		tempVideoStat.TotalCsBytes = arrDetails[date][streamId].TotalCsBytes + csBytesInt
		tempVideoStat.TotalScyBytes = arrDetails[date][streamId].TotalScyBytes + scBytesInt
	}

	//log.Printf("%+v\n", tempVideoStat)
	arrDetails[date][streamId] = &tempVideoStat
	lock.Unlock()

	return
}

func getStreamId(url string) (string, error) {
	toks := strings.Split(url, "/")
	if len(toks) < 3 {
		return "", errors.New("invalid URL format")
	}
	return toks[2], nil
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

func getCsvLine(date string, stream string, countUniqueIPs int, contIPs int, totalCsBytes int64, totalScyBytes int64, totalFilesize int64) string {
	return fmt.Sprintf("%s,%s,%d,%d,%d,%d,%d", date, stream, countUniqueIPs, contIPs, totalCsBytes, totalScyBytes, totalFilesize)
}

func getSqlLine(date string, stream string, countUniqueIPs int, contIPs int, totalCsBytes int64, totalScyBytes int64, totalFilesize int64) string {
	template := `INSERT INTO cdn_stats (id, date,stream_id,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size) 
		VALUES ('%s', '%s', '%s', %d, %d, %d, %d, %d)
		ON CONFLICT (id) DO UPDATE 
		SET date = '%s', 
			stream_id = '%s',
			unique_users = %d,
			total_views = %d,
			total_cs_bytes = %d,
			total_sc_bytes = %d,
			total_file_size = %d;`
	id := date + "_" + stream
	return fmt.Sprintf(template, id, date, stream, countUniqueIPs, contIPs, totalCsBytes, totalScyBytes, totalFilesize, date, stream, countUniqueIPs, contIPs, totalCsBytes, totalScyBytes, totalFilesize)
}

func getCsvHeader() string {
	return "date,stream_id,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size"
}

func getSqlHeader() string {
	return `CREATE TABLE IF NOT EXISTS cdn_stats (
		id text PRIMARY KEY,
		date text,
		stream_id text,
		unique_users integer,
		total_views integer,
		total_cs_bytes integer,
		total_sc_bytes integer,
		total_file_size integer
	 );`
}
