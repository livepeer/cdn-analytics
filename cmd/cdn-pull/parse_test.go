package main

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestValidateParseParameters(t *testing.T) {
	err := validateParseParameters("../../tests_resources", "../../tests_resources/out.csv", ".csv")
	if err == nil {
		t.Errorf(".csv is an invalid extension")
	}

	err = validateParseParameters("../../test_resources", "../../tests_resources/out.csv", "csv")
	if err == nil {
		t.Errorf(".test_resources is an invalid folder")
	}

	err = validateParseParameters("../../tests_resources", "../../test_resources/out.csv", "csv")
	if err == nil {
		t.Errorf("./test_resources/out.csv is an invalid output")
	}

	err = validateParseParameters("../../tests_resources", "../../tests_resources/out.csv", "csv")
	if err != nil {
		t.Errorf("Parametes should be valid")
	}
}
func TestParseFiles(t *testing.T) {
	emptyCsvFileName := "../../tests_resources/out_empty.csv"
	defer os.Remove(emptyCsvFileName)
	if _, err := os.Stat("../../tests_resources/out.csv"); !os.IsNotExist(err) {
		os.Remove("./tests_resources/out.csv")
	}

	err := parseFiles("../../tests_resources/logs_empty", emptyCsvFileName, "csv")
	if err != nil {
		t.Errorf("ParseFiles should not throw. errror: %+v", err)
	}

	if _, err := os.Stat("../../tests_resources/out_empty.csv"); os.IsNotExist(err) {
		t.Errorf("ParseFile didn't create an output file")
	}

	file, _ := os.Open("../../tests_resources/out_empty.csv")
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	fileExpected, _ := os.Open("../../tests_resources/expected_result/logs_empty.csv")
	defer fileExpected.Close()

	var linesExpected []string
	scannerExpected := bufio.NewScanner(fileExpected)
	for scannerExpected.Scan() {
		linesExpected = append(linesExpected, scannerExpected.Text())
	}

	if len(linesExpected) != len(lines) {
		t.Errorf("Result haven't the expected number of line, expected: %d, received: %d", len(linesExpected), len(lines))
	}
	for _, v := range lines {
		ok := false
		for _, e := range linesExpected {
			if v == e {
				ok = true
				break
			}
		}
		if !ok {
			t.Errorf("Result doesn't match expected result. Wrong line is: %s", v)
		}
	}

	err = parseFiles("../../tests_resources/logs", "../../tests_resources/out.csv", "csv")
	if err != nil {
		t.Errorf("ParseFiles should not throw. errror: %+v", err)
	}

	if _, err := os.Stat("../../tests_resources/out.csv"); os.IsNotExist(err) {
		t.Errorf("ParseFile didn't create an output file")
	}

	file2, _ := os.Open("../../tests_resources/out.csv")
	defer file2.Close()

	var lines2 []string
	scanner2 := bufio.NewScanner(file2)
	for scanner2.Scan() {
		lines2 = append(lines2, scanner2.Text())
	}

	fileExpected2, _ := os.Open("../../tests_resources/expected_result/logs.csv")
	defer fileExpected2.Close()

	var linesExpected2 []string
	scannerExpected2 := bufio.NewScanner(fileExpected2)
	for scannerExpected2.Scan() {
		linesExpected2 = append(linesExpected2, scannerExpected2.Text())
	}

	if len(linesExpected2) != len(lines2) {
		t.Errorf("Result haven't the expected number of line")
	}
	for _, v := range lines2 {
		ok := false
		for _, e := range linesExpected2 {
			if v == e {
				ok = true
				break
			}
		}
		if !ok {
			t.Errorf("Result doesn't match expected result. Wrong line is: %s", v)
		}
	}
}

func TestGetStreamId(t *testing.T) {
	streamId := "/wp-admin/index.php"
	id, _, err := getStreamId(streamId)
	if err == nil {
		t.Errorf("%s should be an invalid stream id", streamId)
	}

	streamId = "/hls/fiolz5txbwy3smsr/0_1/index.m3u8"
	idExpected := "fiolz5txbwy3smsr"
	id, _, err = getStreamId(streamId)
	if err != nil {
		t.Errorf("%s should be a valid stream id", streamId)
	}

	if id != idExpected {
		t.Errorf("Invalid stream id. Expected value: %s. Received value: %s", idExpected, id)
	}

}
func TestIsCommentLine(t *testing.T) {
	if isCommentLine("notacomment") {
		t.Errorf("Line should not be a comment")
	}

	if !isCommentLine("# this is a comment") {
		t.Errorf("Line should be a comment")
	}
}
func TestIsEmptyLine(t *testing.T) {
	if isEmptyLine("_") {
		t.Errorf("_ should not be an empty line")
	}

	if !isEmptyLine("") {
		t.Errorf("invalid empty line")
	}
}
func TestIsValidFile(t *testing.T) {
	if isValidFile(filepath.FromSlash("../../tests_resources/invalid.log")) {
		t.Errorf("invalid.log format should be invalid")
	}

	if !(isValidFile(filepath.FromSlash("../../tests_resources/valid.log.gz"))) {
		t.Errorf("valid.log.gz format should be valid")
	}
}

func TestCountUnique(t *testing.T) {
	test := []string{"a", "A", "a", "A", "A"}
	n := countUnique(test)
	if n != 2 {
		t.Errorf("Invalid count. Expected 2, got %d", n)
	}
}
func TestGetCsvLine_valid(t *testing.T) {
	template := "2021,1,,,1,2,3,4,5,404"
	l := getCsvLine("2021", "1", "", "", 1, 2, 3, 4, 5, "404")
	if template != l {
		t.Errorf("Invalid line. Expected value: %s, received value: %s", template, l)
	}
}
func TestGetSqlLine_valid(t *testing.T) {
	template := `INSERT INTO cdn_stats (id, date,stream_id,manifest_id,stream_name,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size,http_code)
		VALUES ('2021__1_404', '2021', '1', '', '', 1, 2, 3, 4, 5, '404')
		ON CONFLICT (id) DO UPDATE
		SET date = '2021',
			stream_id = '1',
			manifest_id = '',
			stream_name = '',
			unique_users = 1,
			total_views = 2,
			total_cs_bytes = 3,
			total_sc_bytes = 4,
			total_file_size = 5,
			http_code = 404;`
	l := getSqlLine("2021", "1", "", "", 1, 2, 3, 4, 5, "", "404")
	space := regexp.MustCompile(`\s+`)
	if space.ReplaceAllString(template, " ") != space.ReplaceAllString(l, " ") {
		t.Errorf("Invalid line. Expected value: \n%s \nreceived value: \n%s", space.ReplaceAllString(template, " "), space.ReplaceAllString(l, " "))
	}

}

func TestGetSqlHeader_valid(t *testing.T) {
	val := `CREATE TABLE IF NOT EXISTS cdn_stats (
		id text PRIMARY KEY,
		date text,
		stream_id text,
		manifest_id text,
		stream_name text,
		unique_users bigint,
		total_views bigint,
		total_cs_bytes bigint,
		total_sc_bytes bigint,
		total_file_size bigint,
		http_code text
	);`

	h := getSqlHeader()
	space := regexp.MustCompile(`\s+`)
	if space.ReplaceAllString(h, " ") != space.ReplaceAllString(val, " ") {
		t.Errorf("Invalid header. Expected value: \n%s, received value: \n%s,", space.ReplaceAllString(val, " "), space.ReplaceAllString(h, " "))
	}
}

func TestGetCsvHeader_valid(t *testing.T) {
	val := "date,stream_id,manifest_id,stream_name,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size,httpCode"
	h := getCsvHeader()
	if h != val {
		t.Errorf("Invalid header. Expected value: %s, received value: %s", val, h)
	}
}

func TestFind(t *testing.T) {
	slice := []string{"this", "is", "a", "test"}
	if !find(slice, "test") {
		t.Errorf("Invalid result. the string 'test' is included in the slice ")
	}

	if find(slice, "nottest") {
		t.Errorf("Invalid result. the string 'nottest' isn't included in the slice ")
	}

}
