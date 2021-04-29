package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateParseParameters(t *testing.T) {
	err := validateParseParameters("./tests_resources", "./tests_resources/out.csv", ".csv")
	if err == nil {
		t.Errorf(".csv is an invalid extension")
	}

	err = validateParseParameters("./test_resources", "./tests_resources/out.csv", "csv")
	if err == nil {
		t.Errorf(".test_resources is an invalid folder")
	}

	err = validateParseParameters("./tests_resources", "./test_resources/out.csv", "csv")
	if err == nil {
		t.Errorf("./test_resources/out.csv is an invalid output")
	}

	err = validateParseParameters("./tests_resources", "./tests_resources/out.csv", "csv")
	if err != nil {
		t.Errorf("Parametes should be valid")
	}
}
func TestParseFiles(t *testing.T) {
	if _, err := os.Stat("./tests_resources/out.csv"); !os.IsNotExist(err) {
		os.Remove("./tests_resources/out.csv")
	}

	err := parseFiles("./tests_resources/logs_empty", "./tests_resources/out.csv", "csv")
	if err != nil {
		t.Errorf("ParseFiles should not throw. errror: %+v", err)
	}

	if _, err := os.Stat("./tests_resources/out.csv"); os.IsNotExist(err) {
		t.Errorf("ParseFile didn't create an output file")
	}

	content, _ := ioutil.ReadFile("./tests_resources/out.csv")
	expectedContent, _ := ioutil.ReadFile("./tests_resources/expected_result/logs_empty.csv")
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("Invalid file content. Expected value: %s, received value: %s", expectedContent, content)
	}

	err = parseFiles("./tests_resources/logs", "./tests_resources/out.csv", "csv")
	if err != nil {
		t.Errorf("ParseFiles should not throw. errror: %+v", err)
	}

	if _, err := os.Stat("./tests_resources/out.csv"); os.IsNotExist(err) {
		t.Errorf("ParseFile didn't create an output file")
	}

	content, _ = ioutil.ReadFile("./tests_resources/out.csv")
	expectedContent, _ = ioutil.ReadFile("./tests_resources/expected_result/logs.csv")
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("Invalid file content. Expected value: %s, received value: %s", expectedContent, content)
	}
}
func TestGetStreamId(t *testing.T) {
	streamId := "/wp-admin/index.php"
	id, err := getStreamId(streamId)
	if err == nil {
		t.Errorf("%s should be an invalid stream id", streamId)
	}

	streamId = "/hls/fiolz5txbwy3smsr/0_1/index.m3u8"
	idExpected := "fiolz5txbwy3smsr"
	id, err = getStreamId(streamId)
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
	if isValidFile(filepath.FromSlash("./tests_resources/invalid.log")) {
		t.Errorf("invalid.log format should be invalid")
	}

	if !(isValidFile(filepath.FromSlash("./tests_resources/valid.log.gz"))) {
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
	template := "2021,1,1,2,3,4,5"
	l := getCsvLine("2021", "1", 1, 2, 3, 4, 5)
	if template != l {
		t.Errorf("Invalid line. Expected value: %s, received value: %s", template, l)
	}
}
func TestGetSqlLine_valid(t *testing.T) {
	template := `INSERT INTO cdn_stats (id, date,stream_id,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size) 
		VALUES ('2021_1', '2021', '1', 1, 2, 3, 4, 5)
		ON CONFLICT (id) DO UPDATE 
		SET date = '2021', 
			stream_id = '1',
			unique_users = 1,
			total_views = 2,
			total_cs_bytes = 3,
			total_sc_bytes = 4,
			total_file_size = 5;`
	l := getSqlLine("2021", "1", 1, 2, 3, 4, 5)
	if template != l {
		t.Errorf("Invalid line. Expected value: %s, received value: %s", template, l)
	}

}

func TestGetSqlHeader_valid(t *testing.T) {
	val := `CREATE TABLE IF NOT EXISTS cdn_stats (
		id text PRIMARY KEY,
		date text,
		stream_id text,
		unique_users integer,
		total_views integer,
		total_cs_bytes integer,
		total_sc_bytes integer,
		total_file_size integer
	 );`

	h := getSqlHeader()
	if h != val {
		t.Errorf("Invalid header. Expected value: %s, received value: %s", val, h)
	}
}

func TestGetCsvHeader_valid(t *testing.T) {
	val := "date,stream_id,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size"
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
