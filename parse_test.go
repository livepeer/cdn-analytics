package main

import (
	"path/filepath"
	"testing"
)

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
	if isValidFile(filepath.FromSlash("./tests_ressources/invalid.log")) {
		t.Errorf("invalid.log format should be invalid")
	}

	if !(isValidFile(filepath.FromSlash("./tests_ressources/valid.log.gz"))) {
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
