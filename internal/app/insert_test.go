package app

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestValidateInsertParameters_valid(t *testing.T) {
	conf, err := ValidateInsertParameters("localhost", 5432, "test", "Pwd", "cdn-log")
	if err != nil {
		t.Errorf("validateInsertParameters thown an error: %+v", err)
	}

	if conf.Host != "localhost" {
		t.Errorf("Invalid host. Expected value: localhost; Received value: %s", conf.Host)
	}
}

func TestValidateInsertParameters_invalidUser(t *testing.T) {
	_, err := ValidateInsertParameters("localhost", 5432, "", "Pwd", "cdn-log")
	if err == nil {
		t.Errorf("Empty PostgreSQL username should not be allowed")
	} else {
		fmt.Printf("Error received: %+v\n", err)
	}

}

func TestValidateInsertParameters_invalidPassword(t *testing.T) {
	_, err := ValidateInsertParameters("localhost", 5432, "test", "", "cdn-log")
	if err == nil {
		t.Errorf("Empty PostgreSQL password should not be allowed")
	} else {
		fmt.Printf("Error received: %+v\n", err)
	}
}

func TestValidateInsertParameters_invalidDatabase(t *testing.T) {
	_, err := ValidateInsertParameters("localhost", 5432, "test", "Pwd", "")
	if err == nil {
		t.Errorf("Empty PostgreSQL db name should not be allowed")
	} else {
		fmt.Printf("Error received: %+v\n", err)
	}
}

func TestGetPgConnectionString_valid(t *testing.T) {
	targetValue := "host=localhost port=5432 user=test password=Pwd dbname=cdn-log sslmode=disable"
	conf, _ := ValidateInsertParameters("localhost", 5432, "test", "Pwd", "cdn-log")
	connectionString := getPgConnectionString(conf)
	if connectionString != targetValue {
		t.Errorf("Invalid connection string")
	}
}

func TestGetPgConnectionString_invalid(t *testing.T) {
	targetValue := "host=localhost port=5432 user=Test password=Pwd dbname=cdn-log sslmode=disable"
	conf, _ := ValidateInsertParameters("localhost", 5432, "test", "Pwd", "cdn-log")
	connectionString := getPgConnectionString(conf)
	if connectionString == targetValue {
		t.Errorf("Invalid connection string")
	}
}

func TestInsertData_invalidhost(t *testing.T) {
	verbose := true
	conf, _ := ValidateInsertParameters("invalidhost", 5432, "test", "Pwd", "cdn-log")
	err := InsertData(conf, "./tests_resources/test_insert.sql", verbose)
	if err == nil {
		t.Errorf("Invalid host should throw an error")
	} else {
		fmt.Printf("Error received: %+v\n", err)
	}
}
func TestInsertData_invalidpath(t *testing.T) {
	verbose := true
	conf, _ := ValidateInsertParameters("rogue.db.elephantsql.com", 5432, "itpqedrl", "BMn2AB7nbffHW84O-Mf_MRG-WZpM67fr", "itpqedrl")
	p := filepath.FromSlash("./tests_resources/test_insert_notvalid.sql")
	err := InsertData(conf, p, verbose)
	if err == nil {
		t.Errorf("Invalid path should throw an error")
	} else {
		fmt.Printf("Error received: %+v\n", err)
	}
}

func TestInsertData_invalidfile(t *testing.T) {
	verbose := true
	conf, _ := ValidateInsertParameters("rogue.db.elephantsql.com", 5432, "itpqedrl", "BMn2AB7nbffHW84O-Mf_MRG-WZpM67fr", "itpqedrl")
	p := filepath.FromSlash("./tests_resources/test_insert_invalid.sql")
	err := InsertData(conf, p, verbose)
	if err == nil {
		t.Errorf("Invalid file should throw an error")
	} else {
		fmt.Printf("Error received: %+v\n", err)
	}
}

func TestInsertData_validfile(t *testing.T) {
	verbose := true
	conf, _ := ValidateInsertParameters("rogue.db.elephantsql.com", 5432, "itpqedrl", "BMn2AB7nbffHW84O-Mf_MRG-WZpM67fr", "itpqedrl")
	p := filepath.FromSlash("../../tests_resources/test_insert.sql")
	err := InsertData(conf, p, verbose)
	if err != nil {
		t.Errorf("Error received: %+v\n", err)
	}
}
