package main

import (
	"testing"
)

func TestValidateDownloadParameters(t *testing.T) {
	err := validateDownloadParameters("lp-cdn-logs-e9u3qf432", "./download_invalid")
	if err == nil {
		t.Errorf("Invalid folder. It should throw an error")
	}

	err = validateDownloadParameters("gs://lp-cdn-logs-e9u3qf432", "./download_invalid")
	if err == nil {
		t.Errorf("Invalid bucket format. It should throw an error")
	}

	err = validateDownloadParameters("lp-cdn-logs-e9u3qf432", "./tests_resources")
	if err != nil {
		t.Errorf("Invalid downlaod parameters. Error: %+v", err)
	}

}

func TestListAndDownloadFiles(t *testing.T) {
	/*err := listAndDownloadFiles("gs://lp-cdn-logs-e9u3qf432", "./download")
	if err == nil {
		t.Errorf("Invalid bucket format should throw")
	}

	err = listAndDownloadFiles("lp-cdn-logs-e9u3qf432", "./download")
	if err != nil {
		t.Errorf("Valid bucket format should throw. Error: %+v", err)
	}*/

}
