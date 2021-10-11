package app

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func ValidateDownloadParameters(bucketUrl string, folder string) error {
	if len(bucketUrl) == 0 {
		return fmt.Errorf("bucket url cannot be null or empty")
	}

	if strings.HasPrefix(bucketUrl, "gs://") {
		return fmt.Errorf("bucket url should not include gs:// prefix. Please remove it")
	}

	// check if folder is a valid path
	if _, err := os.Stat(folder); err != nil {
		return fmt.Errorf("%s is an invalid path. Error: %+v", folder, err)
	}

	return nil
}

// listFiles lists objects within specified bucket.
func ListAndDownloadFiles(bucket string, folder string, credsFile string, verbose bool) error {
	// bucket := "bucket-name"
	ctx := context.Background()
	var opts []option.ClientOption
	if credsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credsFile))
	} else {
		opts = append(opts, option.WithoutAuthentication())
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	defer cancel()

	it := client.Bucket(bucket).Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}

		if verbose {
			log.Println("Download file: ", attrs.Name)
		}
		err = downloadAndSaveFile(client, bucket, attrs.Name, folder, verbose)
		if err != nil {
			log.Printf("Error downloading file name=%s err=%v", attrs.Name, err)
			return err
		}

	}
	return nil
}

func downloadAndSaveFile(client *storage.Client, bucket, name, folder string, verbose bool) error {
	data, err := downloadFile(client, bucket, name, verbose)
	if err != nil {
		return err
	}

	fileName := filepath.FromSlash(folder + "/" + name)
	dir := filepath.Dir(fileName)
	if verbose {
		log.Printf("Making directory %s", dir)
	}
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}
	if verbose {
		log.Printf("Writing file: %s\n\n", fileName)
	}
	err = ioutil.WriteFile(fileName, data, 0644)
	if err != nil {
		return err
	}
	return nil
}

// downloadFile downloads an object.
func downloadFile(client *storage.Client, bucket string, object string, verbose bool) ([]byte, error) {
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", object, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	if verbose {
		log.Printf("Blob %v downloaded.\n", object)
	}
	return data, nil
}
