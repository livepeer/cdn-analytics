package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func validateDownloadParameters(bucketUrl string, folder string) error {
	if len(bucketUrl) == 0 {
		return errors.New("Bucket url cannot be null or empty")
	}
	return nil
}

// listFiles lists objects within specified bucket.
func listAndDownloadFiles(bucket string, downloadFolder string) error {
	// bucket := "bucket-name"
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	folder := downloadFolder
	// create temp directory
	if downloadFolder == "" {
		folder, err = createTempDirectory()
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
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
		log.Println("Download file: ", attrs.Name)
		func() error {
			data, err := downloadFile(bucket, attrs.Name)
			if err != nil {
				return err
			}

			fileName := filepath.FromSlash(folder + "/" + attrs.Name)
			dir := filepath.Dir(fileName)
			os.MkdirAll(dir, os.ModePerm)
			fmt.Printf("Writing file: %s\n\n", fileName)
			err = ioutil.WriteFile(fileName, data, 0644)
			if err != nil {
				return err
			}
			return nil
		}()

	}
	return nil
}

// downloadFile downloads an object.
func downloadFile(bucket string, object string) ([]byte, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
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
	log.Printf("Blob %v downloaded.\n", object)
	return data, nil
}
