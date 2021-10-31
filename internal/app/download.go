package app

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/livepeer/cdn-log-puller/internal/common"
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
func ListAndDownloadFiles(bucket string, folder string, credsFile string) error {
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

	// ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	it := client.Bucket(bucket).Objects(ctx, nil)
	var skipped, downloaded int
	for {
		if skipped > 0 && skipped%1000 == 0 {
			glog.V(common.VERBOSE).Infof("So far skipped %d files", skipped)
		}
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}
		// skip `cdi` dir
		np := strings.Split(attrs.Name, "/")
		if len(np) < 2 {
			skipped++
			continue
		}
		// name should look like this
		// b39nq5o9/cdi/2021/08/27/cdi_20210827-020850-198051434006dc2.log.gz
		if np[1] == "cdi" {
			// we don't need `cdi`
			skipped++
			continue
		}

		glog.V(common.DEBUG).Info("Download file: ", attrs.Name)
		if err = downloadAndSaveFile(client, attrs, folder); err != nil {
			glog.Errorf("Error downloading file name=%s err=%v", attrs.Name, err)
			return err
		}
		downloaded++
		if downloaded%1000 == 0 {
			glog.V(common.VERBOSE).Infof("So far downloaded %d files", downloaded)
		}
	}
	return nil
}

var dirsCreated = make(map[string]bool)

func makeDir(dirName string) error {
	if created := dirsCreated[dirName]; created {
		return nil
	}
	glog.V(common.VVERBOSE).Infof("Making directory %s", dirName)
	if err := os.MkdirAll(dirName, os.ModePerm); err != nil {
		return err
	}
	dirsCreated[dirName] = true
	return nil
}

func downloadAndSaveFile(client *storage.Client, attrs *storage.ObjectAttrs, folder string) error {
	fileName := filepath.FromSlash(folder + "/" + attrs.Name)
	dirName := filepath.Dir(fileName)
	if err := makeDir(dirName); err != nil {
		return err
	}
	if fi, err := os.Stat(fileName); err == nil {
		if fi.Size() == attrs.Size {
			glog.V(common.VERBOSE).Infof("File %s exists on disk, skipping download", fileName)
			return nil
		}
	}

	data, err := downloadFile(client, attrs.Bucket, attrs.Name)
	if err != nil {
		return err
	}

	glog.V(common.VERBOSE).Infof("Writing file: %s\n\n", fileName)
	err = ioutil.WriteFile(fileName, data, 0644)
	if err != nil {
		return err
	}
	return nil
}

// downloadFile downloads an object.
func downloadFile(client *storage.Client, bucket string, object string) ([]byte, error) {
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
	glog.V(common.VERBOSE).Infof("Blob %v downloaded.\n", object)
	return data, nil
}
