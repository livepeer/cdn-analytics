package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/peterbourgon/ff/v3"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/livepeer/cdn-log-puller/internal/app"
	"github.com/livepeer/cdn-log-puller/internal/common"
	"github.com/livepeer/cdn-log-puller/internal/config"
	"github.com/livepeer/cdn-log-puller/internal/etl"
	"github.com/livepeer/cdn-log-puller/model"
)

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	// Get timestamp to measure execution time
	start := time.Now()

	version := flag.Bool("version", false, "Print out the version")

	// Parameter parsing
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)
	downloadVerbosity := downloadCmd.String("v", "", "Log verbosity.  {4|5|6}")

	downloadBucket := downloadCmd.String("bucket", "", "The name of the bucket where logs are located")
	downloadFolder := downloadCmd.String("folder", "", "The destination folder")
	downloadCredentials := downloadCmd.String("creds", "", "File name of file with credentials")

	analyzeCmd := flag.NewFlagSet("analyze", flag.ExitOnError)
	analyzeFolder := analyzeCmd.String("folder", "", "Logs source folder")
	analyzeOutput := analyzeCmd.String("output", "", "Output file path")
	analyzeOutputFormat := analyzeCmd.String("format", "", "Output file format. It can be sql or csv")
	analyzeVerbosity := analyzeCmd.String("v", "", "Log verbosity.  {4|5|6}")

	insertCmd := flag.NewFlagSet("insert", flag.ExitOnError)
	insertHost := insertCmd.String("host", "localhost", "PostgreSQL host. (default value: localhost)")
	insertPort := insertCmd.Int("port", 5432, "PostgreSQL port. (default value: 5432)")
	insertUser := insertCmd.String("user", "", "Database username")
	insertPwd := insertCmd.String("password", "", "Database password")
	insertDb := insertCmd.String("db", "", "Database name")
	insertVerbosity := insertCmd.String("v", "", "Log verbosity.  {4|5|6}")
	insertFile := insertCmd.String("filepath", "", "Path to the file containing the query to execute.")

	etlCmd := flag.NewFlagSet("etl", flag.ExitOnError)
	etlVerbosity := etlCmd.String("v", "", "Log verbosity.  {4|5|6}")
	etlBucket := etlCmd.String("bucket", "", "The name of the bucket where logs are located")
	etlCredentials := etlCmd.String("creds", "", "File name of file with credentials")
	etlConfig := etlCmd.String("config", "config.yaml", "Name of the config file")
	etlStaging := etlCmd.Bool("staging", true, "Parse staging data instead of production")
	etlApiKey := etlCmd.String("api-key", "", "Livepeer API key")
	etlApiUrl := etlCmd.String("api-url", "", "Livepeer API URL")

	catCmd := flag.NewFlagSet("cat", flag.ExitOnError)
	catVerbosity := catCmd.String("v", "", "Log verbosity.  {4|5|6}")
	catBucket := catCmd.String("bucket", "", "The name of the bucket where logs are located")
	catCredentials := catCmd.String("creds", "", "File name of file with credentials")
	catConfig := catCmd.String("config", "config.yaml", "Name of the config file")
	catRegion := catCmd.String("region", "", "Region")
	catDownloadDir := catCmd.String("down", "", "Download to dir instead of printing to console")

	if len(os.Args) < 2 {
		fmt.Printf("Version %s\n", model.Version)
		fmt.Print("expected 'etl', 'download', 'analyze' or 'insert' subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "etl":
		ff.Parse(etlCmd, os.Args[2:],
			ff.WithEnvVarPrefix("CP"),
			ff.WithConfigFileFlag("config"),
			ff.WithConfigFileParser(ff.PlainParser),
		)
		flag.CommandLine.Parse(nil)
		vFlag.Value.Set(*etlVerbosity)
		if *etlBucket == "" {
			glog.Fatalf("Please provide bucket name")
		}
		if *etlApiKey == "" {
			glog.Fatalf("Please provide Livepeer API key")
		}
		if *etlApiUrl == "" {
			glog.Fatalf("Please provide Livepeer API URL")
		}
		apiUrl, err := url.ParseRequestURI(*etlApiUrl)
		if err != nil {
			glog.Errorf("Invalid Livepeer API URL: %v", err)
			os.Exit(9)
		}

		cfg, err := config.ReadConfig(*etlConfig)
		if err != nil {
			glog.Fatal(err)
		}

		glog.Infof("Version %s", model.Version)
		glog.Info("subcommand 'etl'")
		glog.Infof("  bucket: %q", *etlBucket)
		glog.Infof("  credentials: %q", *etlCredentials)
		glog.Infof("  config: %q", *etlConfig)
		gctx := context.TODO()
		etli, err := etl.NewEtl(gctx, cfg, *etlBucket, *etlCredentials, *etlStaging, *etlApiKey, apiUrl)
		if err != nil {
			glog.Fatal(err)
		}
		if err = etli.Do(); err != nil {
			if err == etl.ErrForbidden {
				glog.Errorf("Wrong Livepeer API key ")
				os.Exit(10)
			}
			glog.Fatal(err)
		}
	case "cat":
		ff.Parse(catCmd, os.Args[2:],
			ff.WithEnvVarPrefix("CP"),
			ff.WithConfigFileFlag("config"),
			ff.WithConfigFileParser(ff.PlainParser),
		)
		flag.CommandLine.Parse(nil)
		vFlag.Value.Set(*catVerbosity)
		if *catBucket == "" {
			glog.Fatalf("Please provide bucket name")
		}
		cfg, err := config.ReadConfig(*catConfig)
		if err != nil {
			glog.Fatal(err)
		}
		if *catRegion == "" {
			glog.Fatalf("Please provide region name")
		}
		var topDirName string
		for k, v := range cfg.Names {
			if v == *catRegion {
				topDirName = k
				break
			}
		}
		if topDirName == "" {
			glog.Fatalf("Region %s is invalid", *catRegion)
		}

		glog.Infof("Version %s", model.Version)
		glog.Info("subcommand 'cat'")
		glog.Infof("  bucket: %q", *catBucket)
		glog.Infof("  credentials: %q", *catCredentials)
		glog.Infof("  config: %q", *catConfig)
		gctx := context.TODO()
		var opts []option.ClientOption
		opts = append(opts, option.WithCredentialsFile(*catCredentials))
		client, err := storage.NewClient(gctx, opts...)
		if err != nil {
			glog.Fatal(err)
		}
		query := storage.Query{
			Delimiter: "",
			Prefix:    topDirName + "/cds/",
		}
		ctx, cancel := context.WithTimeout(gctx, time.Second*15)
		it := client.Bucket(*catBucket).Objects(ctx, &query)
		defer cancel()
		for {
			fi, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				glog.Errorf("Returning err=%v", err)
				glog.Fatal(err)
			}
			// if limit > 0 && count >= limit {
			// 	break
			// }
			glog.V(common.DEBUG).Infof("==> Got file name=%q prefix=%q", fi.Name, fi.Prefix)
			if *catDownloadDir != "" {
				downloadFile(client, *catBucket, fi.Name, *catDownloadDir)
			} else {
				printFile(client, *catBucket, fi.Name)
			}
		}

		client.Close()

	case "download":
		ff.Parse(downloadCmd, os.Args[2:],
			ff.WithEnvVarPrefix("CP"),
			ff.WithConfigFileFlag("config"),
			ff.WithConfigFileParser(ff.PlainParser),
		)
		flag.CommandLine.Parse(nil)
		vFlag.Value.Set(*downloadVerbosity)

		// validate parameters
		err := app.ValidateDownloadParameters(*downloadBucket, *downloadFolder)
		if err != nil {
			glog.Fatal(err)
		}

		glog.Info("subcommand 'download'")
		glog.Info("  bucket:", *downloadBucket)
		glog.Info("  download folder:", *downloadFolder)
		glog.Info("  credentials:", *downloadCredentials)
		err = app.ListAndDownloadFiles(*downloadBucket, *downloadFolder, *downloadCredentials)
		if err != nil {
			glog.Fatal(err)
		}
	case "analyze":
		ff.Parse(analyzeCmd, os.Args[2:],
			ff.WithEnvVarPrefix("CP"),
			ff.WithConfigFileFlag("config"),
			ff.WithConfigFileParser(ff.PlainParser),
		)
		flag.CommandLine.Parse(nil)
		vFlag.Value.Set(*analyzeVerbosity)

		// validate parameters
		err := app.ValidateParseParameters(*analyzeFolder, *analyzeOutput, *analyzeOutputFormat)
		if err != nil {
			glog.Fatal(err)
		}

		glog.Info("subcommand 'analyze'")
		glog.Info("  folder:", *analyzeFolder)
		glog.Info("  output:", *analyzeOutput)
		glog.Info("  outputFormat:", *analyzeOutputFormat)

		err = app.ParseFiles(*analyzeFolder, *analyzeOutput, *analyzeOutputFormat)
		if err != nil {
			glog.Fatal(err)
		}

	case "insert":
		ff.Parse(insertCmd, os.Args[2:],
			ff.WithEnvVarPrefix("CP"),
			ff.WithConfigFileFlag("config"),
			ff.WithConfigFileParser(ff.PlainParser),
		)
		flag.CommandLine.Parse(nil)
		vFlag.Value.Set(*insertVerbosity)
		// validate parameters
		pgConf, err := app.ValidateInsertParameters(*insertHost, *insertPort, *insertUser, *insertPwd, *insertDb)
		if err != nil {
			glog.Fatal(err)
		}

		insertCmd.Parse(os.Args[2:])
		glog.Info("subcommand 'insert'")
		glog.Info("  host:", pgConf.Host)
		glog.Info("  port:", pgConf.Port)
		glog.Info("  user:", *insertUser)
		glog.Info("  password:", *insertPwd)
		glog.Info("  database:", *insertDb)
		glog.Info("  file path:", *insertFile)

		err = app.InsertData(pgConf, *insertFile)
		if err != nil {
			glog.Fatal(err)
		}

	default:
		flag.Parse()
		fmt.Printf("Version %s\n", model.Version)
		if *version {
			os.Exit(0)
		}

		fmt.Print("expected 'etl', 'download', 'analyze' or 'insert' subcommands")
		os.Exit(1)
	}

	elapsed := time.Since(start)
	glog.Infof("Execution took %s", elapsed)
}

func printFile(client *storage.Client, bucket, file string) {
	fmt.Printf("Printing file %s\n", file)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*6000)
	defer cancel()
	rc, err := client.Bucket(bucket).Object(file).NewReader(ctx)
	if err != nil {
		panic(fmt.Errorf("Object(%q).NewReader: %v", file, err))
	}
	defer rc.Close()

	reader, err := gzip.NewReader(rc)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	contents := bufio.NewScanner(reader)
	for contents.Scan() {
		line := contents.Text()
		fmt.Println(line)
	}
}

func downloadFile(client *storage.Client, bucket, file, targetDir string) {
	_, targetFileName := filepath.Split(file)
	targetFullPath := filepath.Join(targetDir, targetFileName)
	if _, err := os.Stat(targetFullPath); err == nil {
		glog.V(common.DEBUG).Infof("File %s exists on disk, skipping download", targetFullPath)
		return
	}
	glog.Infof("Downloading file %s", file)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*6000)
	defer cancel()
	rc, err := client.Bucket(bucket).Object(file).NewReader(ctx)
	if err != nil {
		panic(fmt.Errorf("Object(%q).NewReader: %v", file, err))
	}
	defer rc.Close()
	fh, err := os.Create(targetFullPath)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(fh, rc)
	if err != nil {
		panic(err)
	}
	fh.Close()
}
