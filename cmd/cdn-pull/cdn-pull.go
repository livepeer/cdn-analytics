package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/peterbourgon/ff/v3"

	"github.com/livepeer/cdn-log-puller/internal/app"
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
