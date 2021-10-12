package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/peterbourgon/ff/v3"

	"github.com/livepeer/cdn-log-analytics/internal/app"
)

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	// Get timestamp to measure execution time
	start := time.Now()

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
	analyzeVerbosity := downloadCmd.String("v", "", "Log verbosity.  {4|5|6}")

	insertCmd := flag.NewFlagSet("insert", flag.ExitOnError)
	insertHost := insertCmd.String("host", "localhost", "PostgreSQL host. (default value: localhost)")
	insertPort := insertCmd.Int("port", 5432, "PostgreSQL port. (default value: 5432)")
	insertUser := insertCmd.String("user", "", "Database username")
	insertPwd := insertCmd.String("password", "", "Database password")
	insertDb := insertCmd.String("db", "", "Database name")
	insertVerbosity := downloadCmd.String("v", "", "Log verbosity.  {4|5|6}")
	insertFile := insertCmd.String("filepath", "", "Path to the file containing the query to execute.")

	if len(os.Args) < 2 {
		fmt.Print("expected 'download', 'analyze' or 'insert' subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {

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
		glog.Info("expected 'download', 'analyze' or 'insert' subcommands")
		os.Exit(1)
	}

	elapsed := time.Since(start)
	glog.Infof("Execution took %s", elapsed)
}
