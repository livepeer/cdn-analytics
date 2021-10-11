package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/livepeer/cdn-log-analytics/internal/app"
)

var verbose = false

func main() {
	// Get timestamp to measure execution time
	start := time.Now()

	// Parameter parsing
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)

	downloadBucket := downloadCmd.String("bucket", "", "The name of the bucket where logs are located")
	downloadFolder := downloadCmd.String("folder", "", "The destination folder")
	downloadCredentials := downloadCmd.String("creds", "", "File name of file with credentials")
	downloadVerbose := downloadCmd.Bool("verbose", false, "verbose")

	analyzeCmd := flag.NewFlagSet("analyze", flag.ExitOnError)
	analyzeFolder := analyzeCmd.String("folder", "", "Logs source folder")
	analyzeOutput := analyzeCmd.String("output", "", "Output file path")
	analyzeOutputFormat := analyzeCmd.String("format", "", "Output file format. It can be sql or csv")
	analyzeVerbose := analyzeCmd.Bool("verbose", false, "verbose")

	insertCmd := flag.NewFlagSet("insert", flag.ExitOnError)
	insertHost := insertCmd.String("host", "localhost", "PostgreSQL host. (default value: localhost)")
	insertPort := insertCmd.Int("port", 5432, "PostgreSQL port. (default value: 5432)")
	insertUser := insertCmd.String("user", "", "Database username")
	insertPwd := insertCmd.String("password", "", "Database password")
	insertDb := insertCmd.String("db", "", "Database name")
	insertVerbose := insertCmd.Bool("verbose", false, "verbose")
	insertFile := insertCmd.String("filepath", "", "Path to the file containing the query to execute.")

	if len(os.Args) < 2 {
		log.Println("expected 'download', 'analyze' or 'insert' subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {

	case "download":
		downloadCmd.Parse(os.Args[2:])
		verbose = *downloadVerbose
		// validate parameters
		err := app.ValidateDownloadParameters(*downloadBucket, *downloadFolder)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("subcommand 'download'")
		log.Println("  bucket:", *downloadBucket)
		log.Println("  download folder:", *downloadFolder)
		log.Println("  verbose:", verbose)
		err = app.ListAndDownloadFiles(*downloadBucket, *downloadFolder, *downloadCredentials, verbose)
		if err != nil {
			log.Fatal(err)
		}
	case "analyze":
		analyzeCmd.Parse(os.Args[2:])
		verbose = *analyzeVerbose
		// validate parameters
		err := app.ValidateParseParameters(*analyzeFolder, *analyzeOutput, *analyzeOutputFormat)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("subcommand 'analyze'")
		log.Println("  folder:", *analyzeFolder)
		log.Println("  output:", *analyzeOutput)
		log.Println("  outputFormat:", *analyzeOutputFormat)
		log.Println("  verbose:", *analyzeVerbose)

		err = app.ParseFiles(*analyzeFolder, *analyzeOutput, *analyzeOutputFormat, verbose)
		if err != nil {
			log.Fatal(err)
		}

	case "insert":
		insertCmd.Parse(os.Args[2:])
		verbose = *insertVerbose
		// validate parameters
		pgConf, err := app.ValidateInsertParameters(*insertHost, *insertPort, *insertUser, *insertPwd, *insertDb)
		if err != nil {
			log.Fatal(err)
		}

		insertCmd.Parse(os.Args[2:])
		log.Println("subcommand 'insert'")
		log.Println("  host:", pgConf.Host)
		log.Println("  port:", pgConf.Port)
		log.Println("  user:", *insertUser)
		log.Println("  password:", *insertPwd)
		log.Println("  database:", *insertDb)
		log.Println("  file path:", *insertFile)
		log.Println("  verbose:", *insertVerbose)

		err = app.InsertData(pgConf, *insertFile, verbose)
		if err != nil {
			log.Fatal(err)
		}

	default:
		log.Println("expected 'download', 'analyze' or 'insert' subcommands")
		os.Exit(1)
	}

	elapsed := time.Since(start)
	log.Printf("Execution took %s", elapsed)
}
