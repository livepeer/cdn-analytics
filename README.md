# cdn-log-analytics 📊

This tool is used for analyzing CDN logs.
It downloads data, analyze it and insert results into an PosgreSQL database.

## How to use it
### Download
Usage of download:
  -bucketurl string
        The url of the bucket where logs are located
  -folder string
        The destination folder
  -verbose
        verbose

Example:
```bash
./cdn-log-analytics download -bucketurl lp-cdn-logs-e9u3qf432 -folder download -verbose
```

### Analyze
Usage of analyze:
  -folder string
        Logs source folder
  -format string
        Output file format. It can be sql or csv
  -output string
        Output file path
  -verbose
        verbose

Examples:
```bash
./cdn-log-analytics analyze -folder ./example-logs -output test.sql -format sql
./cdn-log-analytics analyze -folder ./example-logs -output test.csv -format csv
```

## Insert
Usage of insert:
  -db string
        Database name
  -filepath string
        Path to the file containing the query to execute.
  -host string
        PostgreSQL host. (default value: localhost) (default "localhost")
  -password string
        Database password
  -port int
        PostgreSQL port. (default value: 5432) (default 5432)
  -user string
        Database username
  -verbose
        verbose

Examples:
```bash
./cdn-log-analytics insert -host localhost -port 5432 -user logs -password Passw0rd -db itpqedrl -filepath ./test.sql -verbose
```