package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"

	_ "github.com/lib/pq"
)

type PostgresConfig struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

func validateInsertParameters(host string, port int, user string, pwd string, db string) (PostgresConfig, error) {
	var pgConf PostgresConfig
	if len(user) == 0 {
		return pgConf, fmt.Errorf("PosgreSQL username cannot be empty.")
	}

	if len(pwd) == 0 {
		return pgConf, fmt.Errorf("PosgreSQL password cannot be empty.")
	}

	if len(db) == 0 {
		return pgConf, fmt.Errorf("PosgreSQL database cannot be empty.")
	}

	pgConf = PostgresConfig{
		host:     host,
		port:     port,
		user:     user,
		password: pwd,
		dbname:   db,
	}

	return pgConf, nil
}

func insertData(pgConf PostgresConfig, file string) error {
	// connection string
	psqlconn := getPgConnectionString(pgConf)

	if verbose {
		log.Println("PostgreSQL connection string: ", psqlconn)
	}

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	// close database
	defer db.Close()

	// check db
	err = db.Ping()
	if err != nil {
		return err
	}

	log.Println("Connected!")

	c, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	sql := string(c)
	result, err := db.Exec(sql)
	if err != nil {
		return err
	}

	if verbose {
		log.Printf("SQL output: %+v", result)
	}
	return nil
}

func getPgConnectionString(pgConf PostgresConfig) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", pgConf.host, pgConf.port, pgConf.user, pgConf.password, pgConf.dbname)
}
