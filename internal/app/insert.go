package app

import (
	"database/sql"
	"fmt"
	"io/ioutil"

	"github.com/golang/glog"
	_ "github.com/lib/pq"
	"github.com/livepeer/cdn-log-puller/internal/common"
)

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Dbname   string
}

func ValidateInsertParameters(host string, port int, user string, pwd string, db string) (PostgresConfig, error) {
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
		Host:     host,
		Port:     port,
		User:     user,
		Password: pwd,
		Dbname:   db,
	}

	return pgConf, nil
}

func InsertData(pgConf PostgresConfig, file string) error {
	// connection string
	psqlconn := getPgConnectionString(pgConf)

	glog.V(common.VERBOSE).Info("PostgreSQL connection string: ", psqlconn)

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

	glog.Info("Connected!")

	c, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	sql := string(c)
	result, err := db.Exec(sql)
	if err != nil {
		return err
	}

	glog.V(common.VERBOSE).Infof("SQL output: %+v", result)
	return nil
}

func getPgConnectionString(pgConf PostgresConfig) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", pgConf.Host, pgConf.Port, pgConf.User, pgConf.Password, pgConf.Dbname)
}
