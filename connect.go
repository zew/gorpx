package gorpx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"

	"github.com/zew/logx"
	"github.com/zew/util"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

type SQLHost struct {
	Type             string            `json:"type"` // sqlite3
	User             string            `json:"user"`
	Host             string            `json:"host"`
	Port             string            `json:"port"`
	DBName           string            `json:"db_name"` // also sqlite3 filename
	ConnectionParams map[string]string `json:"connection_params"`
}

type SQLHosts map[string]SQLHost

func initDB(hosts SQLHosts, keys ...string) (SQLHost, *sql.DB) {

	var (
		db3 *sql.DB
		sh3 SQLHost
		err error
	)

	if len(hosts) == 0 {
		logx.Fatalf("DbInit() requires a map of hosts as argument. Subsequently calls DB()")
	}
	cnKey := util.PrimeDataSource()
	if len(keys) > 0 {
		cnKey = keys[0]
	}
	sh3 = hosts[cnKey]

	if sh3.Type != "mysql" && sh3.Type != "sqlite3" {
		logx.Fatalf("sql host type unknown")
	}

	// param docu at https://github.com/go-sql-driver/mysql
	paramsJoined := "?"
	for k, v := range sh3.ConnectionParams {
		paramsJoined = fmt.Sprintf("%s%s=%s&", paramsJoined, k, v)
	}

	if sh3.Type == "sqlite3" {

		workDir, err := os.Getwd()
		util.CheckErr(err)
		_, srcFile, _, ok := runtime.Caller(1)
		if !ok {
			logx.Fatalf("runtime caller not found")
		}

		fName := fmt.Sprintf("%v.sqlite", sh3.DBName)
		paths := []string{
			path.Join(".", fName),
			path.Join(workDir, fName),
			path.Join(path.Dir(srcFile), fName), // src file location as last option
		}

		found := false
		for _, v := range paths {
			// file, err = os.Open(v)
			db3, err = sql.Open("sqlite3", v)
			if err != nil {
				logx.Printf("cn %q: could not open %v: %v", cnKey, v, err)
				continue
			}
			found = true
			break
		}
		if !found {
			logx.Fatalf("cn %q: check the directory of main.sqlite", cnKey)
		}

	} else {
		connStr2 := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s%s", sh3.User, util.EnvVar("SQL_PW"), sh3.Host, sh3.Port, sh3.DBName, paramsJoined)
		logx.Printf("cn %q - gorp conn: %v", cnKey, connStr2)
		db3, err = sql.Open("mysql", connStr2)
		util.CheckErr(err)
	}

	err = db3.Ping()
	util.CheckErr(err)
	logx.Printf("cn %q: gorp database connection up", cnKey)

	return sh3, db3
}

// Not for independent dbMappers
func TraceOn() {
	if dbmap1 != nil {
		dbmap1.TraceOn("gorpx cn1: ", log.New(os.Stdout, "", 0))
	}
	if dbmap2 != nil {
		dbmap2.TraceOn("gorpx cn1: ", log.New(os.Stdout, "", 0))
	}
}
func TraceOff() {
	if dbmap1 != nil {
		dbmap1.TraceOff()
	}
	if dbmap2 != nil {
		dbmap2.TraceOff()
	}
}

// checkRes is checking the error *and* the sql result
// of a sql query.
func CheckRes(sqlRes sql.Result, err error) {
	defer logx.SL().Incr().Decr()
	defer logx.SL().Incr().Decr()
	util.CheckErr(err)
	liId, err := sqlRes.LastInsertId()
	util.CheckErr(err)
	affected, err := sqlRes.RowsAffected()
	util.CheckErr(err)
	if affected > 0 && liId > 0 {
		logx.Printf("%d row(s) affected ; lastInsertId %d ", affected, liId)
	} else if affected > 0 {
		logx.Printf("%d row(s) affected", affected)
	} else if liId > 0 {
		logx.Printf("%d liId", liId)
	}
}
