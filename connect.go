package gorpx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"

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
	DbName           string            `json:"db_name"` // also sqlite3 filename
	ConnectionParams map[string]string `json:"connection_params"`
}

type SQLHosts map[string]SQLHost

func initDB(hosts SQLHosts, keys ...string) (SQLHost, *sql.DB) {

	var (
		db4 *sql.DB
		sh  SQLHost
		err error
	)

	if len(hosts) == 0 {
		logx.Fatalf("DbInit() requires a map of hosts as argument. Subsequently calls DB()")
	}
	cnKey := util.PrimeDataSource()
	if len(keys) > 0 {
		cnKey = keys[0]
	}
	sh = hosts[cnKey]

	if sh.Type != "mysql" && sh.Type != "sqlite3" {
		logx.Fatalf("sql host type %q unknown", sh.Type)
	}

	// param docu at https://github.com/go-sql-driver/mysql
	paramsJoined := "?"
	for k, v := range sh.ConnectionParams {
		paramsJoined = fmt.Sprintf("%s%s=%s&", paramsJoined, k, v)
	}

	if sh.Type == "sqlite3" {

		workDir, err := os.Getwd()
		util.CheckErr(err)
		_, srcFile, _, ok := runtime.Caller(1)
		if !ok {
			logx.Fatalf("runtime caller not found")
		}

		fName := fmt.Sprintf("%v.sqlite", sh.DbName)
		paths := []string{
			path.Join(".", fName),
			path.Join(workDir, fName),
			path.Join(path.Dir(srcFile), fName), // src file location as last option
		}

		found := false
		for _, v := range paths {
			// file, err = os.Open(v)
			db4, err = sql.Open("sqlite3", v)
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
		connStr2 := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s%s", sh.User, util.EnvVar("SQL_PW"), sh.Host, sh.Port, sh.DbName, paramsJoined)
		connStrWithoutPass := strings.Replace(connStr2, util.EnvVar("SQL_PW"), "secret", -1)
		logx.Printf("cn %q - gorp conn: %v", cnKey, connStrWithoutPass)
		db4, err = sql.Open("mysql", connStr2)
		util.CheckErr(err)
	}

	err = db4.Ping()
	util.CheckErr(err)
	logx.Printf("cn %q: gorp database connection up", cnKey)

	return sh, db4
}

// Not for independent dbMappers
func TraceOn() {
	if db1map != nil {
		db1map.TraceOn("gorpx cn1: ", log.New(os.Stdout, "", 0))
	}
	if db2map != nil {
		db2map.TraceOn("gorpx cn1: ", log.New(os.Stdout, "", 0))
	}
}
func TraceOff() {
	if db1map != nil {
		db1map.TraceOff()
	}
	if db2map != nil {
		db2map.TraceOff()
	}
}

// checkRes is checking the error *and* the sql result
// of a sql query.
func CheckRes(sqlRes sql.Result, err error) {
	defer logx.SL().Incr().Decr()
	// defer logx.SL().Incr().Decr()
	util.CheckErr(err)
	liId, err := sqlRes.LastInsertId()
	util.CheckErr(err)
	affected, err := sqlRes.RowsAffected()
	util.CheckErr(err)
	if affected > 0 && liId > 0 {
		logx.Printf("%d row(s) affected; Id %d ", affected, liId)
	} else if affected > 0 {
		logx.Printf("%d row(s) affected", affected)
	} else if liId > 0 {
		logx.Printf("Id %d", liId)
	}
}
