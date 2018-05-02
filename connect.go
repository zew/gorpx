package gorpx

import (
	"database/sql"
	"fmt"
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

// Connects to a db and pings it as a check
func initDB(hosts SQLHosts, key string) (SQLHost, *sql.DB) {

	var (
		db4 *sql.DB
		sh  SQLHost
		err error
	)

	if len(hosts) == 0 {
		logx.Fatalf("initDb() requires a map of hosts as argument. Subsequently calls sql.Open()")
	}

	sh = hosts[key]

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
		if strings.HasSuffix(fName, ".sqlite.sqlite") {
			fName = strings.TrimSuffix(fName, ".sqlite") // chop off doubly extensions
		}
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
				logx.Printf("cn %q: could not open %v: %v", key, v, err)
				continue
			}

			// Following pragmas speed up backup or clone immensely
			{
				res, err := db4.Exec("PRAGMA automatic_index = false;")
				if err != nil {
					logx.Printf("pragma automatic_index failed: %v", err)
				}
				logx.Printf("pragma automatic_index succeeded; %v", res)
			}

			{
				res, err := db4.Exec("PRAGMA journal_mode = OFF;")
				if err != nil {
					logx.Printf("pragma journal_mode failed: %v", err)
				}
				logx.Printf("pragma journal_mode succeeded; %v", res)
			}

			{
				res, err := db4.Exec("PRAGMA main.journal_mode = OFF;")
				if err != nil {
					logx.Printf("pragma main.journal_mode failed: %v", err)
				}
				logx.Printf("pragma main.journal_mode succeeded; %v", res)
			}

			{
				res, err := db4.Exec("PRAGMA synchronous = 0;")
				if err != nil {
					logx.Printf("pragma synchronous failed: %v", err)
				}
				logx.Printf("pragma synchronous succeeded; %v", res)
			}
			{
				res, err := db4.Exec("PRAGMA main.synchronous = 0;")
				if err != nil {
					logx.Printf("pragma main.synchronous failed: %v", err)
				}
				logx.Printf("pragma main.synchronous succeeded; %v", res)
			}

			found = true
			break
		}
		if !found {
			logx.Fatalf("cn %q: check the directory of main.sqlite", key)
		}

	} else {
		connStr2 := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s%s", sh.User, util.EnvVarRequired("SQL_PW"), sh.Host, sh.Port, sh.DbName, paramsJoined)

		connStrWithoutPass := connStr2
		sqlPw, err := util.EnvVar("SQL_PW")
		if err == nil && sqlPw != "" {
			connStrWithoutPass = strings.Replace(connStrWithoutPass, sqlPw, "secret", -1)
			logx.Printf("cn %q - gorp conn: %v", key, connStrWithoutPass)
		}

		db4, err = sql.Open("mysql", connStr2)
		util.CheckErr(err)
	}

	err = db4.Ping()
	util.CheckErr(err)
	logx.Printf("cn %q: gorp database connection up", key)

	return sh, db4
}

// checkRes is checking the error *and* the sql result
// of a sql query.
func CheckRes(sqlRes sql.Result, err error) {
	defer logx.SL().Incr().Decr()
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
