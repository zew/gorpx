package gorpx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/zew/gorp"
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
	DBName           string            `json:"db_name"`
	ConnectionParams map[string]string `json:"connection_params"`
}

type SQLHosts map[string]SQLHost

var sh SQLHost

var db *sql.DB
var dbmap *gorp.DbMap

func DB(hosts ...SQLHosts) *sql.DB {

	if db == nil {

		var err error

		if len(hosts) == 0 {
			logx.Fatalf("First call to DB() requires host config argument")
		}
		sh = hosts[0][util.Env()]

		if sh.Type != "mysql" && sh.Type != "sqlite3" {
			logx.Fatalf("sql host type unknown")
		}

		// param docu at https://github.com/go-sql-driver/mysql
		paramsJoined := "?"
		for k, v := range sh.ConnectionParams {
			paramsJoined = fmt.Sprintf("%s%s=%s&", paramsJoined, k, v)
		}

		if sh.Type == "sqlite3" {
			db, err = sql.Open("sqlite3", "./main.sqlite")

			logx.Fatalf("check the directory of main.sqlite")

			util.CheckErr(err)
		} else {
			connStr2 := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s%s", sh.User, util.EnvVar("SQL_PW"), sh.Host, sh.Port, sh.DBName, paramsJoined)
			logx.Printf("gorp conn: %v", connStr2)
			db, err = sql.Open("mysql", connStr2)
			util.CheckErr(err)
		}

		err = db.Ping()
		util.CheckErr(err)
		logx.Printf("gorp database connection up")

	}
	return db
}

func IndependentDbMapper() *gorp.DbMap {
	var dbmap *gorp.DbMap
	if sh.Type == "sqlite3" {
		dbmap = &gorp.DbMap{Db: DB(), Dialect: gorp.SqliteDialect{}}
		// We have to enable foreign_keys for EVERY connection
		// There is a gorp pull request, implementing this
		hasFK1, err := dbmap.SelectStr("PRAGMA foreign_keys")
		logx.Printf("PRAGMA foreign_keys is %v | err is %v", hasFK1, err)
		dbmap.Exec("PRAGMA foreign_keys = true")
		hasFK2, err := dbmap.SelectStr("PRAGMA foreign_keys")
		logx.Printf("PRAGMA foreign_keys is %v | err is %v", hasFK2, err)
	} else {
		dbmap = &gorp.DbMap{Db: DB(), Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	}
	return dbmap
}

func DBMap() *gorp.DbMap {
	if dbmap == nil {
		dbmap = IndependentDbMapper()
	}
	return dbmap
}

func DBMapAddTable(i interface{}) {
	if dbmap == nil {
		dbmap = IndependentDbMapper()
	}
	dbmap.AddTable(i)
}

func DBMapAddTableWithName(i interface{}, name string) {
	if dbmap == nil {
		dbmap = IndependentDbMapper()
	}
	dbmap.AddTableWithName(i, name)
}

func TableName(i interface{}) string {
	t := reflect.TypeOf(i)
	if table, err := DBMap().TableFor(t, false); table != nil && err == nil {
		return DBMap().Dialect.QuoteField(table.TableName)
	}
	return t.Name()
}

func TraceOn() {
	DBMap().TraceOn("gorp: ", log.New(os.Stdout, "", 0))
}
func TraceOff() {
	DBMap().TraceOff()
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
