package gorpx

import (
	"database/sql"
	"reflect"

	"github.com/zew/gorp"
	"github.com/zew/logx"
	"github.com/zew/util"
)

//
//
var sh3 SQLHost
var db3 *sql.DB
var db3map *gorp.DbMap

func InitDb3(hosts SQLHosts, key ...string) {
	if db3 == nil {
		sh3, db3 = initDB(hosts, key...)
	}
}
func Db3() *sql.DB {
	if db3 == nil {
		logx.Fatalf("Db3() requires previous call to InitDb3()")
	}
	return db3
}

func Db3Close() {
	if db3 != nil {
		err := db3.Close()
		util.CheckErr(err)
		db3 = nil
	}
}

func IndependentDb3Mapper() *gorp.DbMap {
	var dbmap *gorp.DbMap
	if sh3.Type == "sqlite3" {
		dbmap = &gorp.DbMap{Db: Db3(), Dialect: gorp.SqliteDialect{}}
		// We have to enable foreign_keys for EVERY connection
		// There is a gorp pull request, implementing this
		// dbmap.Exec("PRAGMA foreign_keys = true")
		dbmap.Exec("PRAGMA foreign_keys = ON")
		hasFK_B, err := dbmap.SelectStr("PRAGMA foreign_keys")
		util.CheckErr(err)
		if hasFK_B != "1" {
			logx.Printf("PRAGMA foreign_keys is %v  %T | err is %v", hasFK_B, hasFK_B, err)
		}
	} else {
		dbmap = &gorp.DbMap{Db: Db3(), Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	}
	return dbmap
}

func Db3Map() *gorp.DbMap {
	if db3map == nil {
		db3map = IndependentDb3Mapper()
	}
	// logx.Printf("Dialect3: %v", db3map.Dialect)
	return db3map
}

func Db3TableName(i interface{}) string {
	t := reflect.TypeOf(i)
	if table, err := Db3Map().TableFor(t, false); table != nil && err == nil {
		if Db3Map().Dialect == nil {
			logx.Fatalf("db3map has no dialect")
		}
		ret := Db3Map().Dialect.QuoteField(table.TableName)
		return ret
	}
	return t.Name()
}
