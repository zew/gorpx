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
var sh2 SQLHost
var db2 *sql.DB
var db2map *gorp.DbMap

func InitDb2(hosts SQLHosts, key ...string) {
	if db2 == nil {
		sh2, db2 = initDB(hosts, key...)
	}
}
func Db2() *sql.DB {
	if db2 == nil {
		logx.Fatalf("Db2() requires previous call to InitDb2()")
	}
	return db2
}

func IndependentDb2Mapper() *gorp.DbMap {
	var dbmap *gorp.DbMap
	if sh2.Type == "sqlite3" {
		dbmap = &gorp.DbMap{Db: Db2(), Dialect: gorp.SqliteDialect{}}
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
		dbmap = &gorp.DbMap{Db: Db2(), Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	}
	return dbmap
}

func Db2Map() *gorp.DbMap {
	if db2map == nil {
		db2map = IndependentDb2Mapper()
	}
	// logx.Printf("Dialect2: %v", db2map.Dialect)
	return db2map
}

func Db2TableName(i interface{}) string {
	t := reflect.TypeOf(i)
	if table, err := Db2Map().TableFor(t, false); table != nil && err == nil {
		if Db2Map().Dialect == nil {
			logx.Fatalf("db2map has no dialect")
		}
		ret := Db2Map().Dialect.QuoteField(table.TableName)
		return ret
	}
	return t.Name()
}
