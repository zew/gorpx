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
var sh1 SQLHost
var db1 *sql.DB
var dbmap1 *gorp.DbMap

func InitDb1(hosts SQLHosts, key ...string) {
	if db1 == nil {
		sh1, db1 = initDB(hosts, key...)
	}
}
func Db1() *sql.DB {
	if db1 == nil {
		logx.Fatalf("Db1() requires previous call to InitDb1()")
	}
	return db1
}

func IndependentDb1Mapper() *gorp.DbMap {
	var dbmap *gorp.DbMap
	if sh1.Type == "sqlite3" {
		dbmap = &gorp.DbMap{Db: Db1(), Dialect: gorp.SqliteDialect{}}
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
		dbmap = &gorp.DbMap{Db: Db1(), Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	}
	return dbmap
}

func DbMap1() *gorp.DbMap {
	if dbmap1 == nil {
		dbmap1 = IndependentDb1Mapper()
	}
	// logx.Printf("Dialect1: %v", dbmap1.Dialect)
	return dbmap1
}

func Db1TableName(i interface{}) string {
	t := reflect.TypeOf(i)
	if table, err := DbMap1().TableFor(t, false); table != nil && err == nil {
		if DbMap1().Dialect == nil {
			logx.Fatalf("DbMap1 has no dialect")
		}
		ret := DbMap1().Dialect.QuoteField(table.TableName)
		return ret
	}
	return t.Name()
}
