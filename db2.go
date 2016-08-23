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
var dbmap2 *gorp.DbMap

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
		logx.Printf("PRAGMA foreign_keys is %v  %T | err is %v", hasFK_B, hasFK_B, err)
	} else {
		dbmap = &gorp.DbMap{Db: Db2(), Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	}
	return dbmap
}

func DbMap2() *gorp.DbMap {
	if dbmap2 == nil {
		dbmap2 = IndependentDb2Mapper()
	}
	return dbmap2
}

func DbMap2AddTable(i interface{}) {
	if dbmap2 == nil {
		dbmap2 = IndependentDb2Mapper()
	}
	dbmap2.AddTable(i)
}

func DbMap2AddTableWithName(i interface{}, name string) {
	if dbmap2 == nil {
		dbmap2 = IndependentDb2Mapper()
	}
	dbmap2.AddTableWithName(i, name)
}

func Db2TableName(i interface{}) string {
	t := reflect.TypeOf(i)
	if table, err := DbMap2().TableFor(t, false); table != nil && err == nil {
		return DbMap2().Dialect.QuoteField(table.TableName)
	}
	return t.Name()
}
