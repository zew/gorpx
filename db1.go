package gorpx

import (
	"database/sql"
	"reflect"

	"github.com/zew/gorp"
	"github.com/zew/logx"
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
		hasFK_A, err := dbmap.SelectStr("PRAGMA foreign_keys")
		logx.Printf("PRAGMA foreign_keys is %v | err is %v", hasFK_A, err)
		dbmap.Exec("PRAGMA foreign_keys = true")
		hasFK_B, err := dbmap.SelectStr("PRAGMA foreign_keys")
		logx.Printf("PRAGMA foreign_keys is %v | err is %v", hasFK_B, err)
	} else {
		dbmap = &gorp.DbMap{Db: Db1(), Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	}
	return dbmap
}

func DbMap1() *gorp.DbMap {
	if dbmap1 == nil {
		dbmap1 = IndependentDb1Mapper()
	}
	return dbmap1
}

func DbMap1AddTable(i interface{}) {
	if dbmap1 == nil {
		dbmap1 = IndependentDb1Mapper()
	}
	dbmap1.AddTable(i)
}

func DbMap1AddTableWithName(i interface{}, name string) {
	if dbmap1 == nil {
		dbmap1 = IndependentDb1Mapper()
	}
	dbmap1.AddTableWithName(i, name)
}

func Db1TableName(i interface{}) string {
	t := reflect.TypeOf(i)
	if table, err := DbMap1().TableFor(t, false); table != nil && err == nil {
		return DbMap1().Dialect.QuoteField(table.TableName)
	}
	return t.Name()
}
