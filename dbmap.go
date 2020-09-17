// Package gorpx manages connections to multiple datasources (i.e. one sqlite3, another mysql)
// and keeps a map for each datasource with connection pool and a data dict mapper.
// The mapping of tables   is then application specific.
// Data definition stuff   is then application specific.
// Data modification stuff is then application specific.
package gorpx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/zew/gorp"
	"github.com/zew/util"
)

//
//

type dataSource struct {
	host  SQLHost     // The constituting datasource parameters
	sqlDb *sql.DB     // The golang sql database connection
	mp    *gorp.DbMap // The database map, that brokers the queries to the database
}

// A map of all datasources.
var dataSources = map[int]dataSource{}

// SetAndInitDatasourceId - the key to SQLHosts config is given either
//    by environment variable DATASOURCEX
// or set to default "dsnX"
// or explicitly submitted as optional key (i.e. for temporary backups)
//
// The resulting connection is then set as data source id x.
// data source id 0 is the default.
// data source id 1 is the target for comparisons.
// data source id 2 is for backups
func SetAndInitDatasourceId(hosts SQLHosts, dataSourceId int) {

	key := os.Getenv(fmt.Sprintf("DATASOURCE%v", dataSourceId+1))
	if key != "" {
		log.Printf("Taking datasource %q from env", key)
	}
	if key == "" {
		key = fmt.Sprintf("dsn%v", dataSourceId+1)
		log.Printf("Taking datasource %q from id parameter", key)
	}

	DbClose(dataSourceId) // close previous connection

	// log.Printf("\tinit key %v", key)
	host, sqlDb := initDB(hosts, key)
	dsrc := dataSource{
		host:  host,
		sqlDb: sqlDb,
	}
	dataSources[dataSourceId] = dsrc
}

// Type returns "sqllite or mysql" by data source index
func Type(optDataSourceID ...int) string {
	dataSrcID := 0
	if len(optDataSourceID) > 0 {
		dataSrcID = optDataSourceID[0]
	}
	if _, ok := dataSources[dataSrcID]; !ok {
		log.Fatalf("dataSources[%v] not set. Previous call to SetAndInitDatasourceId() required", dataSrcID)
	}
	return dataSources[dataSrcID].host.Type
}

// Db returns the data source by data source index
func Db(optDataSourceID ...int) *sql.DB {
	dataSrcID := 0
	if len(optDataSourceID) > 0 {
		dataSrcID = optDataSourceID[0]
	}
	if _, ok := dataSources[dataSrcID]; !ok {
		log.Fatalf("open: dataSources[%v] not set. Previous call to SetAndInitDatasourceId() required", dataSrcID)
	}
	if dataSources[dataSrcID].sqlDb == nil {
		log.Fatalf("open: dataSources[%v].sqlDb is nil. Previous call to SetAndInitDatasourceId() required", dataSrcID)
	}
	return dataSources[dataSrcID].sqlDb
}

// DbClose closes by data source index
func DbClose(optDataSourceID ...int) {
	dataSrcID := 0
	if len(optDataSourceID) > 0 {
		dataSrcID = optDataSourceID[0]
	}
	if _, ok := dataSources[dataSrcID]; !ok {
		log.Printf("Closing previous: dataSources[%v] not set. Closing not necessary", dataSrcID)
		return
	}
	if dataSources[dataSrcID].sqlDb == nil {
		log.Printf("Closing previous: dataSources[%v].sqlDb is nil. Closing not necessary", dataSrcID)
		return
	}

	err := dataSources[dataSrcID].sqlDb.Close()
	util.CheckErr(err)
	delete(dataSources, dataSrcID)
}

// IndependentDbMapper creates a new DB Mapper on each call.
// Because for instance EnablePlainInserts() creates irreversible changes to a DB map,
// and we need a new one afterwards.
func IndependentDbMapper(optDataSourceID ...int) *gorp.DbMap {
	dataSrcID := 0
	if len(optDataSourceID) > 0 {
		dataSrcID = optDataSourceID[0]
	}

	var dbmap *gorp.DbMap
	if Type(dataSrcID) == "sqlite3" {
		dbmap = &gorp.DbMap{Db: Db(dataSrcID), Dialect: gorp.SqliteDialect{}}
		// We have to enable foreign_keys for EVERY connection
		// There is a gorp pull request, implementing this
		// dbmap.Exec("PRAGMA foreign_keys = true")
		dbmap.Exec("PRAGMA foreign_keys = ON")
		hasFK_B, err := dbmap.SelectStr("PRAGMA foreign_keys")
		util.CheckErr(err)
		if hasFK_B != "1" {
			log.Printf("PRAGMA foreign_keys is %v  %T | err is %v", hasFK_B, hasFK_B, err)
		}
	} else {
		dbmap = &gorp.DbMap{
			Db:      Db(dataSrcID),
			Dialect: gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"},
		}
	}
	return dbmap
}

// IndependentDbMapperFunc for some operations
// who need several DB mappers in a row.
// For this, we have a DB mapper "factory".
func IndependentDbMapperFunc(idx int) func() *gorp.DbMap {
	return func() *gorp.DbMap {
		return IndependentDbMapper(idx)
	}
}

// DbMap returns default DB Map, that is being reused on each DB operation.
// On the first call, a default map is created anew.
// The DB default map can then be mapped to the application specific tables like this:
// func MapAllTables(argDbMap *gorp.DbMap) {
// 		argDbMap.AddTableWithName(paramgroup.ParamGroup{}, "paramgroup")
// 		argDbMap.AddTable(pivot.Pivot{})
// 		:
// }
func DbMap(optDataSourceID ...int) *gorp.DbMap {
	dataSrcID := 0
	if len(optDataSourceID) > 0 {
		dataSrcID = optDataSourceID[0]
	}
	if _, ok := dataSources[dataSrcID]; !ok {
		log.Fatalf("dataSources[%v] not set. Previous call to SetAndInitDatasourceId() required", dataSrcID)
	}
	if dataSources[dataSrcID].mp == nil {
		dsrc := dataSources[dataSrcID]
		dsrc.mp = IndependentDbMapper(dataSrcID)
		dataSources[dataSrcID] = dsrc
	}
	// log.Printf("Dialect1: %v", dataSources[dataSrcId].mp.Dialect)
	return dataSources[dataSrcID].mp
}

// Db2Map returns the second DB Map
func Db2Map() *gorp.DbMap {
	return DbMap(1)
}

// DbTableName - for fun and confusion, the table names are in lower case or title case,
// depending on windows/linux and mysql/sqlite3.
// It depends on the MySQL server settings, whether it objects to wrong case.
// We cannot take any chances, we must derive the table name dynamical:
func DbTableName(i interface{}, optDataSourceID ...int) string {
	dataSrcID := 0
	if len(optDataSourceID) > 0 {
		dataSrcID = optDataSourceID[0]
	}
	t := reflect.TypeOf(i)
	if table, err := DbMap(dataSrcID).TableFor(t, false); table != nil && err == nil {
		if DbMap(dataSrcID).Dialect == nil {
			log.Fatalf("dbmap dataSrcIdhas no dialect")
		}
		ret := DbMap(dataSrcID).Dialect.QuoteField(table.TableName)
		return ret
	}
	return t.Name()
}

// Db2TableName returns the table name from the second data source index
func Db2TableName(i interface{}) string {
	return DbTableName(i, 1)
}

// TraceOn enables SQL tracing for all default dbMappers.
// Does not affect independent dbMappers.
func TraceOn() {
	for key, dsrc := range dataSources {
		if dsrc.mp != nil {
			dsrc.mp.TraceOn(fmt.Sprintf("gorpx cn %v: ", key), log.New(os.Stdout, "", 0))
		}
		dataSources[key] = dsrc
	}
}

// TraceOff disables SQL tracing
func TraceOff() {
	for key, dsrc := range dataSources {
		if dsrc.mp != nil {
			dsrc.mp.TraceOff()
		}
		dataSources[key] = dsrc
	}
}
