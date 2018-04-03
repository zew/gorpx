// Package gorpx manages multiple database connections
// and a database map for each connection.
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
	"github.com/zew/logx"
	"github.com/zew/util"
)

//
//

type dataSource struct {
	host  SQLHost     // The constituting connection parameters
	sqlDb *sql.DB     // The sql database connection
	mp    *gorp.DbMap // The database map, that brokers the queries
}

// type TSources map[int]dataSource

var dataSources = map[int]dataSource{}

// Previously InitDb
// The key to SQLHosts config is given either
//    by environment variable DATASOURCEX
// or set to default "dsnX"
// or explicitly submitted as optional key (i.e. for temporary backups)
func SetAndInitDatasourceId(dataSourceId int, hosts SQLHosts, optKey ...string) {

	key := os.Getenv(fmt.Sprintf("DATASOURCE%v", dataSourceId+1))
	if key == "" {
		key = fmt.Sprintf("dsn%v", dataSourceId+1)
	}
	if len(optKey) > 0 {
		key = optKey[0]
	}

	DbClose(dataSourceId) // close previous connection

	// logx.Printf("\tinit key %v", key)
	host, sqlDb := initDB(hosts, key)
	dsrc := dataSource{
		host:  host,
		sqlDb: sqlDb,
	}
	dataSources[dataSourceId] = dsrc
}

func Type(optDataSourceId ...int) string {
	dataSrcId := 0
	if len(optDataSourceId) > 0 {
		dataSrcId = optDataSourceId[0]
	}
	if _, ok := dataSources[dataSrcId]; !ok {
		logx.Fatalf("dataSources[%v] not set. Previous call to SetAndInitDatasourceId() required", dataSrcId)
	}
	return dataSources[dataSrcId].host.Type
}

func Db(optDataSourceId ...int) *sql.DB {
	dataSrcId := 0
	if len(optDataSourceId) > 0 {
		dataSrcId = optDataSourceId[0]
	}
	if _, ok := dataSources[dataSrcId]; !ok {
		logx.Fatalf("dataSources[%v] not set. Previous call to SetAndInitDatasourceId() required", dataSrcId)
	}
	if dataSources[dataSrcId].sqlDb == nil {
		logx.Fatalf("dataSources[%v].sqlDb is nil. Previous call to SetAndInitDatasourceId() required", dataSrcId)
	}
	return dataSources[dataSrcId].sqlDb
}

func DbClose(optDataSourceId ...int) {
	dataSrcId := 0
	if len(optDataSourceId) > 0 {
		dataSrcId = optDataSourceId[0]
	}
	if _, ok := dataSources[dataSrcId]; !ok {
		logx.Printf("dataSources[%v] not set. Previous call to SetAndInitDatasourceId() required", dataSrcId)
		return
	}
	if dataSources[dataSrcId].sqlDb == nil {
		logx.Printf("dataSources[%v].sqlDb is nil. Previous call to SetAndInitDatasourceId() required", dataSrcId)
		return
	}

	err := dataSources[dataSrcId].sqlDb.Close()
	util.CheckErr(err)
	delete(dataSources, dataSrcId)
}

func IndependentDbMapper(optDataSourceId ...int) *gorp.DbMap {

	dataSrcId := 0
	if len(optDataSourceId) > 0 {
		dataSrcId = optDataSourceId[0]
	}

	var dbmap *gorp.DbMap
	if Type(dataSrcId) == "sqlite3" {
		dbmap = &gorp.DbMap{Db: Db(dataSrcId), Dialect: gorp.SqliteDialect{}}
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
		dbmap = &gorp.DbMap{Db: Db(dataSrcId), Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	}
	return dbmap
}

func IndependentDbMapperFunc(idx int) func() *gorp.DbMap {
	return func() *gorp.DbMap {
		return IndependentDbMapper(idx)
	}
}

func DbMap(optDataSourceId ...int) *gorp.DbMap {
	dataSrcId := 0
	if len(optDataSourceId) > 0 {
		dataSrcId = optDataSourceId[0]
	}
	if _, ok := dataSources[dataSrcId]; !ok {
		logx.Fatalf("dataSources[%v] not set. Previous call to SetAndInitDatasourceId() required", dataSrcId)
	}
	if dataSources[dataSrcId].mp == nil {
		dsrc := dataSources[dataSrcId]
		dsrc.mp = IndependentDbMapper(dataSrcId)
		dataSources[dataSrcId] = dsrc
	}
	// logx.Printf("Dialect1: %v", dataSources[dataSrcId].mp.Dialect)
	return dataSources[dataSrcId].mp
}
func Db2Map() *gorp.DbMap {
	return DbMap(1)
}

func DbTableName(i interface{}, optDataSourceId ...int) string {
	dataSrcId := 0
	if len(optDataSourceId) > 0 {
		dataSrcId = optDataSourceId[0]
	}
	t := reflect.TypeOf(i)
	if table, err := DbMap(dataSrcId).TableFor(t, false); table != nil && err == nil {
		if DbMap(dataSrcId).Dialect == nil {
			logx.Fatalf("dbmap dataSrcIdhas no dialect")
		}
		ret := DbMap(dataSrcId).Dialect.QuoteField(table.TableName)
		return ret
	}
	return t.Name()
}
func Db2TableName(i interface{}) string {
	return DbTableName(i, 1)
}

// Not for independent dbMappers
func TraceOn() {
	for key, dsrc := range dataSources {
		if dsrc.mp != nil {
			dsrc.mp.TraceOn(fmt.Sprintf("gorpx cn %v: ", key), log.New(os.Stdout, "", 0))
		}
		dataSources[key] = dsrc
	}
}

func TraceOff() {
	for key, dsrc := range dataSources {
		if dsrc.mp != nil {
			dsrc.mp.TraceOff()
		}
		dataSources[key] = dsrc
	}
}
