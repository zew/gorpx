package gorpx

import (
	"sync"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

var l sync.Mutex

func Switch() string {

	l.Lock()

	ret := ""

	if sh2.DBName != "" && db2 != nil && dbmap2 != nil {
		tmpSh1, tmpDb1, tmpDbmap1 := sh1, db1, dbmap1
		sh1, db1, dbmap1 = sh2, db2, dbmap2
		sh2, db2, dbmap2 = tmpSh1, tmpDb1, tmpDbmap1
		ret = "switch of data sources successful"
	} else {
		ret = "data source 2 not set"
	}

	l.Unlock()

	return ret

}
