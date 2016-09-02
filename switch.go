package gorpx

import "sync"

var l sync.Mutex

func Switch() string {

	l.Lock()

	ret := ""

	if sh2.DbName != "" && db2 != nil && db2map != nil {
		tmpSh1, tmpDb1, tmpDb1Map := sh1, db1, db1map
		sh1, db1, db1map = sh2, db2, db2map
		sh2, db2, db2map = tmpSh1, tmpDb1, tmpDb1Map
		ret = "switch of data sources successful"
	} else {
		ret = "data source 2 not set"
	}

	l.Unlock()

	return ret

}
