package linkdb

import "common"
import (
	"testing"
	"flag"
	"time"
)

var dbHelper *common.DBHelper
func init() {
	dbHost := flag.String("host", "127.0.0.1", "db host ip")
	dbPort := flag.Int("port", 3306, "db port")
	dbUser := flag.String("user", "root", "db user")
	dbPassWord := flag.String("passwd", "root", "db passwd")
	dbName := flag.String("db", "cdns", "db name")
	charset := flag.String("charset", "utf8", "charset")
	maxOpenConns := 10
	maxIdleConns := 5
	maxLifeTimeSeconds := time.Duration(10*time.Second)
	flag.Parse()
	dbHelper = common.InitDBHelper(common.NewDBConf(*dbHost, *dbName, *dbUser, *dbPassWord, *charset, *dbPort, maxOpenConns, maxIdleConns, maxLifeTimeSeconds))
}

