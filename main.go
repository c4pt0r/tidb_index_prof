/*
tidb_index_prof
usage: ./tidb_index_prof -u <username> -p <password> -H host -P <port> -l <log level> -db <dbname>
output: sql query summary in last 30minutes, how many times each index is used (will also output non used indexes)
for more detail: https://docs.pingcap.com/tidb/dev/statement-summary-tables


Example:

create table t(a varchar(255) primary key, b varchar(255), c int, key b(b), key c(c));
insert into t values('a', 'b', 1);
insert into t values('aa', 'bb', 2);
insert into t values('aaa', 'bbbb', 3);
insert into t values('aaaaa', 'bbbbb', 4);
select * from t;
select * from t where a='a';
select * from t where a='aa';
select * from t where a='aaa' or c = 4;

$ ./tidb_index_prof | jq .
{
  "full_table_scan_samples": [
    {
      "digest_text": "select * from `t`",
      "digest": "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7",
      "table_names": [
        "test.t"
      ],
      "used_indexes": null,
      "count": 1,
      "first_seen": "2022-08-12T15:49:53Z",
      "last_seen": "2022-08-12T15:49:53Z"
    }
  ],
  "stat": {
    "t": {
      "t:PRIMARY": 3,
      "t:b": 0,
      "t:c": 1,
      "t:primary": 1
    }
  }
}

*/
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/c4pt0r/log"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dbUser   = flag.String("u", "root", "TiDB user")
	dbPass   = flag.String("p", "", "TiDB password")
	dbPort   = flag.String("P", "4000", "TiDB port")
	dbHost   = flag.String("H", "127.0.0.1", "TiDB host")
	dbName   = flag.String("db", "test", "TiDB database name")
	logLevel = flag.String("l", "info", "log level")

	db   *sql.DB
	stat *Stat
)

type Index struct {
	TblName string `json:"tbl_name"`
	IdxName string `json:"idx_name"`
}

func (i Index) String() string {
	return fmt.Sprintf("%s:%s", i.TblName, i.IdxName)
}

type Sample struct {
	DigestText string    `json:"digest_text"`
	Digest     string    `json:"digest"`
	TableNames []string  `json:"table_names"`
	UsedIndex  []Index   `json:"used_indexes"`
	Count      int       `json:"count"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
}

// SampleSource defines how to collect samples.
type SampleSource interface {
	GetSamples(ctx context.Context) ([]Sample, error)
}

func NewSampleSource(sourceName string) SampleSource {
	switch sourceName {
	case "summary_table":
		return NewSummaryTableSampleSource()
	case "raw_sql_stream":
		panic("not implemented")
	}
	panic("not implemented")
}

func DB() *sql.DB {
	return db
}

func main() {
	flag.Parse()
	log.SetLevelByString(*logLevel)

	var err error
	db, err = sql.Open("mysql", *dbUser+":"+*dbPass+"@tcp("+*dbHost+":"+*dbPort+")/?parseTime=true")
	if err != nil {
		log.Fatalf("Failed to connect to TiDB: %s", err)
	}
	defer db.Close()

	stat = NewStat(*dbName)

	source := NewSampleSource("summary_table")
	samples, err := source.GetSamples(context.WithValue(context.TODO(), "dbName", *dbName))
	if err != nil {
		log.Fatal(err)
	}
	// mainloop
	// count used indexes
	for _, sample := range samples {
		stat.Put(sample)
	}
	// output
	out := stat.ToJSON()
	fmt.Println(out)
}
