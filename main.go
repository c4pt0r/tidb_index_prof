package main

/*
tidb_index_prof
usage: ./tidb_index_prof -u <dbname> -p <dbpass> -H host -P <port> -l <log level>
output: sql query summary in last 30minutes, how many times each index is used (will also output non used indexes)


Example:

create table t(a varchar(255), b varchar(255), c int, key a(a), key b(b), key c(c));
insert into t values('a', 'b', 1);
insert into t values('aa', 'bb', 2);
insert into t values('aaa', 'bbbb', 3);
insert into t values('aaaaa', 'bbbbb', 4);
select * from t;
select * from t where a='a';
select * from t where a='aa';
select * from t where a='aaa' or c = 4;

$./tidb_index_prof -u test -p test -H localhost -P 3306 -l debug

--- Index usage stat:
{
  "t": {
    "t:a": 3,
    "t:b": 0,
    "t:c": 1
  }
}
--- Full table scan samples:
[
  {
    "digest_text": "select * from `t`",
    "digest": "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7",
    "used_indexes": null,
    "Count": 1,
    "firstSeen": "2022-08-11T17:39:09Z",
    "lastSeen": "2022-08-11T17:39:09Z"
  }
]

*/

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
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

	db *sql.DB
)

var (
	sqlGetStmtSummary = `
		SELECT 
			digest_text, digest, exec_count, first_seen, last_seen, index_names 
		FROM 
			INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY WHERE STMT_TYPE = 'Select' AND TABLE_NAMES LIKE '%%%s%%'`

	sqlGetAllIndexesForTable = `
		SELECT
			TABLE_NAME, KEY_NAME
		FROM
			INFORMATION_SCHEMA.TIDB_INDEXES
		WHERE
			TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
			`
)

type Index struct {
	TblName string `json:"tbl_name"`
	IdxName string `json:"idx_name"`
}

func (i Index) String() string {
	return fmt.Sprintf("%s:%s", i.TblName, i.IdxName)
}

type Sample struct {
	DigestText string  `json:"digest_text"`
	Digest     string  `json:"digest"`
	UsedIndex  []Index `json:"used_indexes"`
	Count      int
	FirstSeen  time.Time `json:"firstSeen"`
	LastSeen   time.Time `json:"lastSeen"`
}

type IndexCounter map[string]int
type TablesIndexCounter map[string]IndexCounter

var stat TablesIndexCounter = make(TablesIndexCounter)

func DB() *sql.DB {
	return db
}

func getAllIndexesForTable(dbName, tblName string) ([]Index, error) {
	var indexes []Index
	stmt := fmt.Sprintf(sqlGetAllIndexesForTable, dbName, tblName)
	log.D(stmt)
	rows, err := db.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var idx Index
		if err := rows.Scan(&idx.TblName, &idx.IdxName); err != nil {
			return nil, err
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func collectSample(dbName string) ( /* samplesWithIndex */ []Sample /* samplesFullTableScan */, []Sample, error) {
	var (
		err  error
		rows *sql.Rows
	)
	stmt := fmt.Sprintf(sqlGetStmtSummary, dbName)
	log.D(stmt)
	rows, err = db.Query(stmt)
	if err != nil {
		log.Fatalf("Failed to query TiDB: %s", err)
	}
	defer rows.Close()
	var samplesWithIndex []Sample
	var samplesFullTableScan []Sample

	for rows.Next() {
		var sample Sample
		var usedIndexes sql.NullString
		err = rows.Scan(&sample.DigestText, &sample.Digest, &sample.Count, &sample.FirstSeen, &sample.LastSeen, &usedIndexes)
		if err != nil {
			return nil, nil, err
		}
		if usedIndexes.Valid {
			// parse index names from string
			items := strings.Split(usedIndexes.String, ",")
			for _, item := range items {
				if item == "" {
					continue
				}
				parts := strings.Split(item, ":")
				index := Index{
					TblName: strings.ToLower(parts[0]),
					IdxName: strings.ToLower(parts[1]),
				}
				sample.UsedIndex = append(sample.UsedIndex, index)
			}
			samplesWithIndex = append(samplesWithIndex, sample)
		} else {
			// full table scans
			samplesFullTableScan = append(samplesFullTableScan, sample)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return samplesWithIndex, samplesFullTableScan, nil
}

func main() {
	flag.Parse()
	log.SetLevelByString(*logLevel)

	var err error
	db, err = sql.Open("mysql", *dbUser+":"+*dbPass+"@tcp("+*dbHost+":"+*dbPort+")/?parseTime=true")
	if err != nil {
		log.Fatalf("Failed to connect to TiDB: %s", err)
	}

	samples, samplesFullTableScan, err := collectSample(*dbName)
	if err != nil {
		log.Fatal(err)
	}

	// get all tables
	var tableNames []string
	for _, sample := range samples {
		tableNames = append(tableNames, sample.UsedIndex[0].TblName)
	}

	// build stat map
	for _, tblName := range tableNames {
		if _, ok := stat[tblName]; !ok {
			stat[tblName] = make(IndexCounter)
		}
		indexes, err := getAllIndexesForTable(*dbName, tblName)
		if err != nil {
			log.Fatal(err)
		}
		for _, index := range indexes {
			stat[tblName][index.String()] = 0
		}
	}

	// count used indexes
	for _, sample := range samples {
		for _, index := range sample.UsedIndex {
			stat[index.TblName][index.String()] += sample.Count
		}
	}

	// output result
	fmt.Println("--- Index usage stat:")
	b, _ := json.MarshalIndent(stat, "", "  ")
	fmt.Println(string(b))

	// output full table scan result
	fmt.Println("--- Full table scan samples:")
	b, _ = json.MarshalIndent(samplesFullTableScan, "", "  ")
	fmt.Println(string(b))
}
