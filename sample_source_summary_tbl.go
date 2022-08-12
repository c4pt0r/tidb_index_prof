package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/c4pt0r/log"
)

var _ SampleSource = &SampleSourceSummaryTbl{}

type SampleSourceSummaryTbl struct{}

var (
	sqlGetStmtSummary = `
		SELECT 
			digest_text, digest, exec_count, first_seen, last_seen, index_names, table_names, plan
		FROM 
			INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY WHERE STMT_TYPE = 'Select' AND TABLE_NAMES LIKE '%%%s%%'`
)

func (s *SampleSourceSummaryTbl) GetSamples(ctx context.Context) ([]Sample, error) {
	var (
		err  error
		rows *sql.Rows
	)
	dbName := ctx.Value("dbName").(string)
	stmt := fmt.Sprintf(sqlGetStmtSummary, dbName)
	log.D("collectSample: ", stmt)
	rows, err = db.Query(stmt)
	if err != nil {
		log.Fatalf("Failed to query TiDB: %s", err)
	}
	defer rows.Close()
	var samples []Sample

	for rows.Next() {
		var sample Sample
		var usedIndexes sql.NullString
		var tableNames sql.NullString
		var plan sql.NullString
		err = rows.Scan(&sample.DigestText,
			&sample.Digest,
			&sample.Count,
			&sample.FirstSeen,
			&sample.LastSeen,
			&usedIndexes,
			&tableNames,
			&plan)
		if err != nil {
			return nil, err
		}
		// check normal secondary index usage
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
		}
		// FIXME: check primary key usage, implementation is ugly,
		// need to be improved after this issue: https://github.com/pingcap/tidb/issues/37066
		if plan.Valid {
			planStr := plan.String
			rdr := bufio.NewReader(strings.NewReader(planStr))
			for {
				line, _, err := rdr.ReadLine()
				log.D("line: ", string(line))
				if err != nil {
					break
				}
				// this line would look like this:xxx\ttable:tblName, index:PRIMARY(a)\txxx
				var tblName string
				parts := strings.Split(string(line), "\t")
				for _, part := range parts {
					part = strings.TrimSpace(part)
					// make sure primary key is used
					if strings.Contains(part, "index:PRIMARY") {
						kvPairs := strings.Split(part, ",")
						for _, kvPair := range kvPairs {
							kv := strings.Split(kvPair, ":")
							if kv[0] == "table" {
								tblName = kv[1]
							}
						}
					}
				}
				// found primary key usage
				if tblName != "" {
					sample.UsedIndex = append(sample.UsedIndex, Index{
						TblName: strings.ToLower(tblName),
						IdxName: "PRIMARY",
					})
				}
			}
		}

		if tableNames.Valid {
			// parse table names from string
			items := strings.Split(tableNames.String, ",")
			for _, item := range items {
				if item == "" {
					continue
				}
				sample.TableNames = append(sample.TableNames, strings.ToLower(item))
			}
		}
		samples = append(samples, sample)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	log.D("collectSample, result: ", samples)
	return samples, nil
}

func NewSummaryTableSampleSource() SampleSource {
	return &SampleSourceSummaryTbl{}
}
