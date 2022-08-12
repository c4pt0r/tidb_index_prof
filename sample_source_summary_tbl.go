package main

import (
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
			digest_text, digest, exec_count, first_seen, last_seen, index_names, table_names
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
		err = rows.Scan(&sample.DigestText,
			&sample.Digest,
			&sample.Count,
			&sample.FirstSeen,
			&sample.LastSeen,
			&usedIndexes,
			&tableNames)
		if err != nil {
			return nil, err
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
