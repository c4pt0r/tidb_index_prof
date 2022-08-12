package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/c4pt0r/log"
)

var (
	sqlGetAllIndexesForTable = `
		SELECT
			TABLE_NAME, KEY_NAME
		FROM
			INFORMATION_SCHEMA.TIDB_INDEXES
		WHERE
			TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`
)

func getAllIndexesForTable(dbName, tblName string) ([]Index, error) {
	var indexes []Index
	stmt := fmt.Sprintf(sqlGetAllIndexesForTable, dbName, tblName)
	log.D("get index for table:", stmt)
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
	log.D("get index for table, result", indexes)
	return indexes, nil
}

type IndexCounter map[string]int
type TablesIndexCounter map[string]IndexCounter

type Stat struct {
	mu       sync.RWMutex
	dbName   string
	m        TablesIndexCounter
	fullScan []Sample
}

func NewStat(dbName string) *Stat {
	return &Stat{
		mu:     sync.RWMutex{},
		m:      make(TablesIndexCounter),
		dbName: dbName,
	}
}

func (s *Stat) fillStatForTable(tblName string) {
	s.mu.RLock()
	if _, ok := s.m[tblName]; ok {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[tblName] = make(IndexCounter)
	indexes, err := getAllIndexesForTable(s.dbName, tblName)
	if err != nil {
		log.Fatal(err)
	}
	for _, index := range indexes {
		if _, ok := s.m[tblName][index.String()]; !ok {
			s.m[tblName][index.String()] = 0
		}
	}
}

func (s *Stat) Put(sample Sample) {
	for _, tblName := range sample.TableNames {
		// tableName format: `dbName`.`tblName`, but we only need `tblName`
		parts := strings.Split(tblName, ".")
		s.fillStatForTable(parts[1])
	}
	if sample.UsedIndex != nil {
		for _, index := range sample.UsedIndex {
			s.mu.Lock()
			s.m[index.TblName][index.String()] += sample.Count
			s.mu.Unlock()
		}
	} else {
		s.mu.Lock()
		s.fullScan = append(s.fullScan, sample)
		s.mu.Unlock()
	}
}

func (s *Stat) ToJSON() (stats string, fullScanSamples string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stat, _ := json.MarshalIndent(s.m, "", "  ")
	fullScan, _ := json.MarshalIndent(s.fullScan, "", "  ")
	return string(stat), string(fullScan)
}
