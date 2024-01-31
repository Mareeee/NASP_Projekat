package memtable

import (
	"main/record"
	"strings"
)

func FindMinKeyRangeScanSSTable(m Memtable, minKey, maxKey string) (*record.Record, int) {
	index := 0
	records := m.skiplist.GetRecords()
	for _, record := range records {
		if !record.Tombstone && strings.HasPrefix(record.Key, prefix) {
			index++
			return &record, index
		}
		index++
	}
	return nil, -1
}

func GetNextMinRangeScanSSTable(m Memtable, minKey, maxKey string, index int) (*record.Record, int) {
	records := m.skiplist.GetRecords()
	if index > len(records) {
		return nil, -1
	} else {
		record := records[index]
		if !record.Tombstone && strings.HasPrefix(record.Key, prefix) {
			return &record, index
		}
	}
	return nil, -1
}
