package memtable

import (
	"main/record"
	"strings"
)

func PrefixScan(m Memtable, prefix string) []record.Record {
	var result []record.Record
	records := m.skiplist.GetRecords()
	for _, record := range records {
		if !record.Tombstone && strings.HasPrefix(record.Key, prefix) {
			result = append(result, record)
		}
	}
	return result
}
