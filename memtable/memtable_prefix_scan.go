package memtable

import (
	"main/record"
	"strings"
)

func FindFirstPrefixMemtable(m Memtable, prefix string, structure string) (*record.Record, int) {
	index := 0

	var records []record.Record

	if structure == "skiplist" {
		records = m.skiplist.GetRecords()
	} else if structure == "btree" {
		records = m.bTree.ValuesInOrderTraversal()
	}

	for _, record := range records {
		if strings.HasPrefix(record.Key, prefix) {
			index++
			return &record, index
		}
		index++
	}

	return nil, -1
}

func GetNextPrefixMemtable(m Memtable, prefix string, index int, structure string) (*record.Record, int) {
	var records []record.Record

	if structure == "skiplist" {
		records = m.skiplist.GetRecords()
	} else if structure == "btree" {
		records = m.bTree.ValuesInOrderTraversal()
	}

	if index > len(records) {
		return nil, -1
	} else {
		record := records[index]
		if strings.HasPrefix(record.Key, prefix) {
			return &record, index
		}
	}

	return nil, -1
}
