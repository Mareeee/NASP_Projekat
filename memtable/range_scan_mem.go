package memtable

import (
	"main/record"
)

func FindMinRangeScanMemtable(m Memtable, minKey, maxKey, structure string) (*record.Record, int) {
	index := 0

	var records []record.Record

	if structure == "skiplist" {
		records = m.skiplist.GetRecords()
	} else if structure == "btree" {
		records = m.bTree.ValuesInOrderTraversal()
	}

	for _, record := range records {
		if record.Key >= minKey && record.Key <= maxKey {
			index++
			return &record, index
		}
		index++
	}

	return nil, -1
}

func GetNextMinRangeScanMemtable(m Memtable, minKey, maxKey string, index int, structure string) (*record.Record, int) {
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
		if record.Key >= minKey && record.Key <= maxKey {
			return &record, index
		}
	}

	return nil, -1
}
