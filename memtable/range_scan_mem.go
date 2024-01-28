package memtable

import "main/record"

func RangeScan(m Memtable, minKey string, maxKey string) []record.Record {
	var result []record.Record

	records := m.skiplist.GetRecords()
	for _, record := range records {
		if record.Tombstone == true {
			continue
		}
		if record.Key >= minKey && record.Key <= maxKey {
			result = append(result, record)
		}
	}

	return result
}
