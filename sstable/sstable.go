package sstable

import (
	"main/record"
	"main/bloomfilter"
)

type SSTable struct {
	Data []*record.Record
}

func NewSSTable(allRecords []*record.Record) {
}
