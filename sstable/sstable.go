package sstable

import (
	"main/record"
)

type SSTable struct {
	Data []*record.Record
}

func NewSSTable(allRecords []*record.Record) {
}
