package main

import (
	"fmt"
	"main/sstable"
)

func main() {
	// var records []record.Record

	// records = append(records, *record.NewRecord("a", []byte("srbija")))
	// records = append(records, *record.NewRecord("b", []byte("srbija")))
	// records = append(records, *record.NewRecord("c", []byte("srbija")))
	// records = append(records, *record.NewRecord("d", []byte("srbija")))
	// records = append(records, *record.NewRecord("e", []byte("srbija")))
	// records = append(records, *record.NewRecord("f", []byte("srbija")))
	// records = append(records, *record.NewRecord("jablan", []byte("srbija")))
	// records = append(records, *record.NewRecord("jakov", []byte("srbija")))
	// records = append(records, *record.NewRecord("jasta", []byte("srbija")))
	// records = append(records, *record.NewRecord("jovan", []byte("srbija")))
	// records = append(records, *record.NewRecord("k", []byte("srbija")))
	// records = append(records, *record.NewRecord("l", []byte("srbija")))
	// records = append(records, *record.NewRecord("m", []byte("srbija")))
	// records = append(records, *record.NewRecord("n", []byte("srbija")))

	// sstable.NewSSTable(records)

	records, _ := sstable.PrefixScan("ja")
	// // records, _ := sstable.RangeScan("j", "ja")
	for _, record := range records {
		fmt.Println(record.Key)
	}
}
