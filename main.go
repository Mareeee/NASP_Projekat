package main

import (
	"fmt"
	"main/sstable"
)

func main() {
	// wal := wal.LoadWal()
	// wal.AddRecord("1", []byte("Stakic"))
	// wal.AddRecord("2", []byte("Senta"))
	// wal.AddRecord("3", []byte("Koksi"))
	// wal.AddRecord("4", []byte("Stakic"))
	// wal.AddRecord("5", []byte("Senta"))
	// wal.AddRecord("6", []byte("Koksi"))
	// wal.AddRecord("7", []byte("Stakic"))
	// wal.AddRecord("8", []byte("Senta"))
	// wal.AddRecord("9", []byte("Koksi"))
	// rekordi := wal.LoadAllRecords()

	// valueSlice := make([]record.Record, len(rekordi))
	// for i, ptr := range rekordi {
	// 	valueSlice[i] = *ptr
	// }

	// sstable.NewSSTable(valueSlice)

	// record, _ := sstable.Search("Mare")
	// fmt.Println(string(record.Value))

	records, _ := sstable.RangeScan("3", "8")
	for _, record := range records {
		fmt.Println(record.Key)
	}
}
