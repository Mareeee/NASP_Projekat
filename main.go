package main

import (
	"fmt"
	"main/sstable"
)

func main() {
	// wal := wal.LoadWal()
	// wal.AddRecord("a", []byte("Stakic"))
	// wal.AddRecord("b", []byte("Senta"))
	// wal.AddRecord("c", []byte("Koksi"))
	// wal.AddRecord("david", []byte("Stakic"))
	// wal.AddRecord("dacicic", []byte("Senta"))
	// wal.AddRecord("dada", []byte("Koksi"))
	// wal.AddRecord("g", []byte("Stakic"))
	// wal.AddRecord("h", []byte("Senta"))
	// wal.AddRecord("i", []byte("Koksi"))
	// rekordi := wal.LoadAllRecords()

	// valueSlice := make([]record.Record, len(rekordi))
	// for i, ptr := range rekordi {
	// 	valueSlice[i] = *ptr
	// }

	// sstable.NewSSTable(valueSlice)

	record, err := sstable.Search("a")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(record.Value))

	// records, _ := sstable.PrefixScan("da")
	// for _, record := range records {
	// 	fmt.Println(record.Key)
	// }
}
