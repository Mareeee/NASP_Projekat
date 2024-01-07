package main

import (
	"fmt"
	"main/sstable"
)

func main() {
	// wal := wal.LoadWal()
	// wal.WriteRecord(*record.NewRecord("David", []byte("Stakic")))
	// wal.WriteRecord(*record.NewRecord("Mare", []byte("Senta")))
	// wal.WriteRecord(*record.NewRecord("Roksi", []byte("Koksi")))
	// rekordi := wal.LoadAllRecords()

	// valueSlice := make([]record.Record, len(rekordi))
	// for i, ptr := range rekordi {
	// 	valueSlice[i] = *ptr
	// }

	// sstable.NewSSTable(valueSlice)

	value := sstable.Search("Mare")
	fmt.Println(value)
}
