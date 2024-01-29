package main

import "main/wal"

// tBucket "main/tokenBucket"

func main() {
	// wal := wal.LoadWal()
	// wal.WriteRecord(*record.NewRecord("Mare", []byte("Zenta")))
	// wal.WriteRecord(*record.NewRecord("Vlado", []byte("Zdravko")))
	// wal.WriteRecord(*record.NewRecord("Segrecekic", []byte("Gic")))
	// rekordi, _ := wal.LoadAllRecords()

	// valueSlice := make([]record.Record, len(rekordi))
	// for i, ptr := range rekordi {
	// 	valueSlice[i] = *ptr
	// }

	// sstable.NewSSTable(valueSlice, 1)
	// lsm.SizeTiered()
	wal_object, _ := wal.LoadWal()
	wal_object.AddRecord("Marko", []byte("Senta"))
}
