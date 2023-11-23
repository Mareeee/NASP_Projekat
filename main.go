package main

import (
	"main/wal"
	// "main/mmap"
)

func main() {

	wal_object := new(wal.Wal)
	wal_object.Wal("../data/wal")

	wal_object.AddRecord("miroslav", "5")

	wal_object_2 := new(wal.Wal)
	wal_object_2.LoadWal(wal.SEGMENT_FILE_PATH)

	// mmap.WriteToFile()

	// value je niz bajtova
}
