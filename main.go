package main

import (
	"main/wal"
)

func main() {

	// wal_object := new(wal.Wal)
	// wal_object.Wal("../data/wal")

	// wal_object.AddRecord("miroslav", "5")
	// wal_object.AddRecord("sremac", "24400")
	// wal_object.AddRecord("serbijangejmsbl", "032")

	// wal_object.PrintStoredData()

	wal_object_2 := new(wal.Wal)
	wal_object_2.LoadWal(wal.SEGMENT_FILE_PATH)
}
