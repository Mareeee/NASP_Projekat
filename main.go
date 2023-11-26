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
	// wal_object_2.LoadAllRecords()

	// wal_object_2.WriteJson()

	wal_object_2.Wal()
	// wal_object_2.AddRecord("zoran", "5")
	// wal_object_2.AddRecord("goran", "24400")
	// wal_object_2.AddRecord("serbijangejmsbl", "032")
	// wal_object_2.AddRecord("dragan", "5")
	// wal_object_2.AddRecord("milan", "24400")

	wal_object_2.LoadAllRecords()

	// Verify by loading and printing the content
	// wal_object_2.LoadJson()
}
