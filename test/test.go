package test

import (
	"main/record"
	"strconv"
)

func GenerateRandomRecords(kvlength int) []record.Record {
	// key := GenerateRandomString(kvlength)
	value := []byte("BajoJajo")
	var listOfRecords []record.Record
	numberOfRecords := 160
	for i := 0; i < numberOfRecords; i += 1 {
		record := record.NewRecord(strconv.Itoa(i), value, false)
		listOfRecords = append(listOfRecords, *record)
	}
	return listOfRecords
}
