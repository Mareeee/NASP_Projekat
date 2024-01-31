package test

import (
	"main/record"
	"math/rand"
	"time"
)

func GenerateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())

	charSet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	result := make([]byte, length)

	for i := 0; i < length; i++ {
		randomIndex := rand.Intn(len(charSet))
		result[i] = charSet[randomIndex]
	}

	return string(result)
}

func GenerateRandomRecords(kvlength int) []record.Record {
	key := GenerateRandomString(kvlength)
	value := GenerateRandomString(kvlength)
	var listOfRecords []record.Record
	numberOfRecords := 100000
	for i := 0; i < numberOfRecords; i += 1 {
		record := record.NewRecord(key, []byte(value), false)
		listOfRecords = append(listOfRecords, *record)
	}
	return listOfRecords
}
