package test

import (
	"fmt"
	"main/engine"
	"main/record"
	"math/rand"
	"strconv"
	"time"
)

func GenerateRandomRecordsForEvery100(engine engine.Engine) {
	//every 1000th record different key (100 different keys in a batch)
	value := []byte("IDEGAS")
	var listOfRecords []record.Record
	numberOfRecords := 100000
	j := 0
	for i := 0; i < numberOfRecords; i += 1 {
		if i%1000 == 0 {
			j++
		}
		record := record.NewRecord(strconv.Itoa(j), value, false)
		listOfRecords = append(listOfRecords, *record)
	}
	//shuffling records in random order
	shuffle(listOfRecords)
	//putting records
	for i := 0; i < 100000; i++ {
		engine.Put(listOfRecords[i].Key, listOfRecords[i].Value, false)
		fmt.Println(i)
	}
}

func GenerateRandomRecordsForEvery50000(engine engine.Engine) {
	//every 20th record different key (50000 different keys in a batch)
	value := []byte("IDEGAS")
	var listOfRecords []record.Record
	numberOfRecords := 100000
	j := 0
	for i := 0; i < numberOfRecords; i += 1 {
		if i%20 == 0 {
			j++
		}
		record := record.NewRecord(strconv.Itoa(j), value, false)
		listOfRecords = append(listOfRecords, *record)
	}
	//shuffling records in random order
	shuffle(listOfRecords)
	//putting records
	for i := 0; i < 100000; i++ {
		engine.Put(listOfRecords[i].Key, listOfRecords[i].Value, false)
		fmt.Println(i)
	}
}

// shuffling records in random order
func shuffle(records []record.Record) {
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	rng.Shuffle(len(records), func(i, j int) {
		records[i], records[j] = records[j], records[i]
	})
}
