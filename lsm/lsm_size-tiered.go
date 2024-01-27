package lsm

import (
	"fmt"
	"main/record"
	"main/sstable"
	"os"
	"strconv"
	"strings"
)

func SizeTiered() {
	// ove parametre cemo ucitavati iz globalnog config fajla koji cemo naknadno napraviti
	numberOfLevels := 2
	maxTabels := 2

	var currentLevelSSTables []string
	for level := 1; level <= numberOfLevels; level++ {
		currentLevelSSTables = findSSTable(strconv.Itoa(level))

		if len(currentLevelSSTables) == maxTabels {
			for i := 0; i < len(currentLevelSSTables); i += 2 {
				records1 := currentLevelSSTables[i]
				records2 := currentLevelSSTables[i+1]
				resultRecords := mergeTables(records1, records2, level+1)

				sstable.NewSSTable(resultRecords, level+1)
			}
		}
	}
}

func findSSTable(level string) []string {
	var currentLevelSSTables []string

	files, err := os.ReadDir(SSTABLE_DIRECTORY)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return nil
	}

	for _, file := range files {
		if strings.Contains(file.Name(), "sstable_data") {
			sstable_tokens := strings.Split(file.Name(), "_")
			if sstable_tokens[1] == level {
				currentLevelSSTables = append(currentLevelSSTables, file.Name())
			}
		}
	}

	return currentLevelSSTables
}

func mergeTables(records1, records2 string, level int) []record.Record {
	var result_records []record.Record

	allRecords1, _ := record.LoadRecordsFromFile("data/sstable/" + records1)
	allRecords2, _ := record.LoadRecordsFromFile("data/sstable/" + records2)

	counterRecords1 := 0
	counterRecords2 := 0

	var record1 *record.Record
	var record2 *record.Record
	for counterRecords1 != len(allRecords1) || counterRecords2 != len(allRecords2) {
		if counterRecords1 != len(allRecords1) {
			record1 = allRecords1[counterRecords1]
		}
		if counterRecords2 != len(allRecords2) {
			record2 = allRecords2[counterRecords2]
		}

		if record1.Key == record2.Key {
			fmt.Println("record1.Key:", record1.Key, " record1.Value:", record1.Value)
			fmt.Println("record2.Key:", record2.Key, " record2.Value:", record2.Value)
			record := record.GetNewerRecord(*record1, *record2)
			result_records = append(result_records, record)
			counterRecords1++
			counterRecords2++
		} else if record1.Key > record2.Key {
			if counterRecords2 == len(allRecords2) {
				result_records = append(result_records, *record1)
				counterRecords1++
			} else {
				result_records = append(result_records, *record2)
				counterRecords2++
			}
		} else if record1.Key < record2.Key {
			if counterRecords1 == len(allRecords1) {
				result_records = append(result_records, *record2)
				counterRecords2++
			} else {
				result_records = append(result_records, *record1)
				counterRecords1++
			}
		}
	}

	return result_records
}
