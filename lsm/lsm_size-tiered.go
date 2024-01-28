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
	// prolazak kroz nivoe sstabela
	for level := 1; level <= numberOfLevels; level++ {
		currentLevelSSTables = findSSTable(strconv.Itoa(level))
		// radimo kompakciju pod uslovom da je broj sstabela na nivou dostigao limit
		if len(currentLevelSSTables) >= maxTabels && len(currentLevelSSTables)%2 == 0 {
			for i := 0; i < len(currentLevelSSTables); i += 2 {
				records1 := currentLevelSSTables[i]
				records2 := currentLevelSSTables[i+1]
				resultRecords := mergeTables(records1, records2, level+1)

				sstable.NewSSTable(resultRecords, level+1)

				deleteOldTables(records1, records2, level)
			}
		}
	}
}

func deleteOldTables(records1, records2 string, level int) {
	sstableIndex1 := strings.Split(strings.Split(records1, "_")[4], ".")[0]
	sstableIndex2 := strings.Split(strings.Split(records2, "_")[4], ".")[0]
	listTables := []string{sstableIndex1, sstableIndex2}
	for i := 0; i <= 1; i++ {
		os.Remove("lvl_" + strconv.Itoa(level) + "_sstable_data_" + listTables[i] + ".db")
		os.Remove("_sstable_filter_" + listTables[i] + ".bin")
		os.Remove("_sstable_index_" + listTables[i] + ".db")
		os.Remove("_sstable_summary_" + listTables[i] + ".db")
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
	for {
		// ako brojaci nisu stigli do kraja, uzimamo sledece rekorde
		if counterRecords1 < len(allRecords1) {
			record1 = allRecords1[counterRecords1]
		} else {
			record1 = nil
		}
		if counterRecords2 < len(allRecords2) {
			record2 = allRecords2[counterRecords2]
		} else {
			record2 = nil
		}

		// ako su svi rekordi iz jedne sstabele upisani samo nastavi upis iz druge
		if record1 == nil && record2 == nil {
			break
		}
		if record1 == nil && record2 != nil {
			result_records = append(result_records, *record2)
			counterRecords2++
			continue
		} else if record2 == nil && record1 != nil {
			result_records = append(result_records, *record1)
			counterRecords1++
			continue
		}

		// ako su oba rekorda logicki obrisana, ucitavamo nove rekorde iz liste, a ako je samo jedan obrisan ucitavamo novi rekord iz njegove liste
		if record1.Tombstone && record2.Tombstone {
			counterRecords1++
			counterRecords2++
			continue
		} else if record1.Tombstone && !record2.Tombstone {
			counterRecords1++
			continue
		} else if !record1.Tombstone && record2.Tombstone {
			counterRecords2++
			continue
		}

		// poredimo kljuceve i upisujemo noviji rekord po timestampu
		if record1.Key == record2.Key {
			record := record.GetNewerRecord(*record1, *record2)
			result_records = append(result_records, record)
			counterRecords1++
			counterRecords2++
		} else if record1.Key > record2.Key { // upisujemo rekord sa manjim kljucem (leksikografski)
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
