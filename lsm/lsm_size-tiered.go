package lsm

import (
	"fmt"
	"main/config"
	"main/record"
	"main/sstable"
	"os"
	"sort"
	"strconv"
	"strings"
)

func Compact(cfg *config.Config, keyDictionary *map[int]string) bool {
	SSTablesLvl1 := findSSTable("1")
	if len(SSTablesLvl1) < 2 {
		return false
	}
	if cfg.CompactBy == "byte" {
		byteSizeOfCurrentLevelSSTables, _ := calculateSizeOfSSTables(SSTablesLvl1)
		if byteSizeOfCurrentLevelSSTables >= cfg.MaxBytesSSTables {
			if cfg.CompactType == "size_tiered" {
				SizeTiered(cfg, keyDictionary)
				return true
			} else if cfg.CompactType == "level" {
				Level(cfg)
				return true
			}
		}
	} else if cfg.CompactBy == "amount" {
		if len(SSTablesLvl1) >= cfg.MaxTabels {
			if cfg.CompactType == "size_tiered" {
				SizeTiered(cfg, keyDictionary)
				return true
			} else if cfg.CompactType == "level" {
				Level(cfg)
				return true
			}
		}
	}
	return false
}

func Level(cfg *config.Config) {
	var currentLevelSSTables []string
	for level := 1; level < cfg.NumberOfLevels; level++ {
		currentLevelSSTables = findSSTable(strconv.Itoa(level))
		if len(currentLevelSSTables) < 2 { // ne radimo kompakciju za manje od 2 sstabele
			return
		}
		path := config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level+1) + "_sstable_data_" + strconv.Itoa(cfg.NumberOfSSTables-len(currentLevelSSTables)+1) + ".db"
		if cfg.CompactBy == "byte" {
			byteSizeOfCurrentLevelSSTables, _ := calculateSizeOfSSTables(currentLevelSSTables)
			if byteSizeOfCurrentLevelSSTables >= cfg.MaxBytesSSTables {
				LeveledMergeSSTables(currentLevelSSTables, path)
			} else {
				return // nema uslova za kompakciju
			}
		} else if cfg.CompactBy == "amount" {
			if len(currentLevelSSTables) >= cfg.MaxTabels {
				LeveledMergeSSTables(currentLevelSSTables, path)
			} else {
				return // nema uslova za kompakciju
			}
		}
	}
}

func SizeTiered(cfg *config.Config, keyDictionary *map[int]string) {
	var currentLevelSSTables []string
	// prolazak kroz nivoe sstabela
	for level := 1; level < cfg.NumberOfLevels; level++ {
		currentLevelSSTables = findSSTable(strconv.Itoa(level))
		if len(currentLevelSSTables) < 2 {
			return
		}
		path := config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level+1) + "_sstable_data_" + strconv.Itoa(cfg.NumberOfSSTables-len(currentLevelSSTables)+1) + ".db"
		if cfg.CompactBy == "byte" {
			byteSizeOfCurrentLevelSSTables, _ := calculateSizeOfSSTables(currentLevelSSTables)
			if byteSizeOfCurrentLevelSSTables >= cfg.MaxBytesSSTables*level {
				SizeTieredMergeSSTables(currentLevelSSTables, path, keyDictionary)
			} else {
				return // nema uslova za kompakciju
			}
		} else if cfg.CompactBy == "amount" {
			if len(currentLevelSSTables) >= cfg.MaxTabels*level {
				SizeTieredMergeSSTables(currentLevelSSTables, path, keyDictionary)
			} else {
				return // nema uslova za kompakciju
			}
		}

		deleteOldTables(currentLevelSSTables, level)
		cfg.NumberOfSSTables -= len(currentLevelSSTables) - 1
		cfg.WriteConfig()
		sstable.WriteDataIndexSummaryLSM(path, level+1, *cfg, keyDictionary)
	}
}

func LeveledMergeSSTables(SSTables []string, filepath string) bool {
	return false
}

// vraca velicinu svih sstabeli na nekom nivou
func calculateSizeOfSSTables(SSTables []string) (int, error) {
	totalSize := int64(0)
	for i := 0; i < len(SSTables); i++ {
		fileInfo, err := os.Stat(config.SSTABLE_DIRECTORY + SSTables[i])
		if err != nil {
			return 0, err
		}
		totalSize += fileInfo.Size()
	}

	return int(totalSize), nil
}

func deleteOldTables(oldSSTables []string, level int) {
	for i := 0; i < len(oldSSTables); i++ {
		sstableIndex := strings.Split(strings.Split(oldSSTables[i], "_")[4], ".")[0]
		prefix := config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level)
		os.Remove(prefix + "_sstable_data_" + sstableIndex + ".db")
		os.Remove(prefix + "_sstable_filter_" + sstableIndex + ".bin")
		os.Remove(prefix + "_sstable_index_" + sstableIndex + ".db")
		os.Remove(prefix + "_sstable_summary_" + sstableIndex + ".db")
		os.Remove(prefix + "_sstable_metadata_" + sstableIndex + ".bin")
	}
}

func findSSTable(level string) []string {
	var currentLevelSSTables []string

	files, err := os.ReadDir(config.SSTABLE_DIRECTORY)
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

func SizeTieredMergeSSTables(SSTables []string, filepath string, keyDictionary *map[int]string) bool {
	SSTableFiles := []*os.File{}
	dataFile, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return false
	}
	defer dataFile.Close()

	// ako dodje do situacije da se jedna sstablea skroz isprazni, onda njen indeks samo brisem is SSTables
	// ucitavam sve pokazivace na fajlove trenutnih sstabela
	for i := 0; i < len(SSTables); i++ {
		file, _ := os.Open(config.SSTABLE_DIRECTORY + SSTables[i])
		SSTableFiles = append(SSTableFiles, file)
	}

	allRecords := record.LoadAllRecordsFromFiles(SSTableFiles, keyDictionary)

	// loop dok postoje podaci
	for len(SSTableFiles) > 0 {

		// idemo kroz rekorde i nabavljamo one koji nisu obrisani
		for i := 0; i < len(SSTableFiles); i++ {
			for {
				if allRecords[i].Tombstone {
					rekord, err := record.LoadRecordFromFile(*SSTableFiles[i], keyDictionary)
					if err != nil {
						// edgecase kada prodjemo kroz sve rekorde iz jedne sstabele
						allRecords, SSTableFiles = deleteFromArrays(allRecords, SSTableFiles, i)
					} else {
						allRecords[i] = rekord
					}
				} else {
					break
				}
			}
		}

		rec := findSuitableRecord(allRecords)
		index := findRecordIndex(allRecords, rec)

		recordBytes := rec.ToBytesSSTable(keyDictionary)

		_, err := dataFile.Write(recordBytes)
		if err != nil {
			fmt.Println("Error writing record to", filepath)
			return false
		}

		rekord, err := record.LoadRecordFromFile(*SSTableFiles[index], keyDictionary)
		if err != nil {
			allRecords, SSTableFiles = deleteFromArrays(allRecords, SSTableFiles, index)
		} else {
			allRecords[index] = rekord
		}
	}

	return true
}

func findRecordIndex(allRecords []record.Record, target record.Record) int {
	for i := 0; i < len(allRecords); i++ {
		if record.IsSimilar(allRecords[i], target) {
			return i
		}
	}
	return -1
}

func findSuitableRecord(allRecords []record.Record) record.Record {
	sort.Slice(allRecords, func(i, j int) bool {
		// sortiramo leksikografski
		if allRecords[i].Key != allRecords[j].Key {
			return allRecords[i].Key < allRecords[j].Key
		}
		// sortira po tajmstempu
		return allRecords[i].Timestamp > allRecords[j].Timestamp
	})

	return allRecords[0]
}

func findSmallestRecordIndex(allRecords []record.Record) int {
	smallestRecord := allRecords[0]
	index := 0

	for i, record := range allRecords[1:] {
		if record.Key < smallestRecord.Key {
			smallestRecord = record
			index = i + 1
		}
	}

	return index
}

func allRecordsHaveSameKey(records []record.Record) bool {
	firstKey := records[0].Key

	for _, record := range records[1:] {
		if record.Key != firstKey {
			return false
		}
	}

	return true
}

func deleteFromArrays(allRecords []record.Record, allFiles []*os.File, index int) ([]record.Record, []*os.File) {
	allFiles[index].Close()
	allRecordsResult := append(allRecords[:index], allRecords[index+1:]...)
	allFilesResult := append(allFiles[:index], allFiles[index+1:]...)
	return allRecordsResult, allFilesResult
}

// func mergeTables(records1, records2 string, level int) []record.Record {
// 	var result_records []record.Record

// 	allRecords1, _ := record.LoadRecordsFromFile("data/sstable/" + records1, )
// 	allRecords2, _ := record.LoadRecordsFromFile("data/sstable/" + records2)

// 	counterRecords1 := 0
// 	counterRecords2 := 0

// 	var record1 *record.Record
// 	var record2 *record.Record
// 	for {
// 		// ako brojaci nisu stigli do kraja, uzimamo sledece rekorde
// 		if counterRecords1 < len(allRecords1) {
// 			record1 = allRecords1[counterRecords1]
// 		} else {
// 			record1 = nil
// 		}
// 		if counterRecords2 < len(allRecords2) {
// 			record2 = allRecords2[counterRecords2]
// 		} else {
// 			record2 = nil
// 		}

// 		// ako su svi rekordi iz jedne sstabele upisani samo nastavi upis iz druge
// 		if record1 == nil && record2 == nil {
// 			break
// 		}
// 		if record1 == nil && record2 != nil {
// 			result_records = append(result_records, *record2)
// 			counterRecords2++
// 			continue
// 		} else if record2 == nil && record1 != nil {
// 			result_records = append(result_records, *record1)
// 			counterRecords1++
// 			continue
// 		}

// 		// ako su oba rekorda logicki obrisana, ucitavamo nove rekorde iz liste, a ako je samo jedan obrisan ucitavamo novi rekord iz njegove liste
// 		if record1.Tombstone && record2.Tombstone {
// 			counterRecords1++
// 			counterRecords2++
// 			continue
// 		} else if record1.Tombstone && !record2.Tombstone {
// 			counterRecords1++
// 			continue
// 		} else if !record1.Tombstone && record2.Tombstone {
// 			counterRecords2++
// 			continue
// 		}

// 		// poredimo kljuceve i upisujemo noviji rekord po timestampu
// 		if record1.Key == record2.Key {
// 			record := record.GetNewerRecord(*record1, *record2)
// 			result_records = append(result_records, record)
// 			counterRecords1++
// 			counterRecords2++
// 		} else if record1.Key > record2.Key { // upisujemo rekord sa manjim kljucem (leksikografski)
// 			if counterRecords2 == len(allRecords2) {
// 				result_records = append(result_records, *record1)
// 				counterRecords1++
// 			} else {
// 				result_records = append(result_records, *record2)
// 				counterRecords2++
// 			}
// 		} else if record1.Key < record2.Key {
// 			if counterRecords1 == len(allRecords1) {
// 				result_records = append(result_records, *record2)
// 				counterRecords2++
// 			} else {
// 				result_records = append(result_records, *record1)
// 				counterRecords1++
// 			}
// 		}
// 	}

// 	return result_records
// }
