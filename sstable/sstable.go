package sstable

import (
	"encoding/binary"
	"main/bloom-filter"
	"main/config"
	"main/record"
	"os"
	"strconv"
)

type SSTableOptions struct {
	config config.Config
}

type SSTable struct {
	filter *bloom.BloomFilter
	// metadata *merkle.MerkleTree
	options SSTableOptions
}

//		INDEX ENTRY NA DISKU
//	   +----------------------+----------+---------------+
//	   |    Key Length (8B)   |    Key   |   Offset (8B) |
//	   +----------------------+----------+---------------+
type IndexEntry struct {
	key    string
	offset int64 // offset sa kog citamo iz data
}

//		SUMMARY ENTRY NA DISKU
//	   +---------------------+---------+--------------------+--------+---------------------+--------------------+----------------+
//	   |First Key Length (8B)|First Key|Last Key Length (8B)|Last Key|First Key Offset (8B)|Last Key Offset (8B)|Num Entries (8B)|
//	   +---------------------+---------+--------------------+--------+---------------------+--------------------+----------------+
type SummaryEntry struct {
	firstKey string
	lastKey  string
	offset   int64 //  offset s kog citamo iz indexa
}

func NewSSTable(allRecords []record.Record, level int) {
	sst := new(SSTable)

	config.LoadConfig(&sst.options.config)
	sst.options.config.NumberOfSSTables++

	sst.writeDataIndexSummary(allRecords, level)
	sst.createFilter(allRecords)
	sst.createMetaData(allRecords)

	sst.options.config.WriteConfig()
}

func (s *SSTable) writeDataIndexSummary(allRecords []record.Record, level int) {
	count := 0
	offset := 0

	var index []IndexEntry

	for _, record := range allRecords {
		// ovde se pravi data, upisujem sve rekorde
		s.WriteRecord(record, config.DATA_FILE_PATH+"lvl_"+strconv.Itoa(level)+"_sstable_data_"+strconv.Itoa(s.options.config.NumberOfSSTables)+".db")

		if count%s.options.config.IndexInterval == 0 {
			index = append(index, IndexEntry{key: record.Key, offset: int64(offset)})
		}
		count++
		offset += len(record.ToBytes())
	}

	s.writeIndex(index)
	summary := s.buildSummary(index)
	s.writeSummaryToFile(summary)
}

func (s *SSTable) writeIndex(index []IndexEntry) {
	f, _ := os.OpenFile(config.INDEX_FILE_PATH+strconv.Itoa(s.options.config.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	for _, entry := range index {
		keyBytes := []byte(entry.key)
		binary.Write(f, binary.BigEndian, int64(len(keyBytes)))
		f.Write(keyBytes)
		binary.Write(f, binary.BigEndian, entry.offset)
	}
}

func loadAndFindValueOffset(fileNumber int, summaryOffset uint64, key string, lastKey string) int64 {
	f, _ := os.Open(config.INDEX_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	for {
		// provera da li smo dosli do kraja fajla
		if int(summaryOffset)+8 > len(data) {
			break
		}

		keySize := binary.BigEndian.Uint64(data[summaryOffset : summaryOffset+8])
		foundKey := string(data[summaryOffset+8 : summaryOffset+8+keySize])

		if key >= foundKey {
			return int64(binary.BigEndian.Uint64(data[summaryOffset+8+keySize : summaryOffset+16+keySize]))
		}

		// ako prodjemo citav interval na indeksnoj tabeli, vracamo gresku da nismo nasli kljuc
		if foundKey == lastKey {
			break
		}
		summaryOffset = summaryOffset + 16 + keySize
	}
	return -1
}

func (s *SSTable) buildSummary(index []IndexEntry) []SummaryEntry {
	var summary []SummaryEntry

	offset := 0

	for i := 0; i < len(index); i += s.options.config.SummaryInterval {
		endIndex := i + s.options.config.SummaryInterval - 1

		if endIndex >= len(index) {
			endIndex = len(index) - 1
		}

		summaryEntry := SummaryEntry{
			firstKey: index[i].key,
			lastKey:  index[endIndex].key,
			offset:   int64(offset),
		}

		offset = 16 + len([]byte(index[i].key))
		summary = append(summary, summaryEntry)
	}

	return summary
}

func (s *SSTable) writeSummaryToFile(summary []SummaryEntry) {
	f, _ := os.OpenFile(config.SUMMARY_FILE_PATH+strconv.Itoa(s.options.config.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	for _, entry := range summary {
		firstKeyBytes := []byte(entry.firstKey)
		lastKeyBytes := []byte(entry.lastKey)

		binary.Write(f, binary.BigEndian, int64(len(firstKeyBytes)))
		f.Write(firstKeyBytes)

		binary.Write(f, binary.BigEndian, int64(len(lastKeyBytes)))
		f.Write(lastKeyBytes)

		binary.Write(f, binary.BigEndian, entry.offset)
	}
}

func loadAndFindIndexOffset(fileNumber int, key string) (string, int64) {
	f, _ := os.Open(config.SUMMARY_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	initialOffset := uint64(0)
	for {
		// provera da li smo stigli do kraja fajla
		if int(initialOffset)+8 > len(data) {
			return "", -1
		}

		firstKeySize := binary.BigEndian.Uint64(data[initialOffset : initialOffset+8])
		firstKey := string(data[initialOffset+8 : initialOffset+8+firstKeySize])

		lastKeySize := binary.BigEndian.Uint64(data[initialOffset+8+firstKeySize : initialOffset+16+firstKeySize])
		lastKey := string(data[initialOffset+16+firstKeySize : initialOffset+16+firstKeySize+lastKeySize])

		offset := int64(binary.BigEndian.Uint64(data[initialOffset+16+firstKeySize+lastKeySize : initialOffset+24+firstKeySize+lastKeySize]))

		initialOffset = initialOffset + 24 + firstKeySize + lastKeySize

		if key >= firstKey && key <= lastKey {
			return lastKey, offset
		}
	}
}

func (s *SSTable) createFilter(allRecords []record.Record) {
	s.filter = new(bloom.BloomFilter)
	s.filter.NewBloomFilter(len(allRecords), 0.01)

	for _, record := range allRecords {
		s.filter.AddElement(record.Key)
	}

	s.filter.WriteToBinFile(config.FILTER_FILE_PATH + strconv.Itoa(s.options.config.NumberOfSSTables) + ".bin")
}

func (s *SSTable) createMetaData(allRecords []record.Record) {
	// ovde treba da se svi rekordi prevedi u nizove nizova bajtova i da se od toga napravi merkle
	// serijalizacija
}

func (s *SSTable) WriteRecord(record record.Record, filepath string) {
	recordBytes := record.ToBytes()

	f, _ := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	f.Write(recordBytes)
}

func Search(key string) string {
	options := new(SSTableOptions)
	config.LoadConfig(&options.config)
	fileNumber := findSSTableNumber(key, options.config.NumberOfSSTables) // broj tabele u kojoj je zapis
	if fileNumber == -1 {
		return "Inputed key does not exist in SSTable!"
	}

	lastKey, offset := loadAndFindIndexOffset(fileNumber, key)
	if lastKey == "" && offset == -1 {
		return "Inputed key does not exist!"
	}

	valueOffset := loadAndFindValueOffset(fileNumber, uint64(offset), key, lastKey)
	if valueOffset == -1 {
		return "Inputed key does not exist!"
	}

	arrayOfBytesOfRecordValue := loadRecord(fileNumber, key, uint64(valueOffset))
	if arrayOfBytesOfRecordValue == nil {
		return "Inputed key does not exist!"
	}
	return string(arrayOfBytesOfRecordValue)
}

func loadRecord(fileNumber int, key string, valueOffset uint64) []byte {
	f, _ := os.Open(config.DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	for {
		// provera da li smo stigli do kraja fajla
		if int(valueOffset)+21 > len(data) {
			return nil
		}

		keySize := binary.BigEndian.Uint64(data[valueOffset+13 : valueOffset+21])
		valueSize := binary.BigEndian.Uint64(data[valueOffset+21 : valueOffset+29])
		loadedKey := []byte(data[valueOffset+29 : valueOffset+29+keySize])
		value := []byte(data[valueOffset+29+keySize : valueOffset+29+keySize+valueSize])

		// uporedjujemo ucitan kljuc iz rekorda sa korisnikovim kljucem, ako su isti vracamo vrednost rekroda
		if string(loadedKey) != key {
			valueOffset = valueOffset + 29 + keySize + valueSize
			continue
		}

		return value
	}
}

// trazimo SSTabelu gde gde je sa velikom verovatnocom nas zapis
func findSSTableNumber(key string, numOfSSTables int) int {
	for i := numOfSSTables; i > 0; i-- {
		bf := new(bloom.BloomFilter)
		bf.LoadBloomFilter(config.FILTER_FILE_PATH + strconv.Itoa(i) + ".bin")

		if bf.CheckElement(key) {
			return i
		}
	}
	return -1
}
