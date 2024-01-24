package sstable

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"main/bloom-filter"
	"main/record"
	"os"
	"strconv"
)

type SSTableOptions struct {
	NumberOfSSTables int `json:"NumberOfSSTables"`
	IndexInterval    int `json:"IndexInterval"`
	SummaryInterval  int `json:"SummaryInterval"`
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
//	   +---------------------+---------+--------------------+--------+-----------+
//	   |First Key Length (8B)|First Key|Last Key Length (8B)|Last Key|Offset (8B)|
//	   +---------------------+---------+--------------------+--------+-----------+
type SummaryEntry struct {
	firstKey string
	lastKey  string
	offset   int64 //  offset s kog citamo iz indexa
}

func NewSSTable(allRecords []record.Record) {
	sst := new(SSTable)

	sst.options.LoadJson()
	sst.options.NumberOfSSTables++

	sst.writeDataIndexSummary(allRecords)
	sst.createFilter(allRecords)
	sst.createMetaData(allRecords)

	sst.options.WriteJson()
}

func (s *SSTable) writeDataIndexSummary(allRecords []record.Record) {
	count := 0
	offset := 0

	var index []IndexEntry

	for _, record := range allRecords {
		// ovde se pravi data, upisujem sve rekorde
		s.WriteRecord(record, DATA_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables)+".db")

		if count%s.options.IndexInterval == 0 {
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
	f, _ := os.OpenFile(INDEX_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	for _, entry := range index {
		keyBytes := []byte(entry.key)
		binary.Write(f, binary.BigEndian, int64(len(keyBytes)))
		f.Write(keyBytes)
		binary.Write(f, binary.BigEndian, entry.offset)
	}
}

func (s *SSTable) buildSummary(index []IndexEntry) []SummaryEntry {
	var summary []SummaryEntry

	offset := 0

	for i := 0; i < len(index); i += s.options.SummaryInterval {
		endIndex := i + s.options.SummaryInterval - 1

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
	f, _ := os.OpenFile(SUMMARY_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
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

func Search(key string) string {
	options := new(SSTableOptions)
	options.LoadJson()
	fileNumber := findSSTableNumber(key, options.NumberOfSSTables) // broj tabele u kojoj je zapis
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

	record := loadRecord(fileNumber, key, uint64(valueOffset))
	if record == nil {
		return "Inputed key does not exist!"
	}
	return record.Key
}

// trazimo SSTabelu gde gde je sa velikom verovatnocom nas zapis
func findSSTableNumber(key string, numOfSSTables int) int {
	for i := numOfSSTables; i > 0; i-- {
		bf := new(bloom.BloomFilter)
		bf.LoadBloomFilter(FILTER_FILE_PATH + strconv.Itoa(i) + ".bin")

		if bf.CheckElement(key) {
			return i
		}
	}
	return -1
}

func loadAndFindIndexOffset(fileNumber int, key string) (string, int64) {
	f, _ := os.Open(SUMMARY_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	defer f.Close()

	var initialOffset int64 = 0

	for {
		f.Seek(initialOffset, io.SeekStart)

		firstKeySizeBytes := make([]byte, 8)
		_, readErr := f.Read(firstKeySizeBytes)
		if readErr == io.EOF { // proveravamo da li smo dosli do kraja fajla
			return "", -1
		}
		firstKeySize := binary.BigEndian.Uint64(firstKeySizeBytes)

		firstKeyBytes := make([]byte, firstKeySize)
		f.Read(firstKeyBytes)
		firstKey := string(firstKeyBytes)

		lastKeySizeBytes := make([]byte, 8)
		f.Read(lastKeySizeBytes)
		lastKeySize := binary.BigEndian.Uint64(lastKeySizeBytes)

		lastKeyBytes := make([]byte, lastKeySize)
		f.Read(lastKeyBytes)
		lastKey := string(lastKeyBytes)

		offsetBytes := make([]byte, 8)
		f.Read(offsetBytes)
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if key >= firstKey && key <= lastKey {
			return lastKey, offset
		}

		initialOffset += 24 + int64(firstKeySize) + int64(lastKeySize)
	}
}

func loadAndFindValueOffset(fileNumber int, summaryOffset uint64, key string, lastKey string) int64 {
	f, _ := os.Open(INDEX_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	defer f.Close()

	for {
		f.Seek(int64(summaryOffset), io.SeekStart)

		keySizeBytes := make([]byte, 8)
		_, readErr := f.Read(keySizeBytes)
		if readErr == io.EOF { // provera da li smo dosli do kraja fajla
			break
		}
		keySize := binary.BigEndian.Uint64(keySizeBytes)

		keyBytes := make([]byte, keySize)
		f.Read(keyBytes)
		foundKey := string(keyBytes)

		offsetBytes := make([]byte, 8)
		f.Read(offsetBytes)
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if key >= foundKey {
			return offset
		}

		// ako prodjemo citav interval na indeksnoj tabeli, vracamo gresku da nismo nasli kljuc
		if foundKey == lastKey {
			break
		}

		summaryOffset += 16 + keySize
	}
	return -1
}

func loadRecord(fileNumber int, key string, valueOffset uint64) *record.Record {
	f, _ := os.Open(DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	defer f.Close()

	for {
		f.Seek(int64(valueOffset), io.SeekStart)

		headerBytes := make([]byte, 29)
		_, readErr := f.Read(headerBytes)
		if readErr == io.EOF { // provera da li smo stigli do kraja fajla
			return nil
		}

		crc32 := binary.BigEndian.Uint32(headerBytes[0:4])
		timestamp := int64(binary.BigEndian.Uint64(headerBytes[4:12]))
		tombstone := headerBytes[12] != 0
		keySize := int64(binary.BigEndian.Uint64(headerBytes[13:21]))
		valueSize := int64(binary.BigEndian.Uint64(headerBytes[21:29]))

		keyBytes := make([]byte, keySize)
		f.Read(keyBytes)
		loadedKey := string(keyBytes)

		value := make([]byte, valueSize)
		f.Read(value)

		checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, key, value)
		if checkCrc32 != crc32 { // potrebno je pri ucitavanju proveriti da li je doslo do promene zapisa
			continue
		}

		// uporedjujemo ucitan kljuc iz rekorda sa korisnikovim kljucem, ako su isti vracamo vrednost rekroda
		if loadedKey == key {
			return record.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, loadedKey, value)
		}

		valueOffset += 29 + uint64(keySize) + uint64(valueSize)
	}
}

func (s *SSTable) createFilter(allRecords []record.Record) {
	s.filter = new(bloom.BloomFilter)
	s.filter.NewBloomFilter(len(allRecords), 0.01)

	for _, record := range allRecords {
		s.filter.AddElement(record.Key)
	}

	s.filter.WriteToBinFile(FILTER_FILE_PATH + strconv.Itoa(s.options.NumberOfSSTables) + ".bin")
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

func (o *SSTableOptions) LoadJson() {
	jsonData, _ := os.ReadFile(SSTABLE_CONFIG_FILE_PATH)

	json.Unmarshal(jsonData, &o)
}

func (o *SSTableOptions) WriteJson() {
	jsonData, _ := json.MarshalIndent(o, "", "  ")

	os.WriteFile(SSTABLE_CONFIG_FILE_PATH, jsonData, 0644)
}
