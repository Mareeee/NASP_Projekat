package sstable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"main/bloom-filter"
	"main/merkle"
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
	filter   *bloom.BloomFilter
	metadata *merkle.MerkleTree
	options  SSTableOptions
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
	f, err := os.OpenFile(INDEX_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening or creating index file:", err)
		return
	}
	defer f.Close()

	for _, entry := range index {
		keyBytes := []byte(entry.key)

		err := binary.Write(f, binary.BigEndian, int64(len(keyBytes)))
		if err != nil {
			fmt.Println("Error writing key size:", err)
			return
		}
		_, err = f.Write(keyBytes)
		if err != nil {
			fmt.Println("Error writing key:", err)
			return
		}

		err = binary.Write(f, binary.BigEndian, entry.offset)
		if err != nil {
			fmt.Println("Error writing offset:", err)
			return
		}
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
	f, err := os.OpenFile(SUMMARY_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening or creating summary file:", err)
		return
	}
	defer f.Close()

	for _, entry := range summary {
		firstKeyBytes := []byte(entry.firstKey)
		lastKeyBytes := []byte(entry.lastKey)

		err := binary.Write(f, binary.BigEndian, int64(len(firstKeyBytes)))
		if err != nil {
			fmt.Println("Error writing first key size:", err)
			return
		}
		_, err = f.Write(firstKeyBytes)
		if err != nil {
			fmt.Println("Error writing first key:", err)
			return
		}

		err = binary.Write(f, binary.BigEndian, int64(len(lastKeyBytes)))
		if err != nil {
			fmt.Println("Error writing last key size:", err)
			return
		}
		_, err = f.Write(lastKeyBytes)
		if err != nil {
			fmt.Println("Error writing last key:", err)
			return
		}

		err = binary.Write(f, binary.BigEndian, entry.offset)
		if err != nil {
			fmt.Println("Error writing offset:", err)
			return
		}
	}
}

func Search(key string) (*record.Record, error) {
	options := new(SSTableOptions)
	options.LoadJson()
	fileNumber := findSSTableNumber(key, options.NumberOfSSTables) // broj tabele u kojoj je zapis
	if fileNumber == -1 {
		return nil, errors.New("Inputed key does not exist in SSTable!")
	}

	lastKey, offset := loadAndFindIndexOffset(fileNumber, key)
	if lastKey == "" && offset == -1 {
		return nil, errors.New("Inputed key does not exist!")
	}

	valueOffset := loadAndFindValueOffset(fileNumber, uint64(offset), key, lastKey)
	if valueOffset == -1 {
		return nil, errors.New("Inputed key does not exist!")
	}

	record := loadRecord(fileNumber, key, uint64(valueOffset))
	if record == nil {
		return nil, errors.New("Inputed key does not exist!")
	}
	return record, nil
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
	f, err := os.Open(SUMMARY_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return "", -1
	}
	defer f.Close()

	var initialOffset int64 = 0

	for {
		_, seekErr := f.Seek(initialOffset, io.SeekStart)
		if seekErr != nil {
			fmt.Println("Error seeking in file:", seekErr)
			return "", -1
		}

		firstKeySizeBytes := make([]byte, 8)
		_, readErr := f.Read(firstKeySizeBytes)
		if readErr == io.EOF {
			fmt.Println("End of file reached.")
			return "", -1
		} else if readErr != nil {
			fmt.Println("Error reading first key size:", readErr)
			return "", -1
		}
		firstKeySize := binary.BigEndian.Uint64(firstKeySizeBytes)

		firstKeyBytes := make([]byte, firstKeySize)
		_, readErr = f.Read(firstKeyBytes)
		if readErr != nil {
			fmt.Println("Error reading first key:", readErr)
			return "", -1
		}
		firstKey := string(firstKeyBytes)

		lastKeySizeBytes := make([]byte, 8)
		_, readErr = f.Read(lastKeySizeBytes)
		if readErr != nil {
			fmt.Println("Error reading last key size:", readErr)
			return "", -1
		}
		lastKeySize := binary.BigEndian.Uint64(lastKeySizeBytes)

		lastKeyBytes := make([]byte, lastKeySize)
		_, readErr = f.Read(lastKeyBytes)
		if readErr != nil {
			fmt.Println("Error reading last key:", readErr)
			return "", -1
		}
		lastKey := string(lastKeyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			fmt.Println("Error reading offset:", readErr)
			return "", -1
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if key >= firstKey && key <= lastKey {
			return lastKey, offset
		}

		initialOffset += 24 + int64(firstKeySize) + int64(lastKeySize)
	}
}

func loadAndFindValueOffset(fileNumber int, summaryOffset uint64, key string, lastKey string) int64 {
	f, err := os.Open(INDEX_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return -1
	}
	defer f.Close()

	for {
		_, seekErr := f.Seek(int64(summaryOffset), io.SeekStart)
		if seekErr != nil {
			fmt.Println("Error seeking in file:", seekErr)
			return -1
		}

		keySizeBytes := make([]byte, 8)
		_, readErr := f.Read(keySizeBytes)
		if readErr == io.EOF {
			break
		} else if readErr != nil {
			fmt.Println("Error reading key size:", readErr)
			return -1
		}
		keySize := binary.BigEndian.Uint64(keySizeBytes)

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			fmt.Println("Error reading key:", readErr)
			return -1
		}
		foundKey := string(keyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			fmt.Println("Error reading offset:", readErr)
			return -1
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if key >= foundKey {
			return offset
		}

		if foundKey == lastKey {
			break
		}

		summaryOffset += 16 + keySize
	}

	fmt.Println("Key not found.")
	return -1
}

func loadRecord(fileNumber int, key string, valueOffset uint64) *record.Record {
	f, err := os.Open(DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		fmt.Println("Error opening data file:", err)
		return nil
	}
	defer f.Close()

	for {
		_, seekErr := f.Seek(int64(valueOffset), io.SeekStart)
		if seekErr != nil {
			fmt.Println("Error seeking in data file:", seekErr)
			return nil
		}

		headerBytes := make([]byte, 29)
		_, readErr := f.Read(headerBytes)
		if readErr == io.EOF {
			return nil
		} else if readErr != nil {
			fmt.Println("Error reading header:", readErr)
			return nil
		}

		crc32 := binary.BigEndian.Uint32(headerBytes[0:4])
		timestamp := int64(binary.BigEndian.Uint64(headerBytes[4:12]))
		tombstone := headerBytes[12] != 0
		keySize := int64(binary.BigEndian.Uint64(headerBytes[13:21]))
		valueSize := int64(binary.BigEndian.Uint64(headerBytes[21:29]))

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			fmt.Println("Error reading key:", readErr)
			return nil
		}
		loadedKey := string(keyBytes)

		value := make([]byte, valueSize)
		_, readErr = f.Read(value)
		if readErr != nil {
			fmt.Println("Error reading value:", readErr)
			return nil
		}

		checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, key, value)
		if checkCrc32 != crc32 {
			continue
		}

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
	var allRecordsBytes [][]byte
	for _, record := range allRecords {
		allRecordsBytes = append(allRecordsBytes, record.ToBytes())
	}
	s.metadata = merkle.NewMerkleTree(allRecordsBytes)
	//s.metadata.WriteToBinFile()
}

func (s *SSTable) WriteRecord(record record.Record, filepath string) {
	recordBytes := record.ToBytes()

	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening or creating file:", err)
		return
	}
	defer f.Close()

	_, writeErr := f.Write(recordBytes)
	if writeErr != nil {
		fmt.Println("Error writing record to file:", writeErr)
	}
}

func (o *SSTableOptions) LoadJson() {
	jsonData, err := os.ReadFile(SSTABLE_CONFIG_FILE_PATH)
	if err != nil {
		fmt.Println("Error reading JSON file:", err)
		return
	}

	err = json.Unmarshal(jsonData, o)
	if err != nil {
		fmt.Println("Error unmarshalling JSON data:", err)
	}
}

func (o *SSTableOptions) WriteJson() {
	jsonData, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling JSON data:", err)
		return
	}

	err = os.WriteFile(SSTABLE_CONFIG_FILE_PATH, jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing JSON file:", err)
	}
}
