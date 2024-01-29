package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"main/bloom-filter"
	"main/config"
	"main/merkle"
	"main/record"
	"os"
	"strconv"
)

type SSTable struct {
	filter   *bloom.BloomFilter
	metadata *merkle.MerkleTree
	config   config.Config
}

type IndexEntry struct {
	key    string
	offset int64 // offset sa kog citamo iz data
}

type SummaryEntry struct {
	firstKey string
	lastKey  string
	offset   int64 //  offset s kog citamo iz indexa
}

func LoadSSTable(fileNumber int) (*SSTable, error) {
	cfg := new(config.Config)
	err := config.LoadConfig(cfg)
	if err != nil {
		return nil, err
	}

	allRecords, err := record.LoadRecordsFromFile(config.DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")

	if err != nil {
		return nil, err
	}

	var allRecordsBytes [][]byte
	for _, record := range allRecords {
		allRecordsBytes = append(allRecordsBytes, record.ToBytes())
	}
	mtNew := merkle.NewMerkleTree(allRecordsBytes)

	mtFile := merkle.ReadFromBinFile(config.METADATA_FILE_PATH + strconv.Itoa(fileNumber) + ".bin")
	mtFileNode := merkle.DeserializeMerkleTree(mtFile)

	check := merkle.CompareMerkleTrees(mtFileNode, mtNew.Root)
	if !check {
		return nil, errors.New("Data has been altered!")
	}

	bf := new(bloom.BloomFilter)
	bf.LoadBloomFilter(config.FILTER_FILE_PATH + strconv.Itoa(fileNumber) + ".bin")

	sst := new(SSTable)
	sst.filter = bf
	sst.metadata = mtNew

	return sst, nil
}

func NewSSTable(allRecords []record.Record) (*SSTable, error) {
	sst := new(SSTable)
	cfg := new(config.Config)
	err := config.LoadConfig(cfg)
	if err != nil {
		return nil, err
	}
	sst.config = *cfg

	sst.config.NumberOfSSTables++

	sst.writeDataIndexSummary(allRecords)
	sst.createFilter(allRecords)
	sst.createMetaData(allRecords)

	err = sst.config.WriteConfig()
	if err != nil {
		return nil, err
	}

	return sst, nil
}

func (s *SSTable) writeDataIndexSummary(allRecords []record.Record) {
	count := 0
	offset := 0

	var index []IndexEntry

	for _, record := range allRecords {
		// ovde se pravi data, upisujem sve rekorde
		s.WriteRecord(record, config.DATA_FILE_PATH+strconv.Itoa(s.config.NumberOfSSTables)+".db")

		if count%s.config.IndexInterval == 0 {
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
	f, err := os.OpenFile(config.INDEX_FILE_PATH+strconv.Itoa(s.config.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
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

	for i := 0; i < len(index); i += s.config.SummaryInterval {
		endIndex := i + s.config.SummaryInterval - 1

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
	f, err := os.OpenFile(config.SUMMARY_FILE_PATH+strconv.Itoa(s.config.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
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
	cfg := new(config.Config)
	config.LoadConfig(cfg)

	fileNumber, err := findSSTableNumber(key, cfg.NumberOfSSTables) // broj tabele u kojoj je zapis
	if fileNumber == -1 && err == nil {
		return nil, errors.New("Key not found in any of SSTables!")
	} else if err != nil {
		return nil, err
	}

	lastKey, offset, err := loadAndFindIndexOffset(fileNumber, key)
	if err != nil {
		return nil, err
	}

	valueOffset, err := loadAndFindValueOffset(fileNumber, uint64(offset), key, lastKey)
	if err != nil {
		return nil, err
	}

	record, err := loadRecord(fileNumber, key, uint64(valueOffset))
	if err != nil {
		return nil, err
	}
	return record, nil
}

// trazimo SSTabelu gde gde je sa velikom verovatnocom nas zapis
func findSSTableNumber(key string, numOfSSTables int) (int, error) {
	for i := numOfSSTables; i > 0; i-- {
		sst, err := LoadSSTable(i)

		if err != nil {
			return -1, err
		}

		if sst.filter.CheckElement(key) {
			return i, nil
		}
	}
	return -1, nil
}

func loadAndFindIndexOffset(fileNumber int, key string) (string, int64, error) {
	f, err := os.Open(config.SUMMARY_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return "", -1, err
	}
	defer f.Close()

	var initialOffset int64 = 0

	for {
		_, seekErr := f.Seek(initialOffset, io.SeekStart)
		if seekErr != nil {
			return "", -1, seekErr
		}

		firstKeySizeBytes := make([]byte, 8)
		_, readErr := f.Read(firstKeySizeBytes)
		if readErr == io.EOF {
			return "", -1, readErr
		} else if readErr != nil {
			return "", -1, readErr
		}
		firstKeySize := binary.BigEndian.Uint64(firstKeySizeBytes)

		firstKeyBytes := make([]byte, firstKeySize)
		_, readErr = f.Read(firstKeyBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		firstKey := string(firstKeyBytes)

		lastKeySizeBytes := make([]byte, 8)
		_, readErr = f.Read(lastKeySizeBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		lastKeySize := binary.BigEndian.Uint64(lastKeySizeBytes)

		lastKeyBytes := make([]byte, lastKeySize)
		_, readErr = f.Read(lastKeyBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		lastKey := string(lastKeyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if key >= firstKey && key <= lastKey {
			return lastKey, offset, nil
		}

		initialOffset += 24 + int64(firstKeySize) + int64(lastKeySize)
	}
}

func loadAndFindValueOffset(fileNumber int, summaryOffset uint64, key string, lastKey string) (int64, error) {
	f, err := os.Open(config.INDEX_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return -1, err
	}
	defer f.Close()

	var lastReadOffset int64

	for {
		_, seekErr := f.Seek(int64(summaryOffset), io.SeekStart)
		if seekErr != nil {
			return -1, seekErr
		}

		keySizeBytes := make([]byte, 8)
		_, readErr := f.Read(keySizeBytes)
		if readErr == io.EOF {
			return -1, readErr
		} else if readErr != nil {
			return -1, readErr
		}
		keySize := binary.BigEndian.Uint64(keySizeBytes)

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			return -1, readErr
		}
		foundKey := string(keyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			return -1, readErr
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if key >= foundKey {
			lastReadOffset = offset
		} else {
			return lastReadOffset, nil
		}

		if foundKey == lastKey {
			break
		}

		summaryOffset += 16 + keySize
	}

	return lastReadOffset, nil
}

func loadRecord(fileNumber int, key string, valueOffset uint64) (*record.Record, error) {
	f, err := os.Open(config.DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	for {
		_, seekErr := f.Seek(int64(valueOffset), io.SeekStart)
		if seekErr != nil {
			return nil, seekErr
		}

		headerBytes := make([]byte, 29)
		_, readErr := f.Read(headerBytes)
		if readErr == io.EOF {
			return nil, readErr
		} else if readErr != nil {
			return nil, readErr
		}

		crc32 := binary.BigEndian.Uint32(headerBytes[0:4])
		timestamp := int64(binary.BigEndian.Uint64(headerBytes[4:12]))
		tombstone := headerBytes[12] != 0
		keySize := int64(binary.BigEndian.Uint64(headerBytes[13:21]))
		valueSize := int64(binary.BigEndian.Uint64(headerBytes[21:29]))

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			return nil, readErr
		}
		loadedKey := string(keyBytes)

		value := make([]byte, valueSize)
		_, readErr = f.Read(value)
		if readErr != nil {
			return nil, readErr
		}

		checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, loadedKey, value)
		if checkCrc32 != crc32 {
			valueOffset += 29 + uint64(keySize) + uint64(valueSize)
			continue
		}

		if loadedKey == key {
			return record.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, loadedKey, value), nil
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

	s.filter.WriteToBinFile(config.FILTER_FILE_PATH + strconv.Itoa(s.config.NumberOfSSTables) + ".bin")
}

func (s *SSTable) createMetaData(allRecords []record.Record) {
	var allRecordsBytes [][]byte
	for _, record := range allRecords {
		allRecordsBytes = append(allRecordsBytes, record.ToBytes())
	}
	s.metadata = merkle.NewMerkleTree(allRecordsBytes)
	s.metadata.WriteToBinFile(config.METADATA_FILE_PATH + strconv.Itoa(s.config.NumberOfSSTables) + ".bin")
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
