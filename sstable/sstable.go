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
	"sort"
	"strconv"
	"strings"
)

type SSTable struct {
	filter        *bloom.BloomFilter
	metadata      *merkle.MerkleTree
	config        *config.Config
	keyDictionary *map[int]string
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

func LoadSSTable(sstLevel int, fileNumber int) (*SSTable, error) {
	cfg := new(config.Config)
	err := config.LoadConfig(cfg)
	if err != nil {
		return nil, err
	}

	allRecords, err := record.LoadRecordsFromFile(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(sstLevel) + "_sstable_data_" + strconv.Itoa(fileNumber) + ".db")

	if err != nil {
		return nil, err
	}

	var allRecordsBytes [][]byte
	for _, record := range allRecords {
		allRecordsBytes = append(allRecordsBytes, record.ToBytes())
	}
	mtNew := merkle.NewMerkleTree(allRecordsBytes)

	mtFile := merkle.ReadFromBinFile(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(sstLevel) + "_sstable_data_" + strconv.Itoa(fileNumber) + ".bin")
	mtFileNode := merkle.DeserializeMerkleTree(mtFile)

	check := merkle.CompareMerkleTrees(mtFileNode, mtNew.Root)
	if !check {
		return nil, errors.New("data has been altered")
	}

	bf := bloom.LoadBloomFilter(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(sstLevel) + "_sstable_data_" + strconv.Itoa(fileNumber) + ".bin")

	sst := new(SSTable)
	sst.filter = bf
	sst.metadata = mtNew

	return sst, nil
}

func NewSSTable(allRecords []record.Record, config *config.Config, level int, keyDictionary *map[int]string) (*SSTable, error) {
	sst := new(SSTable)
	config.NumberOfSSTables++
	sst.keyDictionary = keyDictionary
	sst.config = config

	sst.writeDataIndexSummary(allRecords, level)
	sst.createFilter(allRecords, level)
	sst.createMetaData(allRecords, level)

	err := config.WriteConfig()
	if err != nil {
		return nil, err
	}

	return sst, nil
}

func (s *SSTable) writeDataIndexSummary(allRecords []record.Record, level int) {
	count := 0
	offset := 0

	var index []IndexEntry

	for i, record := range allRecords {
		// ovde se pravi data, upisujem sve rekorde
		s.WriteRecord(&record, config.SSTABLE_DIRECTORY+"lvl_"+strconv.Itoa(level)+"_sstable_data_"+strconv.Itoa(s.config.NumberOfSSTables)+".db")

		if count%s.config.IndexInterval == 0 {
			index = append(index, IndexEntry{key: record.Key, offset: int64(offset)})
		}
		count++
		offset += len(record.ToBytesSSTable(s.keyDictionary))
		allRecords[i] = record
	}

	s.writeIndex(index, level)
	summary := s.buildSummary(index)
	s.writeSummaryToFile(summary, level)
}

func (s *SSTable) writeIndex(index []IndexEntry, level int) {
	f, err := os.OpenFile(config.SSTABLE_DIRECTORY+"lvl_"+strconv.Itoa(level)+"_sstable_index_"+strconv.Itoa(s.config.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
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

func (s *SSTable) writeSummaryToFile(summary []SummaryEntry, level int) {
	f, err := os.OpenFile(config.SSTABLE_DIRECTORY+"lvl_"+strconv.Itoa(level)+"_sstable_summary_"+strconv.Itoa(s.config.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
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

func Search(key string, keyDicitonary *map[int]string) (*record.Record, error) {
	cfg := new(config.Config)
	config.LoadConfig(cfg)

	level, fileNumber, err := findSSTableNumber(key, cfg.NumberOfSSTables) // broj tabele u kojoj je zapis
	if fileNumber == -1 && err == nil {
		return nil, errors.New("key not found in any of sstables")
	} else if err != nil {
		return nil, err
	}

	lastKey, offset, err := loadAndFindIndexOffset(fileNumber, level, key, keyDicitonary)
	if err != nil {
		return nil, err
	}

	valueOffset, err := loadAndFindValueOffset(fileNumber, level, uint64(offset), key, lastKey, keyDicitonary)
	if err != nil {
		return nil, err
	}

	record, err := loadRecord(fileNumber, level, key, uint64(valueOffset), keyDicitonary)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// trazimo SSTabelu gde gde je sa velikom verovatnocom nas zapis
func findSSTableNumber(key string, numOfSSTables int) (int, int, error) {
	var data [][]int
	files, _ := os.ReadDir(config.SSTABLE_DIRECTORY)

	for _, file := range files {
		if strings.Contains(file.Name(), "sstable_data") {
			sstable_tokens := strings.Split(file.Name(), "_")
			level, _ := strconv.Atoi(sstable_tokens[1])
			index, _ := strconv.Atoi(sstable_tokens[4])
			data = append(data, []int{level, index})

			sort.Slice(data, func(i, j int) bool {
				return data[i][1] < data[j][1]
			})
		}
	}

	for i := len(data) - 1; i >= 0; i-- {
		sst, err := LoadSSTable(data[i][0], data[i][1])

		if err != nil {
			return -1, -1, err
		}

		if sst.filter.CheckElement(key) {
			return data[i][0], data[i][1], nil
		}
	}
	return -1, -1, nil
}

func loadAndFindIndexOffset(fileNumber, level int, key string, keyDictionary *map[int]string) (string, int64, error) {
	f, err := os.Open(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level) + "_sstable_summary_" + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return "", -1, err
	}
	defer f.Close()

	cfg := new(config.Config)
	config.LoadConfig(cfg)

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

		var firstKey string
		if cfg.Compress {
			index := binary.BigEndian.Uint64(firstKeyBytes)
			firstKey = (*keyDictionary)[int(index)]
		} else {
			firstKey = string(firstKeyBytes)
		}

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

		var lastKey string
		if cfg.Compress {
			index := binary.BigEndian.Uint64(lastKeyBytes)
			firstKey = (*keyDictionary)[int(index)]
		} else {
			lastKey = string(lastKeyBytes)
		}

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

func loadAndFindValueOffset(fileNumber, level int, summaryOffset uint64, key string, lastKey string, keyDictionary *map[int]string) (int64, error) {
	f, err := os.Open(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level) + "_sstable_index_" + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return -1, err
	}
	defer f.Close()

	cfg := new(config.Config)
	config.LoadConfig(cfg)

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
		var foundKey string
		if cfg.Compress {
			index := binary.BigEndian.Uint64(keyBytes)
			foundKey = (*keyDictionary)[int(index)]
		} else {
			foundKey = string(keyBytes)
		}

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

func loadRecord(fileNumber, level int, key string, valueOffset uint64, keyDictionary *map[int]string) (*record.Record, error) {
	f, err := os.Open(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level) + "_sstable_data_" + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg := new(config.Config)
	config.LoadConfig(cfg)

	for {
		_, seekErr := f.Seek(int64(valueOffset), io.SeekStart)
		if seekErr != nil {
			return nil, seekErr
		}

		headerBytes := make([]byte, 21)
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
		var valueSize int64 = 0
		if !tombstone {
			extraBytes := make([]byte, 8)
			_, readErr := f.Read(extraBytes)
			if readErr == io.EOF {
				return nil, readErr
			} else if readErr != nil {
				return nil, readErr
			}
			valueSize = int64(binary.BigEndian.Uint64(headerBytes[0:8]))
		}

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			return nil, readErr
		}
		var loadedKey string
		if cfg.Compress {
			index := binary.BigEndian.Uint64(keyBytes)
			loadedKey = (*keyDictionary)[int(index)]
		} else {
			loadedKey = string(keyBytes)
		}

		var value []byte = nil
		if !tombstone {
			value = make([]byte, valueSize)
			_, readErr = f.Read(value)
			if readErr != nil {
				return nil, readErr
			}
		}

		checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, loadedKey, value)
		if checkCrc32 != crc32 {
			if !tombstone {
				valueOffset += 29 + uint64(keySize) + uint64(valueSize)
			} else {
				valueOffset += 21 + uint64(keySize)
			}
			continue
		}

		if loadedKey == key {
			return record.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, loadedKey, value), nil
		}

		if !tombstone {
			valueOffset += 29 + uint64(keySize) + uint64(valueSize)
		} else {
			valueOffset += 21 + uint64(keySize)
		}
	}
}

func (s *SSTable) createFilter(allRecords []record.Record, level int) {
	s.filter = bloom.NewBloomFilter(len(allRecords), 0.01)

	for _, record := range allRecords {
		s.filter.AddElement(record.Key)
	}

	s.filter.WriteToBinFile(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level) + "_sstable_filter_" + strconv.Itoa(s.config.NumberOfSSTables) + ".bin")
}

func (s *SSTable) createMetaData(allRecords []record.Record, level int) {
	var allRecordsBytes [][]byte
	for _, record := range allRecords {
		allRecordsBytes = append(allRecordsBytes, record.ToBytesSSTable(s.keyDictionary))
	}
	s.metadata = merkle.NewMerkleTree(allRecordsBytes)
	s.metadata.WriteToBinFile(config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level) + "_sstable_metadata_" + strconv.Itoa(s.config.NumberOfSSTables) + ".bin")
}

func (s *SSTable) putElementToMap(ogKey string) int {
	for key, value := range *s.keyDictionary {
		if value == ogKey {
			return key
		}
	}

	nextKey := 0
	for key := range *s.keyDictionary {
		if key >= nextKey {
			nextKey = key + 1
		}
	}
	(*s.keyDictionary)[nextKey] = ogKey

	return nextKey
}

func (s *SSTable) ToBytesSSTable(r *record.Record) []byte {
	var bufferSize int64

	index := s.putElementToMap(r.Key)
	indexBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(indexBytes, uint16(index))
	r.KeySize = int64(len(indexBytes))

	if r.Tombstone {
		bufferSize = 15 + r.KeySize
		r.Crc32 = record.CalculateCRC(r.Timestamp, r.Tombstone, r.KeySize, 0, r.Key, nil)
	} else {
		bufferSize = 23 + r.KeySize + r.ValueSize
		if s.config.Compress {
			r.Crc32 = record.CalculateCRC(r.Timestamp, r.Tombstone, r.KeySize, r.ValueSize, r.Key, r.Value)
		}
	}
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(r.Crc32))
	binary.BigEndian.PutUint64(buffer[4:12], uint64(r.Timestamp))

	buffer[12] = 0
	if r.Tombstone {
		buffer[12] = 1
		binary.BigEndian.PutUint16(buffer[13:15], uint16(r.KeySize))
		copy(buffer[15:15+r.KeySize], []byte(r.Key))
		return buffer
	}
	binary.BigEndian.PutUint16(buffer[13:15], uint16(r.KeySize))
	binary.BigEndian.PutUint64(buffer[15:23], uint64(r.ValueSize))
	binary.BigEndian.PutUint16(buffer[23:23+r.KeySize], uint16(index))
	copy(buffer[23+r.KeySize:bufferSize], r.Value)
	return buffer
}

func (s *SSTable) WriteRecord(record *record.Record, filepath string) {
	var recordBytes []byte
	if s.config.Compress {
		recordBytes = s.ToBytesSSTable(record)
	} else {
		recordBytes = record.ToBytes()
	}

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

func WriteDataIndexSummaryLSM(path string, level int, cfg config.Config, keyDictionary *map[int]string) {
	dataFile, err := os.Open(path)
	if err != nil {
		return
	}
	defer dataFile.Close()

	sstableIndex := strings.Split(strings.Split(path, "_")[4], ".")[0]
	prefix := config.SSTABLE_DIRECTORY + "lvl_" + strconv.Itoa(level)

	indexFile, err := os.OpenFile(prefix+"_sstable_index_"+sstableIndex+".db", os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer indexFile.Close()

	summaryFile, err := os.OpenFile(prefix+"_sstable_summary_"+sstableIndex+".db", os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer summaryFile.Close()

	filterFile, err := os.OpenFile(prefix+"_sstable_filter_"+sstableIndex+".bin", os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer filterFile.Close()

	count := 0
	indexOffset := 0
	var index []IndexEntry
	var allRecordsBytes [][]byte

	for {
		record, err := record.LoadRecordFromFile(*dataFile, keyDictionary)
		if err != nil {
			break // procitali smo sve rekorde
		}

		if count%cfg.IndexInterval == 0 {
			index = append(index, IndexEntry{key: record.Key, offset: int64(indexOffset)})
			keyBytes := []byte(record.Key)

			binary.Write(indexFile, binary.BigEndian, int64(len(keyBytes)))
			indexFile.Write(keyBytes)
			binary.Write(indexFile, binary.BigEndian, int64(indexOffset))

		}
		count++
		indexOffset += len(record.ToBytesSSTable(keyDictionary))
		allRecordsBytes = append(allRecordsBytes, record.ToBytesSSTable(keyDictionary))
	}

	mt := merkle.NewMerkleTree(allRecordsBytes)
	mt.WriteToBinFile(prefix + "_sstable_metadata_" + sstableIndex + ".bin")

	summaryOffset := 0

	for i := 0; i < len(index); i += cfg.SummaryInterval {
		endIndex := i + cfg.SummaryInterval - 1

		if endIndex >= len(index) {
			endIndex = len(index) - 1
		}

		firstKeyBytes := []byte(index[i].key)
		lastKeyBytes := []byte(index[endIndex].key)

		binary.Write(summaryFile, binary.BigEndian, int64(len(firstKeyBytes)))
		summaryFile.Write(firstKeyBytes)

		binary.Write(summaryFile, binary.BigEndian, int64(len(lastKeyBytes)))
		summaryFile.Write(lastKeyBytes)

		binary.Write(summaryFile, binary.BigEndian, int64(summaryOffset))

		summaryOffset = 16 + len([]byte(index[i].key))
	}

	bf := bloom.NewBloomFilter(count, 0.01)

	dataFile, err = os.Open(path)
	if err != nil {
		return
	}
	defer dataFile.Close()

	for {
		record, err := record.LoadRecordFromFile(*dataFile, keyDictionary)
		if err != nil {
			break // procitali smo sve rekorde
		}
		bf.AddElement(record.Key)
	}

	filterFile.Write(bf.ToBytes())
}

// TODO: da se u lsm racuna broj rekorad i na osnovu toga pravi sstabela
