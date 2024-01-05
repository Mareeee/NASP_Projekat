package sstable

import (
	"encoding/binary"
	"encoding/json"
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

/*
	INDEX ENTRY NA DISKU (ovo sam stavio da znas zbog citanja)
   +----------------------+----------+---------------+
   |    Key Length (8B)   |    Key   |   Offset (8B) |
   +----------------------+----------+---------------+
*/
type IndexEntry struct {
	key    string
	offset int64 // offset sa kog citamo iz data
}


/*
	SUMMARY ENTRY NA DISKU (ovo sam stavio da znas zbog citanja)
   +---------------------+---------+--------------------+--------+---------------------+--------------------+----------------+
   |First Key Length (8B)|First Key|Last Key Length (8B)|Last Key|First Key Offset (8B)|Last Key Offset (8B)|Num Entries (8B)|
   +---------------------+---------+--------------------+--------+---------------------+--------------------+----------------+
*/
type SummaryEntry struct {
	firstKey       string
	lastKey        string
	firstKeyOffset int64 //  offset s kog citamo iz indexa
	lastKeyOffset  int64 //  offset s kog citamo iz indexa
	numEntries     int64 //  ovo nam govori koliko index entrija obuhvata jedan summary entry zapis
}

func NewSSTable(allRecords []record.Record) {
	sst := new(SSTable)

	sst.options.NumberOfSSTables++
	sst.options.WriteJson()

	sst.writeDataIndexSummary(allRecords)
	sst.createFilter(allRecords)
	sst.createMetaData(allRecords)
}

func (s *SSTable) writeDataIndexSummary(allRecords []record.Record) {
	count := 0
	offset := 0

	var index []IndexEntry

	for _, record := range allRecords {
		// ovde se pravi data, upisujem sve rekorde
		s.WriteRecord(record, DATA_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables)+".db")

		count++
		offset += len(record.ToBytes())

		if count%s.options.IndexInterval == 0 {
			index = append(index, IndexEntry{key: record.Key, offset: int64(offset)})
		}
	}

	s.writeIndex(index)
	summary := s.buildSummary(index)
	s.writeSummaryToFile(summary)
}

func (s *SSTable) writeIndex(index []IndexEntry) {
	f, _ := os.OpenFile(INDEX_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables), os.O_CREATE|os.O_APPEND, 0644)
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

	firstKeyOffset := 0
	lastKeyOffset := 0

	for i := 0; i < len(index); i += s.options.SummaryInterval {
		endIndex := i + s.options.SummaryInterval - 1

		firstKeyOffset = 16 + len([]byte(index[i].key))
		lastKeyOffset = 16 + len([]byte(index[endIndex].key))

		if endIndex >= len(index) {
			endIndex = len(index) - 1
		}

		summaryEntry := SummaryEntry{
			firstKey:       index[i].key,
			lastKey:        index[endIndex].key,
			firstKeyOffset: int64(firstKeyOffset),
			lastKeyOffset:  int64(lastKeyOffset),
			numEntries:     int64(endIndex - i + 1),
		}

		summary = append(summary, summaryEntry)
	}

	return summary
}

func (s *SSTable) writeSummaryToFile(summary []SummaryEntry) {
	f, _ := os.OpenFile(INDEX_FILE_PATH+strconv.Itoa(s.options.NumberOfSSTables)+".db", os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	for _, entry := range summary {
		firstKeyBytes := []byte(entry.firstKey)
		lastKeyBytes := []byte(entry.lastKey)

		binary.Write(f, binary.BigEndian, int64(len(firstKeyBytes)))
		f.Write(firstKeyBytes)

		binary.Write(f, binary.BigEndian, int64(len(lastKeyBytes)))
		f.Write(lastKeyBytes)

		binary.Write(f, binary.BigEndian, entry.firstKeyOffset)
		binary.Write(f, binary.BigEndian, entry.lastKeyOffset)
		binary.Write(f, binary.BigEndian, entry.numEntries)
	}
}

func (s *SSTable) createFilter(allRecords []record.Record) {
	s.filter = new(bloom.BloomFilter)
	s.filter.NewBloomFilter(len(allRecords), 0.01)

	for _, record := range allRecords {
		s.filter.AddElement(record.Key)
	}

	s.filter.WriteToBinFile()
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
