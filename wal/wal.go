package wal

import (
	"hash/crc32"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8
	BUFFER_SIZE     = 16
	SEGMENT_SIZE    = 64

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

// TODO: da li segmenti da budu strukture???
// type WALSegment struct {
// 	Index    int
// 	FilePath string
// 	Records  []*WALRecord
// }

type Wal struct {
	numberOfSegments uint
	lowWaterMark     uint
	walDirectorium   string
	buffer           [BUFFER_SIZE]WalRecord
}

type WalRecord struct {
	CRC32     uint32
	Timestamp int64
	Tombstone bool // 1 byte
	KeySize   int64
	ValueSize int64
	Key       []byte
	Value     []byte
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// TODO: Implementiraj dodavanje zapisa u WAL datoteku
// func (wm *WALManager) AddRecord(record *WALRecord) error {}

// TODO: Implementiraj čitanje svih zapisa iz WAL datoteke
// func (wm *WALManager) ReadAllRecords() ([]*WALRecord, error) {
// 	return nil, nil
// }

// TODO: Implementiraj čitanje jednog po jednog zapisa
// func (wm *WALManager) ReadNextRecord() (*WALRecord, error) {
// 	return nil, nil
// }

// skenira wal folder i zabeležava sve segmente
// func (wm *WALManager) scanSegments() {}

// izvlači indeks segmenta iz imena datoteke
// func extractSegmentIndex(filePath string) int {
// 	return index
// }

// čisti starije segmente prema low water mark
// func (wm *WALManager) CleanSegments() {
// }
