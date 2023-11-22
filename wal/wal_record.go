package wal

import (
	"encoding/binary"
	"hash/crc32"
	"time"
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

type Record struct {
	crc32     uint32
	timestamp int64
	tombstone bool // 1 byte
	keySize   int64
	valueSize int64
	key       string
	value     string // sa konzole ucitavamo vrednost kao string, pa posle konvertujemo u sta treba
	// string najlakse mozemo prebaciti u niz bajtova
}

func (r *Record) NewRecord(key string, value string) {
	r.tombstone = false
	r.timestamp = time.Now().Unix()
	r.keySize = int64(len([]byte(key)))
	r.valueSize = int64(len([]byte(value)))
	r.key = key
	r.value = value
	r.crc32 = r.calculateCRC(r.timestamp, r.tombstone, r.keySize, r.valueSize, r.key, r.value)
}

func (r *Record) LoadRecord(crc32 uint32, timestamp int64, tombstone bool, keySize int64, valueSize int64, key string, value string) {
	r.crc32 = crc32
	r.timestamp = timestamp
	r.tombstone = tombstone
	r.keySize = keySize
	r.valueSize = valueSize
	r.key = key
	r.value = value
}

func (r Record) calculateCRC(timestamp int64, tombstone bool, keySize int64, valueSize int64, key string, value string) uint32 {
	bufferSize := 25 + keySize + valueSize
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint64(buffer[0:8], uint64(r.timestamp))
	buffer[8] = 0
	if r.tombstone {
		buffer[8] = 1
	}
	binary.BigEndian.PutUint64(buffer[9:17], uint64(keySize))
	binary.BigEndian.PutUint64(buffer[17:25], uint64(valueSize))
	copy(buffer[25:25+keySize], []byte(r.key))
	copy(buffer[25+keySize:bufferSize], []byte(r.value))

	return crc32.ChecksumIEEE(buffer)
}

// pretvara zapis u niz bajtova, trebace nam zbog upisivanja u fajl
func (r Record) ToBytes() []byte {
	bufferSize := 29 + r.keySize + r.valueSize
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(r.crc32))
	binary.BigEndian.PutUint64(buffer[4:12], uint64(r.timestamp))
	buffer[12] = 0
	if r.tombstone {
		buffer[12] = 1
	}
	binary.BigEndian.PutUint64(buffer[13:21], uint64(r.keySize))
	binary.BigEndian.PutUint64(buffer[21:29], uint64(r.valueSize))
	copy(buffer[29:29+r.keySize], []byte(r.key))
	copy(buffer[29+r.keySize:bufferSize], []byte(r.value))
	return buffer
}
