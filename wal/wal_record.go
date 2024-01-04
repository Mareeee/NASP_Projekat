package wal

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

type Record struct {
	crc32     uint32
	timestamp int64
	tombstone bool // 1 byte
	keySize   int64
	valueSize int64
	key       string
	value     []byte // sa konzole ucitavamo vrednost kao string, pa posle konvertujemo u niz bajtova
}

/* Konstruktor za pravljenje novog zapisa */
func NewRecord(key string, value []byte) *Record {
	record := &Record{
		tombstone: false,
		timestamp: time.Now().Unix(),
		keySize:   int64(len([]byte(key))),
		valueSize: int64(len([]byte(value))),
		key:       key,
		value:     value,
	}
	record.crc32 = CalculateCRC(record.timestamp, record.tombstone, record.keySize, record.valueSize, record.key, record.value)
	return record
}

/* Konstruktor za ucitavanje zapisa u memoriju */
func LoadRecord(crc32 uint32, timestamp int64, tombstone bool, keySize int64, valueSize int64, key string, value []byte) *Record {
	return &Record{
		crc32:     crc32,
		timestamp: timestamp,
		tombstone: tombstone,
		keySize:   keySize,
		valueSize: valueSize,
		key:       key,
		value:     value,
	}
}

func CalculateCRC(timestamp int64, tombstone bool, keySize int64, valueSize int64, key string, value []byte) uint32 {
	bufferSize := 25 + keySize + valueSize // 25 zato sto su svi pre key i value fiksni, a key i value su promenljive duzine, crc nije uracunat
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint64(buffer[0:8], uint64(timestamp))
	buffer[8] = 0
	if tombstone {
		buffer[8] = 1
	}
	binary.BigEndian.PutUint64(buffer[9:17], uint64(keySize))
	binary.BigEndian.PutUint64(buffer[17:25], uint64(valueSize))
	copy(buffer[25:25+keySize], []byte(key))
	copy(buffer[25+keySize:bufferSize], value)

	return crc32.ChecksumIEEE(buffer)
}

/* Konvertuje zapis u niz bajtova */
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
	copy(buffer[29+r.keySize:bufferSize], r.value)
	return buffer
}
