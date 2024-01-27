package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

type Record struct {
	Crc32     uint32
	Timestamp int64
	Tombstone bool // 1 byte
	KeySize   int64
	ValueSize int64
	Key       string
	Value     []byte // sa konzole ucitavamo vrednost kao string, pa posle konvertujemo u niz bajtova
}

/* Konstruktor za pravljenje novog zapisa */
func NewRecord(key string, value []byte) *Record {
	record := &Record{
		Tombstone: false,
		Timestamp: time.Now().Unix(),
		KeySize:   int64(len([]byte(key))),
		ValueSize: int64(len([]byte(value))),
		Key:       key,
		Value:     value,
	}
	record.Crc32 = CalculateCRC(record.Timestamp, record.Tombstone, record.KeySize, record.ValueSize, record.Key, record.Value)
	fmt.Println(record.Crc32)
	fmt.Println(record.Timestamp)
	fmt.Println(record.Tombstone)
	fmt.Println(record.KeySize)
	fmt.Println(record.ValueSize)
	fmt.Println(record.Key)
	fmt.Println(record.Value)
	return record
}

/* Konstruktor za ucitavanje zapisa u memoriju */
func LoadRecord(crc32 uint32, timestamp int64, tombstone bool, keySize int64, valueSize int64, key string, value []byte) *Record {
	return &Record{
		Crc32:     crc32,
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
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
	bufferSize := 29 + r.KeySize + r.ValueSize
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(r.Crc32))
	binary.BigEndian.PutUint64(buffer[4:12], uint64(r.Timestamp))
	buffer[12] = 0
	if r.Tombstone {
		buffer[12] = 1
	}
	binary.BigEndian.PutUint64(buffer[13:21], uint64(r.KeySize))
	binary.BigEndian.PutUint64(buffer[21:29], uint64(r.ValueSize))
	copy(buffer[29:29+r.KeySize], []byte(r.Key))
	copy(buffer[29+r.KeySize:bufferSize], r.Value)
	return buffer
}
