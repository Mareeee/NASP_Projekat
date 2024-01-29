package record

import (
	"encoding/binary"
	"hash/crc32"
	"os"
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

func LoadRecordsFromFile(fileName string) ([]*Record, error) {
	var records []*Record

	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	data := make([]byte, stat.Size())
	_, err = f.Read(data)
	if err != nil {
		return nil, err
	}

	for len(data) != 0 { // ucitavaj iz fajla sve dok ima nesto
		crc32 := binary.BigEndian.Uint32(data[0:4])
		timestamp := int64(binary.BigEndian.Uint64(data[4:12]))
		tombstone := false
		if data[12] == 1 {
			tombstone = true
		}
		keySize := int64(binary.BigEndian.Uint64(data[13:21]))
		valueSize := int64(binary.BigEndian.Uint64(data[21:29]))
		key := string(data[29 : 29+keySize])
		value := data[29+keySize : 29+keySize+valueSize]

		checkCrc32 := CalculateCRC(timestamp, tombstone, keySize, valueSize, key, value)

		if checkCrc32 == crc32 { // potrebno je pri ucitavanju proveriti da li je doslo do promene zapisa
			loadedRecord := LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, key, value)
			records = append(records, loadedRecord)
		}

		data = data[29+keySize+valueSize:]
	}

	return records, nil
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

func GetNewerRecord(record1, record2 Record) Record {
	if record1.Timestamp > record2.Timestamp {
		return record1
	} else {
		return record2
	}
}
