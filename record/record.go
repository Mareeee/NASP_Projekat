package record

import (
	"encoding/binary"
	"hash/crc32"
	"main/config"
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
func NewRecord(key string, value []byte, delete bool) *Record {
	record := &Record{
		Tombstone: delete,
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

func LoadRecordFromFile(file os.File, keyDictionary *map[int]string) (Record, error) {
	var record Record
	cfg := new(config.Config)
	config.LoadConfig(cfg)

	CRCBytes := make([]byte, 4)
	_, err := file.Read(CRCBytes)
	if err != nil {
		return Record{}, err
	}
	record.Crc32 = binary.BigEndian.Uint32(CRCBytes)

	timestampBytes := make([]byte, 8)
	file.Read(timestampBytes)
	record.Timestamp = int64(binary.BigEndian.Uint64(timestampBytes))

	tombstoneBytes := make([]byte, 1)
	file.Read(tombstoneBytes)
	record.Tombstone = tombstoneBytes[0] == 1

	var keySizeBytes []byte
	if cfg.Compress {
		keySizeBytes = make([]byte, 2)
		file.Read(keySizeBytes)
		record.KeySize = int64(binary.BigEndian.Uint16(keySizeBytes))
	} else {
		keySizeBytes = make([]byte, 8)
		file.Read(keySizeBytes)
		record.KeySize = int64(binary.BigEndian.Uint64(keySizeBytes))
	}

	if !record.Tombstone {
		valueSizeBytes := make([]byte, 8)
		file.Read(valueSizeBytes)
		record.ValueSize = int64(binary.BigEndian.Uint64(valueSizeBytes))

		keyBytes := make([]byte, record.KeySize)
		file.Read(keyBytes)
		if cfg.Compress {
			index := binary.BigEndian.Uint16(keyBytes)
			record.Key = (*keyDictionary)[int(index)]
		} else {
			record.Key = string(keyBytes)
		}

		valueBytes := make([]byte, record.ValueSize)
		file.Read(valueBytes)
		record.Value = valueBytes
	} else {
		record.ValueSize = 0

		keyBytes := make([]byte, record.KeySize)
		file.Read(keyBytes)
		record.Key = string(keyBytes)

		record.Value = nil
	}
	checkCrc32 := CalculateCRC(record.Timestamp, record.Tombstone, record.KeySize, record.ValueSize, record.Key, record.Value)
	if checkCrc32 != record.Crc32 {
		return Record{}, nil
	}

	return record, nil
}

func LoadAllRecordsFromFiles(filePaths []*os.File, keyDictionary *map[int]string) []Record {
	var allRecords []Record

	for i := 0; i < len(filePaths); i++ {
		record, _ := LoadRecordFromFile(*filePaths[i], keyDictionary)
		allRecords = append(allRecords, record)
	}

	return allRecords
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

func putElementToMap(ogKey string, keyDictionary *map[int]string) int {
	for key, value := range *keyDictionary {
		if value == ogKey {
			return key
		}
	}

	nextKey := 0
	for key := range *keyDictionary {
		if key >= nextKey {
			nextKey = key + 1
		}
	}
	(*keyDictionary)[nextKey] = ogKey

	return nextKey
}

func (r Record) ToBytesSSTable(keyDictionary *map[int]string) []byte {
	cfg := new(config.Config)
	config.LoadConfig(cfg)
	var bufferSize int64
	if r.Tombstone {
		if cfg.Compress {
			bufferSize = 15 + r.KeySize
		} else {
			bufferSize = 21 + r.KeySize
		}
		r.Crc32 = CalculateCRC(r.Timestamp, r.Tombstone, r.KeySize, 0, r.Key, nil)
	} else {
		if cfg.Compress {
			bufferSize = 23 + r.KeySize + r.ValueSize
		} else {
			bufferSize = 29 + r.KeySize + r.ValueSize
		}
	}
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(r.Crc32))
	binary.BigEndian.PutUint64(buffer[4:12], uint64(r.Timestamp))
	buffer[12] = 0
	if r.Tombstone {
		buffer[12] = 1
		if cfg.Compress {
			index := putElementToMap(r.Key, keyDictionary)
			indexBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(indexBytes, uint16(index))
			binary.BigEndian.PutUint16(buffer[13:15], uint16(r.KeySize))
			binary.BigEndian.PutUint64(buffer[15:23], uint64(r.ValueSize))
			binary.BigEndian.PutUint16(buffer[23:23+r.KeySize], uint16(index))
			copy(buffer[23+r.KeySize:bufferSize], r.Value)
		} else {
			binary.BigEndian.PutUint64(buffer[13:21], uint64(r.KeySize))
			copy(buffer[21:21+r.KeySize], []byte(r.Key))
		}
		return buffer
	}
	if cfg.Compress {
		index := putElementToMap(r.Key, keyDictionary)
		indexBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(indexBytes, uint16(index))
		binary.BigEndian.PutUint16(buffer[13:15], uint16(r.KeySize))
		binary.BigEndian.PutUint64(buffer[15:23], uint64(r.ValueSize))
		binary.BigEndian.PutUint16(buffer[23:23+r.KeySize], uint16(index))
		copy(buffer[23+r.KeySize:bufferSize], r.Value)
	} else {
		binary.BigEndian.PutUint64(buffer[13:21], uint64(r.KeySize))
		binary.BigEndian.PutUint64(buffer[21:29], uint64(r.ValueSize))
		copy(buffer[29:29+r.KeySize], []byte(r.Key))
		copy(buffer[29+r.KeySize:bufferSize], r.Value)
	}
	return buffer
}

func GetNewerRecord(record1, record2 Record) Record {
	if record1.Timestamp > record2.Timestamp {
		return record1
	} else {
		return record2
	}
}

func IsSimilar(rec Record, target Record) bool {
	return rec.Crc32 == target.Crc32 && rec.Timestamp == target.Timestamp && rec.Tombstone == target.Tombstone && rec.KeySize == target.KeySize && rec.ValueSize == target.ValueSize && rec.Key == target.Key && string(rec.Value) == string(target.Value)
}
