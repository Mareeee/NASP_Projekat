package wal

import (
	"encoding/binary"
	"os"
)

type Segment struct {
	records         []*Record
	numberOfRecords int
	fileName        string
}

/* Konstruktor za pravljenje novog segmenta */
func (s *Segment) NewSegment(fileName string) {
	s.records = make([]*Record, 0)
	s.numberOfRecords = 0
	s.fileName = fileName
}

/* Konstruktor za ucitavanje segmenta iz memorije */
func (s *Segment) LoadSegment(fileName string) {
	s.LoadRecords(fileName)
	s.fileName = fileName
	s.numberOfRecords = len(s.records)
}

/* Ucitava sve zapise segmenta u memoriju */
func (s *Segment) LoadRecords(fileName string) {
	f, _ := os.OpenFile(fileName, os.O_RDONLY, 0644)
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

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
		value := string(data[29+keySize : 29+keySize+valueSize])

		checkCrc32 := CalculateCRC(timestamp, tombstone, keySize, valueSize, key, value)

		if checkCrc32 == crc32 { // potrebno je pri ucitavanju proveriti da li je doslo do promene zapisa
			loadedRecord := new(Record)
			loadedRecord.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, key, value)
			s.records = append(s.records, loadedRecord)
		}

		data = data[29+keySize+valueSize:]
	}
}

func (s *Segment) AddRecordToSegment(record Record) {
	s.numberOfRecords++
	s.records = append(s.records, &record)

	s.WriteRecord(record)
}

func (s Segment) WriteRecord(record Record) {
	recordBytes := record.ToBytes()

	f, _ := os.OpenFile(s.fileName, os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	f.Write(recordBytes)
}
