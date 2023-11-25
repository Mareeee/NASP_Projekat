package wal

import (
	"log"
	"os"
)

type Segment struct {
	records         []*Record
	numberOfRecords int
	fileName        string
}

func (s *Segment) NewSegment(fileName string) {
	s.records = make([]*Record, 0)
	s.numberOfRecords = 0
	s.fileName = fileName
}

func (s *Segment) LoadSegment(records []*Record, numberOfRecords int, fileName string) {
	s.records = records
	s.numberOfRecords = numberOfRecords
	s.fileName = fileName
}

func (s *Segment) AddRecordToSegment(record Record) {
	s.numberOfRecords++
	s.records = append(s.records, &record)

	s.WriteRecord(record)
}

func (s Segment) WriteRecord(record Record) {
	recordBytes := record.ToBytes()

	f, err := os.OpenFile(s.fileName, os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	f.Write(recordBytes)
}
