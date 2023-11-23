package wal

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
	s.numberOfRecords += 1
	s.records = append(s.records, &record)

	// record_bytes := record.ToBytes()
	// pozivano mmap za record_bytes
}

// mozda ce nam trebati neka funkcija koja dodaje zapis na kraj segmenta
// mozda ce nam trebati neka funkcija koja ucitava ceo segment iz fajla, i to nekako treba da povezemo sa ovim drugim konstruktorom
