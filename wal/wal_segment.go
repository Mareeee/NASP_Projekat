package wal

type Segment struct {
	records         []*Record
	numberOfRecords int
	fileName        string
	fileIndex       int
}

func (s *Segment) NewSegment(fileName string, fileIndex int) {
	s.records = make([]*Record, 0)
	s.numberOfRecords = 0
	s.fileName = fileName
	s.fileIndex = fileIndex
}

func (s *Segment) LoadSegment(records []*Record, numberOfRecords int, fileName string, fileIndex int) {
	s.records = records
	s.numberOfRecords = numberOfRecords
	s.fileName = fileName
	s.fileIndex = fileIndex
}

func (s *Segment) AddRecordToSegment(record Record) {
	s.records = append(s.records, &record)
}

// mozda ce nam trebati neka funkcija koja dodaje zapis na kraj segmenta
// mozda ce nam trebati neka funkcija koja ucitava ceo segment iz fajla, i to nekako treba da povezemo sa ovim drugim konstruktorom
