package wal

type Wal struct {
	segments         []*Segment
	numberOfSegments int
	walDirectory     string
	lowWaterMark     int
}

func (w *Wal) Wal(walDirectory string, lowWaterMark int) {
	w.segments = make([]*Segment, 0)
	w.numberOfSegments = 0
	w.walDirectory = walDirectory
	w.lowWaterMark = lowWaterMark
}

func (w *Wal) LoadWal(walDirectory string) {
	// Skeniranje Wal - a
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
