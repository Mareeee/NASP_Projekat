package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
)

type WalOptions struct {
	NumberOfSegments int `json:"NumberOfSegments"`
	LowWaterMark     int `json:"LowWaterMark"`
}

func (wo *WalOptions) WalOptions(NumberOfSegments int, LowWaterMark int) {
	wo.NumberOfSegments = NumberOfSegments
	wo.LowWaterMark = LowWaterMark
}

type Wal struct {
	segments     []*Segment
	walDirectory string
	walOptions   *WalOptions
}

func (w *Wal) Wal(walDirectory string) {
	w.walOptions = new(WalOptions)
	w.walOptions.WalOptions(1, 0)

	w.segments = make([]*Segment, 1)
	w.segments[0] = new(Segment)
	w.segments[0].NewSegment(w.GetPath())

	w.walDirectory = walDirectory
}

func (w *Wal) LoadWal(walDirectory string) {
	w.LoadJson()
}

func (w *Wal) LoadJson() {
	jsonData, err := os.ReadFile(WAL_CONFIG_FILE_PATH)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	err = json.Unmarshal(jsonData, &w.walOptions)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
}

func (w *Wal) AddRecord(key string, value string) {
	wal_record := new(Record)
	wal_record.NewRecord(key, value)
	last_segment := w.segments[w.walOptions.NumberOfSegments-1]

	if len(last_segment.records) == 64 {
		w.walOptions.NumberOfSegments++
		wal_segment := new(Segment)
		wal_segment.NewSegment(w.GetPath())
		w.segments = append(w.segments, wal_segment)
		last_segment = wal_segment
	}
	last_segment.AddRecordToSegment(*wal_record)
}

// allows up to 1000 segments
func (w Wal) GetPath() string {
	path := SEGMENT_FILE_PATH

	stringNumberOfSegments := strconv.Itoa(w.walOptions.NumberOfSegments)
	lenString := len(stringNumberOfSegments)

	switch lenString {
	case 1:
		path += "00" + stringNumberOfSegments + ".log"
	case 2:
		path += "0" + stringNumberOfSegments + ".log"
	case 3:
		path += stringNumberOfSegments + ".log"
	default:
		err := errors.New("number of segments exceededs")
		fmt.Println("Error: ", err)
	}

	return path
}

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
