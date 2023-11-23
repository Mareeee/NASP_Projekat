package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

type WalOptions struct {
	numberOfSegments int
	lowWaterMark     int
}

func (wo *WalOptions) WalOptions(numberOfSegments int, lowWaterMark int) {
	wo.numberOfSegments = numberOfSegments
	wo.lowWaterMark = lowWaterMark
}

type Wal struct {
	segments     []*Segment
	walDirectory string
	walOptions   *WalOptions
}

func (w *Wal) Wal(walDirectory string) {
	w.segments = make([]*Segment, 0)
	w.walDirectory = walDirectory
	w.walOptions = new(WalOptions)
	w.walOptions.WalOptions(0, 0)
}

func (w Wal) LoadJson() {
	jsonFile, err := os.Open(WAL_CONFIG_FILE_PATH)
	if err != nil {
		fmt.Println(err)
	}

	byteValue, err := ioutil.ReadAll(jsonFile)

	fmt.Println("Loaded JSON Data:", string(byteValue))

	// Unmarshal the JSON data into the Wal struct
	err = json.Unmarshal(byteValue, w)

	defer jsonFile.Close()
}

func (w *Wal) LoadWal(walDirectory string) {
	w.LoadJson()
}

func (w *Wal) AddRecord(key string, value string) {
	wal_record := new(Record)
	wal_record.NewRecord(key, value)

	fmt.Println(w.walOptions.numberOfSegments)

	if len(w.segments[w.walOptions.numberOfSegments].records) == 64 {
		w.walOptions.numberOfSegments += 1
		wal_segment := new(Segment)
		wal_segment.NewSegment(w.GetPath())
	}

	w.segments[w.walOptions.numberOfSegments].AddRecordToSegment(*wal_record)
}

// allows up to 1000 segments
func (w Wal) GetPath() string {
	path := SEGMENT_FILE_PATH

	stringNumberOfSegments := strconv.Itoa(w.walOptions.numberOfSegments)
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
