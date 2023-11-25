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

	w.segments = make([]*Segment, 1) // inijalno postoji jedan segment
	w.segments[0] = new(Segment)
	w.segments[0].NewSegment(getPath(w.walOptions.NumberOfSegments))

	w.walDirectory = walDirectory
}

func (w *Wal) LoadWal(walDirectory string) {
	w.LoadJson()
	w.LoadSegments()
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

func (w *Wal) LoadSegments() {
	for i := 1; i <= w.walOptions.NumberOfSegments; i++ {
		loadedSegment := new(Segment)
		loadedSegment.LoadSegment(getPath(i))
		w.segments = append(w.segments, loadedSegment)
	}
}

// func (w *Wal) WriteJson() {

// }

func (w *Wal) AddRecord(key string, value string) {
	wal_record := new(Record)
	wal_record.NewRecord(key, value)
	last_segment := w.segments[w.walOptions.NumberOfSegments-1]

	if len(last_segment.records) == 64 {
		w.walOptions.NumberOfSegments++
		wal_segment := new(Segment)
		wal_segment.NewSegment(getPath(w.walOptions.NumberOfSegments))
		w.segments = append(w.segments, wal_segment)
		last_segment = wal_segment
	}
	last_segment.AddRecordToSegment(*wal_record)
}

// nas wal moze imati do 1000 segmenata
func getPath(numberOfSegments int) string {
	path := SEGMENT_FILE_PATH

	stringNumberOfSegments := strconv.Itoa(numberOfSegments)
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

// čisti starije segmente prema low water mark
// func (wm *WALManager) CleanSegments() {
// }
