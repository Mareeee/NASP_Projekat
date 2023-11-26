package wal

import (
	"encoding/binary"
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
	segments   []*Segment
	walOptions *WalOptions
}

func (w *Wal) Wal() {
	w.walOptions = new(WalOptions)
	w.walOptions.WalOptions(1, 0)

	w.segments = make([]*Segment, 1) // inijalno postoji jedan segment
	w.segments[0] = new(Segment)
	w.segments[0].NewSegment(getPath(w.walOptions.NumberOfSegments))
}

func (w *Wal) LoadAllRecords() {
	w.LoadJson()
	w.LoadSegments()
}

func (w *Wal) LoadNextRecord() {
	w.LoadJson()
	if len(w.segments[len(w.segments)-1].records) == 3 {
		newSegment := new(Segment)
		newSegment.NewSegment(getPath(len(w.segments) + 1))
		w.segments = append(w.segments, newSegment)
	}
	fileName := getPath(len(w.segments))

	w.GetNextRecord(fileName)
}

// func (w *Wal) PrintRecord() string {
// 	return w.segments[len(w.segments)-1].records[len(w.segments[len(w.segments)-1].records)-1].key
// }

func (w *Wal) GetNextRecord(fileName string) {
	f, _ := os.OpenFile(fileName, os.O_RDONLY, 0644)
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	offset := w.CalculateOffset()
	data = data[offset:]
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

	loadedRecord := new(Record)
	loadedRecord.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, key, value)
	lastSegment := w.segments[len(w.segments)-1]
	lastSegment.records = append(lastSegment.records, loadedRecord)
}

func (w *Wal) CalculateOffset() int {
	lastSegment := w.segments[len(w.segments)-1]
	offset := 0

	for i := 0; i < len(lastSegment.records); i++ {
		offset += len(lastSegment.records[i].ToBytes())
	}

	return offset
}

func (w *Wal) LoadJson() {
	jsonData, _ := os.ReadFile(WAL_CONFIG_FILE_PATH)

	json.Unmarshal(jsonData, &w.walOptions)
}

func (w *Wal) LoadSegments() {
	for i := 1; i <= w.walOptions.NumberOfSegments; i++ {
		loadedSegment := new(Segment)
		loadedSegment.LoadSegment(getPath(i))
		w.segments = append(w.segments, loadedSegment)
	}
}

func (w *Wal) WriteJson() {
	jsonData, _ := json.MarshalIndent(w.walOptions, "", "  ")

	os.WriteFile(WAL_CONFIG_FILE_PATH, jsonData, 0644)
}

func (w *Wal) ChangeLowWaterMark(newLowWaterMark int) {
	w.walOptions.LowWaterMark = newLowWaterMark
	w.WriteJson()
}

// func (w *Wal) DeleteSegments() {

// }

func (w *Wal) AddRecord(key string, value string) {
	wal_record := new(Record)
	wal_record.NewRecord(key, value)
	last_segment := w.segments[w.walOptions.NumberOfSegments-1]

	if len(last_segment.records) == 3 {
		w.walOptions.NumberOfSegments++
		wal_segment := new(Segment)
		wal_segment.NewSegment(getPath(w.walOptions.NumberOfSegments))
		w.segments = append(w.segments, wal_segment)
		last_segment = wal_segment
		w.WriteJson()
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
