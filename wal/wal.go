package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
)

/* Ova struktura nam sluzi pri ucitavanju WAL-a */
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

/* Inicijalno kreiranje novog WAL-a */
func (w *Wal) Wal() {
	w.walOptions = new(WalOptions)
	w.walOptions.WalOptions(1, 0)

	w.segments = make([]*Segment, 1) // inijalno postoji jedan segment
	w.segments[0] = new(Segment)
	w.segments[0].NewSegment(getPath(w.walOptions.NumberOfSegments))
}

/* Ucitavanje svih zapisa odjednom */
func (w *Wal) LoadAllRecords() {
	w.LoadJson()
	w.LoadSegments()
}

/* Ucitavanje jednog po jednog zapisa */
func (w *Wal) LoadNextRecord() {
	w.LoadJson()
	if len(w.segments[len(w.segments)-1].records) == 64 {
		newSegment := new(Segment)
		newSegment.NewSegment(getPath(len(w.segments) + 1))
		w.segments = append(w.segments, newSegment)
	}
	fileName := getPath(len(w.segments))

	w.GetNextRecord(fileName)
}

/* Dodaje zapis u segment, ako je segment pun pravi novi segment */
func (w *Wal) AddRecord(key string, value string) {
	walRecord := new(Record)
	walRecord.NewRecord(key, value)
	lastSegment := w.segments[w.walOptions.NumberOfSegments-1]

	if len(lastSegment.records) == 64 {
		w.walOptions.NumberOfSegments++
		walSegment := new(Segment)
		walSegment.NewSegment(getPath(w.walOptions.NumberOfSegments))
		w.segments = append(w.segments, walSegment)
		lastSegment = walSegment
		w.WriteJson()
	}
	lastSegment.AddRecordToSegment(*walRecord)
}

/* Ova funkcija ucitava sledeci zapis na osnovu poslednje procitanog */
func (w *Wal) GetNextRecord(fileName string) {
	f, _ := os.OpenFile(fileName, os.O_RDONLY, 0644)
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	offset := w.CalculateOffset() // vrednost za koju pomeramo pokazivac u fajlu, pamtimo dokle je ucitano
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

	checkCrc32 := CalculateCRC(timestamp, tombstone, keySize, valueSize, key, value)

	if checkCrc32 == crc32 { // proveravanje da li je doslo do promene zapisa
		loadedRecord := new(Record)
		loadedRecord.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, key, value)
		lastSegment := w.segments[len(w.segments)-1]
		lastSegment.records = append(lastSegment.records, loadedRecord)
	}
}

/* Racuna koliko je zapisa procitano i za koliko se pomeramo pri citanju */
func (w *Wal) CalculateOffset() int {
	lastSegment := w.segments[len(w.segments)-1]
	offset := 0

	for i := 0; i < len(lastSegment.records); i++ {
		offset += len(lastSegment.records[i].ToBytes())
	}

	return offset
}

/* Ucitava sve segmente u memoriju */
func (w *Wal) LoadSegments() {
	for i := 1; i <= w.walOptions.NumberOfSegments; i++ {
		loadedSegment := new(Segment)
		loadedSegment.LoadSegment(getPath(i))
		w.segments = append(w.segments, loadedSegment)
	}
}

func (w *Wal) ChangeLowWaterMark(newLowWaterMark int) {
	w.walOptions.LowWaterMark = newLowWaterMark
	w.WriteJson()
}

/* Brise segmente na osnovu lowWaterMark iz WalOptions */
func (w *Wal) DeleteSegments() {
	for i := 1; i <= w.walOptions.LowWaterMark; i++ {
		w.walOptions.NumberOfSegments--
		w.segments = w.segments[1:]
		os.Remove(getPath(i)) // brise fajl
	}
	for i := 1; i <= w.walOptions.NumberOfSegments; i++ {
		w.segments[i-1].fileName = getPath(i)
		os.Rename(getPath(w.walOptions.LowWaterMark+i), getPath(i)) // preimenuje fajl
	}
	w.WriteJson()
}

/* Na osnovu rednog broja segmenta kreira filePath za segment */
func getPath(numberOfSegment int) string {
	path := SEGMENT_FILE_PATH

	stringNumberOfSegment := strconv.Itoa(numberOfSegment)
	lenString := len(stringNumberOfSegment)

	switch lenString {
	case 1:
		path += "00" + stringNumberOfSegment + ".log"
	case 2:
		path += "0" + stringNumberOfSegment + ".log"
	case 3:
		path += stringNumberOfSegment + ".log"
	default:
		err := errors.New("number of segments exceededs") // mozemo imati do 1000 segmenata
		fmt.Println("Error: ", err)
	}

	return path
}

/* Ucitava WalOptions iz config JSON fajla */
func (w *Wal) LoadJson() {
	jsonData, _ := os.ReadFile(WAL_CONFIG_FILE_PATH)

	json.Unmarshal(jsonData, &w.walOptions)
}

/* Upisuje WalOptions u config JSON fajl */
func (w *Wal) WriteJson() {
	jsonData, _ := json.MarshalIndent(w.walOptions, "", "  ")

	os.WriteFile(WAL_CONFIG_FILE_PATH, jsonData, 0644)
}
