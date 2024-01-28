package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"main/record"
	"os"
	"strconv"
)

type Wal struct {
	NumberOfSegments           int `json:"NumberOfSegments"`
	LowWaterMark               int `json:"LowWaterMark"`
	SegmentSize                int `json:"SegmentSize"`
	LastSegmentNumberOfRecords int `json:"LastSegmentNumberOfRecords"`
}

func LoadWal() *Wal {
	w := new(Wal)
	w.LoadJson()
	return w
}

/* Dodaje zapis u segment, ako je segment pun pravi novi segment */
func (w *Wal) AddRecord(key string, value []byte) {
	record := record.NewRecord(key, value)

	if w.LastSegmentNumberOfRecords == w.SegmentSize {
		w.NumberOfSegments++
		w.LastSegmentNumberOfRecords = 0
	}

	w.AddRecordToSegment(*record)
}

func (w *Wal) AddRecordToSegment(record record.Record) {
	w.LastSegmentNumberOfRecords++
	w.WriteRecord(record)
	w.WriteJson()
}

func (w Wal) WriteRecord(record record.Record) {
	recordBytes := record.ToBytes()

	f, _ := os.OpenFile(getPath(w.NumberOfSegments), os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	f.Write(recordBytes)
}

/* Ucitavanje svih zapisa odjednom */
func (w *Wal) LoadAllRecords() []*record.Record {
	var records []*record.Record

	for i := 1; i <= w.NumberOfSegments; i++ {
		records = append(records, w.LoadRecordsFromSegment(getPath(i))...)
	}

	return records
}

/* Ucitava sve zapise segmenta u memoriju */
func (w *Wal) LoadRecordsFromSegment(fileName string) []*record.Record {
	var records []*record.Record

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
		value := data[29+keySize : 29+keySize+valueSize]

		checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, key, value)

		if checkCrc32 == crc32 { // potrebno je pri ucitavanju proveriti da li je doslo do promene zapisa
			loadedRecord := record.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, key, value)
			records = append(records, loadedRecord)
		}

		data = data[29+keySize+valueSize:]
	}

	return records
}

/* Brise segmente na osnovu lowWaterMark iz WalOptions */
func (w *Wal) DeleteSegments() {
	for i := 1; i <= w.LowWaterMark; i++ {
		w.NumberOfSegments--
		os.Remove(getPath(i)) // brise fajl
	}

	for i := 1; i <= w.NumberOfSegments; i++ {
		os.Rename(getPath(w.LowWaterMark+i), getPath(i)) // preimenuje fajl
	}

	if w.NumberOfSegments == 0 { // ako su obrisani svi segmenti
		w.NumberOfSegments = 1           // uvek mora postojati jedan u koji se upisuje
		w.LastSegmentNumberOfRecords = 0 // prazan je
	}

	w.WriteJson()
}

func (w *Wal) SetLowWaterMark(newLowWaterMark int) {
	w.LowWaterMark = newLowWaterMark
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

	json.Unmarshal(jsonData, &w)
}

/* Upisuje WalOptions u config JSON fajl */
func (w *Wal) WriteJson() {
	jsonData, _ := json.MarshalIndent(w, "", "  ")

	os.WriteFile(WAL_CONFIG_FILE_PATH, jsonData, 0644)
}
