package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"main/config"
	"main/record"
	"os"
	"strconv"
)

type Wal struct {
	config config.Config
}

func LoadWal() *Wal {
	w := new(Wal)
	config.LoadConfig(&w.config)

	return w
}

/* Dodaje zapis u segment, ako je segment pun pravi novi segment */
func (w *Wal) AddRecord(key string, value []byte) {
	record := record.NewRecord(key, value)

	if w.config.LastSegmentNumberOfRecords == w.config.SegmentSize {
		w.config.NumberOfSegments++
		w.config.LastSegmentNumberOfRecords = 0
	}

	w.AddRecordToSegment(*record)
}

func (w *Wal) AddRecordToSegment(record record.Record) {
	w.config.LastSegmentNumberOfRecords++
	w.WriteRecord(record)
	w.config.WriteConfig()
}

func (w Wal) WriteRecord(record record.Record) error {
	recordBytes := record.ToBytes()

	f, err := os.OpenFile(getPath(w.config.NumberOfSegments), os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(recordBytes)
	return err
}

/* Ucitavanje svih zapisa odjednom */
func (w *Wal) LoadAllRecords() ([]*record.Record, error) {
	var records []*record.Record

	config.LoadConfig(&w.config)

	for i := 1; i <= w.config.NumberOfSegments; i++ {
		loadedRecords, err := w.LoadRecordsFromSegment(getPath(i))
		if err != nil {
			return nil, err
		}
		records = append(records, loadedRecords...)
	}

	return records, nil
}

/* Ucitava sve zapise segmenta u memoriju */
func (w *Wal) LoadRecordsFromSegment(fileName string) ([]*record.Record, error) {
	var records []*record.Record

	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	data := make([]byte, stat.Size())
	_, err = f.Read(data)
	if err != nil {
		return nil, err
	}

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

	return records, nil
}

/* Brise segmente na osnovu lowWaterMark iz WalOptions */
func (w *Wal) DeleteSegments() {
	for i := 1; i <= w.config.LowWaterMark; i++ {
		w.config.NumberOfSegments--
		os.Remove(getPath(i)) // brise fajl
	}

	for i := 1; i <= w.config.NumberOfSegments; i++ {
		os.Rename(getPath(w.config.LowWaterMark+i), getPath(i)) // preimenuje fajl
	}

	if w.config.NumberOfSegments == 0 { // ako su obrisani svi segmenti
		w.config.NumberOfSegments = 1           // uvek mora postojati jedan u koji se upisuje
		w.config.LastSegmentNumberOfRecords = 0 // prazan je
	}

	w.config.WriteConfig()
}

func (w *Wal) SetLowWaterMark(newLowWaterMark int) {
	w.config.LowWaterMark = newLowWaterMark
	w.config.WriteConfig()
}

/* Na osnovu rednog broja segmenta kreira filePath za segment */
func getPath(numberOfSegment int) string {
	path := config.SEGMENT_FILE_PATH

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
