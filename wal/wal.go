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

func LoadWal(config config.Config) (*Wal, error) {
	w := new(Wal)
	w.config = config
	return w, nil
}

/* Dodaje zapis u segment, ako je segment pun pravi novi segment */
func (w *Wal) AddRecord(key string, value []byte) error {
	record := record.NewRecord(key, value)
	recordBytes := record.ToBytes()

	remainingSpaceInLastSegment := w.config.SegmentSize - w.config.LastSegmentSize

	if remainingSpaceInLastSegment < len(recordBytes) {
		err := w.AddRecordToSegment(recordBytes[:remainingSpaceInLastSegment])
		if err != nil {
			return err
		}
		w.config.NumberOfSegments++
		w.config.LastSegmentSize = 0
		err = w.AddRecordToSegment(recordBytes[remainingSpaceInLastSegment:])
		if err != nil {
			return err
		}
	} else {
		err := w.AddRecordToSegment(recordBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Wal) AddRecordToSegment(recordBytes []byte) error {
	w.config.LastSegmentSize += len(recordBytes)
	w.WriteToLastSegment(recordBytes)
	err := w.config.WriteConfig()
	return err
}

func (w Wal) WriteToLastSegment(recordBytes []byte) error {
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
	var data []byte

	err := config.LoadConfig(&w.config)
	if err != nil {
		return nil, err
	}

	for i := 1; i <= w.config.NumberOfSegments; i++ {
		loadedData, err := w.LoadDataFromSegment(getPath(i))
		if err != nil {
			return nil, err
		}
		data = append(data, loadedData...) // ucitavam sve segmente u veliki niz bajtova, ovako radim da bih lakse resio prelamanje rekorda
	}

	for len(data) != 0 {
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

/* Ucitava sve zapise segmenta u memoriju */
func (w *Wal) LoadDataFromSegment(fileName string) ([]byte, error) {
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

	return data, nil
}

/* Brise segmente na osnovu lowWaterMark iz WalOptions */
func (w *Wal) DeleteSegments() error {
	for i := 1; i <= w.config.LowWaterMark; i++ {
		w.config.NumberOfSegments--
		os.Remove(getPath(i)) // brise fajl
	}

	for i := 1; i <= w.config.NumberOfSegments; i++ {
		os.Rename(getPath(w.config.LowWaterMark+i), getPath(i)) // preimenuje fajl
	}

	if w.config.NumberOfSegments == 0 { // ako su obrisani svi segmenti
		w.config.NumberOfSegments = 1 // uvek mora postojati jedan u koji se upisuje
		w.config.LastSegmentSize = 0  // prazan je
	}

	err := w.config.WriteConfig()
	return err
}

func (w *Wal) ChangeLowWaterMark(newLowWaterMark int) error {
	w.config.LowWaterMark = newLowWaterMark
	err := w.config.WriteConfig()
	return err
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
