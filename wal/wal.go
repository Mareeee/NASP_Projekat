package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"main/config"
	"main/record"
	"math"
	"os"
	"strconv"
)

type Wal struct {
	config       config.Config
	LowWaterMark int
}

func LoadWal(config config.Config) (*Wal, error) {
	w := new(Wal)
	w.config = config
	w.LowWaterMark = 0
	return w, nil
}

/* Dodaje zapis u segment, ako je segment pun pravi novi segment */
func (w *Wal) AddRecord(key string, value []byte, delete bool) error {
	record := record.NewRecord(key, value, delete)
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

func (w *Wal) IndependentLoadAllRecords() ([]record.Record, error) {
	i := 1
	var allRecords []record.Record
	var rec record.Record
	f, err := os.Open(getPath(i))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	for {
		CRCBytes := make([]byte, 4)
		n, _ := f.Read(CRCBytes)
		if n != 4 {
			partOfPrevious := CRCBytes[len(CRCBytes)-n:]
			f, err = os.Open(getPath(i + 1))
			if err != nil {
				break
			}
			defer f.Close()
			i++
			CRCBytesNext := make([]byte, 4-len(partOfPrevious))
			_, err = f.Read(CRCBytesNext)
			if err != nil {
				return nil, err
			}
			rec.Crc32 = binary.BigEndian.Uint32(append(partOfPrevious[:], CRCBytesNext[:]...))
		} else {
			rec.Crc32 = binary.BigEndian.Uint32(CRCBytes)
		}

		timestampBytes := make([]byte, 8)
		n, _ = f.Read(timestampBytes)
		if n != 8 {
			partOfPrevious := timestampBytes[len(timestampBytes)-n:]
			f, _ = os.Open(getPath(i + 1))
			defer f.Close()
			i++
			timestampBytesNext := make([]byte, 8-len(partOfPrevious))
			_, err := f.Read(timestampBytesNext)
			if err != nil {
				return nil, err
			}
			rec.Timestamp = int64(binary.BigEndian.Uint64(append(partOfPrevious[:], timestampBytesNext[:]...)))
		} else {
			rec.Timestamp = int64(binary.BigEndian.Uint32(timestampBytes))
		}

		tombstoneBytes := make([]byte, 1)
		n, _ = f.Read(tombstoneBytes)
		if n != 1 {
			partOfPrevious := tombstoneBytes[len(tombstoneBytes)-n:]
			f, _ = os.Open(getPath(i + 1))
			defer f.Close()
			i++
			tombstoneBytesNext := make([]byte, 1-len(partOfPrevious))
			_, err := f.Read(tombstoneBytesNext)
			if err != nil {
				return nil, err
			}
			rec.Tombstone = (append(partOfPrevious[:], tombstoneBytesNext[:]...))[0] == 1
		} else {
			rec.Tombstone = tombstoneBytes[0] == 1
		}

		keySizeBytes := make([]byte, 8)
		n, _ = f.Read(keySizeBytes)
		if n != 8 {
			partOfPrevious := keySizeBytes[len(keySizeBytes)-n:]
			f, _ = os.Open(getPath(i + 1))
			defer f.Close()
			i++
			keySizeBytesNext := make([]byte, 8-len(partOfPrevious))
			_, err := f.Read(keySizeBytesNext)
			if err != nil {
				return nil, err
			}
			rec.KeySize = int64(binary.BigEndian.Uint64(append(partOfPrevious[:], keySizeBytesNext[:]...)))
		} else {
			rec.KeySize = int64(binary.BigEndian.Uint64(keySizeBytes))
		}

		valueSizeBytes := make([]byte, 8)
		n, _ = f.Read(valueSizeBytes)
		if n != 8 {
			partOfPrevious := valueSizeBytes[len(valueSizeBytes)-n:]
			f, _ = os.Open(getPath(i + 1))
			defer f.Close()
			i++
			valueSizeBytesBytesNext := make([]byte, 8-len(partOfPrevious))
			_, err := f.Read(valueSizeBytesBytesNext)
			if err != nil {
				return nil, err
			}
			rec.ValueSize = int64(binary.BigEndian.Uint64(append(partOfPrevious[:], valueSizeBytesBytesNext[:]...)))
		} else {
			rec.ValueSize = int64(binary.BigEndian.Uint64(valueSizeBytes))
		}

		keyBytes := make([]byte, rec.KeySize)
		n, _ = f.Read(keyBytes)
		if n != int(rec.KeySize) {
			partOfPrevious := keyBytes[len(keyBytes)-n:]
			f, _ = os.Open(getPath(i + 1))
			defer f.Close()
			i++
			keyBytesNext := make([]byte, int(rec.KeySize)-len(partOfPrevious))
			_, err := f.Read(keyBytesNext)
			if err != nil {
				return nil, err
			}
			rec.Key = string(append(partOfPrevious[:], keyBytesNext[:]...))
		} else {
			rec.Key = string(keyBytes)
		}

		valueBytes := make([]byte, rec.ValueSize)
		n, _ = f.Read(valueBytes)
		if n != int(rec.ValueSize) {
			partOfPrevious := valueBytes[len(valueBytes)-n:]
			f, _ = os.Open(getPath(i + 1))
			defer f.Close()
			i++
			valueBytesNext := make([]byte, int(rec.ValueSize)-len(partOfPrevious))
			_, err := f.Read(valueBytesNext)
			if err != nil {
				return nil, err
			}
			rec.Value = append(partOfPrevious[:], valueBytesNext[:]...)
		} else {
			rec.Value = valueBytes
		}

		allRecords = append(allRecords, rec)
		rec = record.Record{}
	}

	return allRecords, nil
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
func (w *Wal) DeleteSegments(newLowWaterMark int) error {
	w.LowWaterMark = newLowWaterMark
	for i := 1; i <= w.LowWaterMark; i++ {
		w.config.NumberOfSegments--
		os.Remove(getPath(i)) // brise fajl
	}

	for i := 1; i <= w.config.NumberOfSegments; i++ {
		os.Rename(getPath(w.LowWaterMark+i), getPath(i)) // preimenuje fajl
	}

	if w.config.NumberOfSegments == 0 { // ako su obrisani svi segmenti
		w.config.NumberOfSegments = 1 // uvek mora postojati jedan u koji se upisuje
		w.config.LastSegmentSize = 0  // prazan je
	}

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

func (w *Wal) DeleteWalSegmentsEngine(SizeOfRecordsInWal int) {
	walsToDelete := int(math.Floor(float64(SizeOfRecordsInWal) / float64(w.config.SegmentSize)))
	fmt.Println(walsToDelete)
	remainingBytesToTruncate := SizeOfRecordsInWal - int(walsToDelete)*w.config.SegmentSize
	// slucaj ako brisemo ceo wal
	if walsToDelete+1 == w.config.NumberOfSegments && remainingBytesToTruncate == w.config.LastSegmentSize {
		walsToDelete += 1
		w.DeleteSegments(int(walsToDelete))
		return
	}
	w.DeleteSegments(int(walsToDelete))
	for i := 1; i <= w.config.NumberOfSegments-1; i++ {
		f, err := os.OpenFile(getPath(i), os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			continue
		}
		defer f.Close()

		// shiftovanje byteova na pocetak trenutnog filea
		remainingFileData := w.getSegmentSize(i) - remainingBytesToTruncate
		fmt.Println(remainingBytesToTruncate)

		f.Seek(int64(remainingBytesToTruncate), 0)

		fmt.Println(remainingFileData)
		data := make([]byte, remainingFileData)
		_, err = f.Read(data)
		if err != nil {
			fmt.Println("Error reading file:  1", err)
			continue
		}

		f.Seek(0, 0)
		f.Write(data)

		// uzimanje remainingBytesToTruncate iz sledeceg filea sa pocetka
		f2, err := os.OpenFile(getPath(i+1), os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			continue
		}
		defer f2.Close()

		var data2 []byte
		if i+1 == w.config.NumberOfSegments && remainingBytesToTruncate > w.config.LastSegmentSize {
			data2 = make([]byte, w.config.LastSegmentSize)
			_, err = f2.Read(data2)
			if err != nil {
				fmt.Println("Error reading file:  2", err)
				continue
			}

			f.Write(data2)
			bytesToTruncate := remainingBytesToTruncate - w.config.LastSegmentSize
			previousFilePosition := int64(w.config.SegmentSize - bytesToTruncate)
			os.Remove(getPath(w.config.NumberOfSegments))
			f2.Seek(previousFilePosition, 0)
			os.Truncate(getPath(w.config.NumberOfSegments-1), int64(bytesToTruncate))
			w.config.NumberOfSegments--
			w.config.LastSegmentSize = int(previousFilePosition)
			w.config.WriteConfig()
			return
		} else {
			data2 = make([]byte, remainingBytesToTruncate)
			_, err = f2.Read(data2)
			if err != nil {
				fmt.Println("Error reading file:  3", err)
				continue
			}

			f.Write(data2)
			if i+1 == w.config.NumberOfSegments {
				f2.Seek(int64(remainingBytesToTruncate), 0)
				data3 := make([]byte, w.config.LastSegmentSize-remainingBytesToTruncate)
				_, err = f2.Read(data3)
				if err != nil {
					fmt.Println("Error reading file:  4", err)
					continue
				}

				f2.Seek(0, 0)
				f2.Write(data3)
				os.Truncate(getPath(w.config.NumberOfSegments), int64(remainingBytesToTruncate))
				w.config.LastSegmentSize -= remainingBytesToTruncate
				w.config.WriteConfig()
				return
			}
		}
	}
}

func (w *Wal) getSegmentSize(segmentIndex int) int {
	if segmentIndex == w.config.NumberOfSegments {
		return w.config.LastSegmentSize
	}
	return w.config.SegmentSize
}
