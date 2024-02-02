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
	lastSegmentSize  int
	segmentSize      int
	numberOfSegments int
	lowWaterMark     int
}

func LoadWal(segmentSize int) (*Wal, error) {
	w := new(Wal)
	w.segmentSize = segmentSize
	w.numberOfSegments = countFilesInDirectory(config.WAL_DIRECTORY)
	w.lastSegmentSize = getFileSize(getPath(w.numberOfSegments))
	w.lowWaterMark = 0
	return w, nil
}

func countFilesInDirectory(dirPath string) int {
	files, _ := os.ReadDir(dirPath)

	if len(files) == 0 {
		return 1
	}

	return len(files)
}

func getFileSize(filePath string) int {
	fileInfo, _ := os.Stat(filePath)

	if fileInfo == nil {
		return 0
	}

	return int(fileInfo.Size())
}

/* Dodaje zapis u segment, ako je segment pun pravi novi segment */
func (w *Wal) AddRecord(key string, value []byte, delete bool) {
	record := record.NewRecord(key, value, delete)
	recordBytes := record.ToBytes()

	remainingSpaceInLastSegment := w.segmentSize - w.lastSegmentSize

	if remainingSpaceInLastSegment < len(recordBytes) {
		w.AddRecordToSegment(recordBytes[:remainingSpaceInLastSegment])
		w.numberOfSegments++
		w.lastSegmentSize = 0
		w.AddRecordToSegment(recordBytes[remainingSpaceInLastSegment:])
	} else {
		w.AddRecordToSegment(recordBytes)
	}
}

func (w *Wal) AddRecordToSegment(recordBytes []byte) {
	w.lastSegmentSize += len(recordBytes)
	w.WriteToLastSegment(recordBytes)
}

func (w Wal) WriteToLastSegment(recordBytes []byte) error {
	f, err := os.OpenFile(getPath(w.numberOfSegments), os.O_CREATE|os.O_APPEND, 0644)
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

	for i := 1; i <= w.numberOfSegments; i++ {
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

func (w *Wal) DeleteSegments(newLowWaterMark int) {
	w.lowWaterMark = newLowWaterMark
	for i := 1; i <= w.lowWaterMark; i++ {
		w.numberOfSegments--
		os.Remove(getPath(i)) // brise fajl
	}

	for i := 1; i <= w.numberOfSegments; i++ {
		os.Rename(getPath(w.lowWaterMark+i), getPath(i)) // preimenuje fajl
	}

	if w.numberOfSegments == 0 { // ako su obrisani svi segmenti
		w.numberOfSegments = 1 // uvek mora postojati jedan u koji se upisuje
		w.lastSegmentSize = 0  // prazan je
	}
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
	walsToDelete := int(math.Floor(float64(SizeOfRecordsInWal) / float64(w.segmentSize)))
	remainingBytesToTruncate := SizeOfRecordsInWal - walsToDelete*w.segmentSize
	// slucaj ako brisemo ceo wal
	if walsToDelete+1 == w.numberOfSegments && remainingBytesToTruncate == w.lastSegmentSize {
		walsToDelete += 1
		w.DeleteSegments(int(walsToDelete))
		return
	}
	w.DeleteSegments(int(walsToDelete))
	for i := 1; i <= w.numberOfSegments-1; i++ {
		f, err := os.OpenFile(getPath(i), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			continue
		}

		// shiftovanje byteova na pocetak trenutnog filea
		remainingFileData := w.getSegmentSize(i) - remainingBytesToTruncate
		f.Seek(int64(remainingBytesToTruncate), 0)

		data := make([]byte, remainingFileData)
		_, err = f.Read(data)
		if err != nil {
			fmt.Println("Error reading file: ", err)
			continue
		}
		f.Seek(0, 0)
		f.Write(data)

		// uzimanje remainingBytesToTruncate iz sledeceg filea sa pocetka
		f2, err := os.OpenFile(getPath(i+1), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			continue
		}

		var data2 []byte
		if i+1 == w.numberOfSegments &&
			remainingBytesToTruncate > w.lastSegmentSize {
			data2 = make([]byte, w.lastSegmentSize)
			_, err = f2.Read(data2)
			if err != nil {
				fmt.Println("Error reading file: ", err)
				continue
			}

			f.Write(data2)
			os.Remove(getPath(w.numberOfSegments))
			os.Truncate(getPath(w.numberOfSegments-1), int64(remainingFileData+w.lastSegmentSize))
			w.numberOfSegments--
			w.lastSegmentSize = int(remainingFileData + w.lastSegmentSize)
			f.Close()
			f2.Close()
			return
		} else {
			data2 = make([]byte, remainingBytesToTruncate)
			_, err = f2.Read(data2)
			if err != nil {
				fmt.Println("Error reading file: ", err)
				continue
			}

			f.Write(data2)
			if i+1 == w.numberOfSegments {
				f2.Seek(int64(remainingBytesToTruncate), 0)
				data3 := make([]byte, w.lastSegmentSize-remainingBytesToTruncate)
				_, err = f2.Read(data3)
				if err != nil {
					fmt.Println("Error reading file: ", err)
					continue
				}

				f2.Seek(0, 0)
				f2.Write(data3)
				os.Truncate(getPath(w.numberOfSegments), int64(w.lastSegmentSize-remainingBytesToTruncate))
				w.lastSegmentSize -= remainingBytesToTruncate
				f.Close()
				f2.Close()
				return
			}
		}
	}
	//if there is only one file left
	if w.numberOfSegments == 1 && remainingBytesToTruncate != 0 {
		f, err := os.OpenFile(getPath(1), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return
		}
		data := make([]byte, w.lastSegmentSize-remainingBytesToTruncate)
		f.Seek(int64(remainingBytesToTruncate), 0)
		_, err = f.Read(data)
		if err != nil {
			fmt.Println("Error reading file: ", err)
			return
		}
		f.Seek(0, 0)
		f.Write(data)
		os.Truncate(getPath(1), int64(w.lastSegmentSize-remainingBytesToTruncate))
		w.lastSegmentSize -= remainingBytesToTruncate
		f.Close()
	}
}

func (w *Wal) getSegmentSize(segmentIndex int) int {
	if segmentIndex == w.numberOfSegments {
		return w.lastSegmentSize
	}
	return w.segmentSize
}
