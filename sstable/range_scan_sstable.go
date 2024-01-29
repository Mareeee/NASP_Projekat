package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"main/record"
	"os"
	"strconv"
)

func RangeScan(minKey string, maxKey string) ([]record.Record, error) {
	var options SSTableOptions
	options.LoadJson()

	var results []record.Record

	for i := options.NumberOfSSTables; i > 0; i-- {
		lastKey, offset := loadAndFindIndexOffsetRangeScan(i, minKey)
		if lastKey == "" && offset == -1 {
			continue
		}

		valueOffset := loadAndFindValueOffsetRangeScan(i, uint64(offset), minKey, lastKey)
		if valueOffset == -1 {
			return nil, errors.New("Inputed key does not exist!")
		}

		for {
			record := loadRecordRangeScan(i, minKey, uint64(valueOffset))

			if record == nil {
				fmt.Println(valueOffset)
				break
			}

			if record.Key > maxKey {
				break
			}

			results = append(results, *record)

			valueOffset += 29 + int64(record.KeySize) + int64(record.ValueSize)
		}
	}

	return results, nil
}

func loadAndFindIndexOffsetRangeScan(fileNumber int, minKey string) (string, int64) {
	f, err := os.Open(SUMMARY_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return "", -1
	}
	defer f.Close()

	var initialOffset int64 = 0

	for {
		_, seekErr := f.Seek(initialOffset, io.SeekStart)
		if seekErr != nil {
			fmt.Println("Error seeking in file:", seekErr)
			return "", -1
		}

		firstKeySizeBytes := make([]byte, 8)
		_, readErr := f.Read(firstKeySizeBytes)
		if readErr == io.EOF {
			fmt.Println("End of file reached.")
			return "", -1
		} else if readErr != nil {
			fmt.Println("Error reading first key size:", readErr)
			return "", -1
		}
		firstKeySize := binary.BigEndian.Uint64(firstKeySizeBytes)

		firstKeyBytes := make([]byte, firstKeySize)
		_, readErr = f.Read(firstKeyBytes)
		if readErr != nil {
			fmt.Println("Error reading first key:", readErr)
			return "", -1
		}
		firstKey := string(firstKeyBytes)

		lastKeySizeBytes := make([]byte, 8)
		_, readErr = f.Read(lastKeySizeBytes)
		if readErr != nil {
			fmt.Println("Error reading last key size:", readErr)
			return "", -1
		}
		lastKeySize := binary.BigEndian.Uint64(lastKeySizeBytes)

		lastKeyBytes := make([]byte, lastKeySize)
		_, readErr = f.Read(lastKeyBytes)
		if readErr != nil {
			fmt.Println("Error reading last key:", readErr)
			return "", -1
		}
		lastKey := string(lastKeyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			fmt.Println("Error reading offset:", readErr)
			return "", -1
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if minKey >= firstKey && minKey <= lastKey {
			return lastKey, offset
		}

		initialOffset += 24 + int64(firstKeySize) + int64(lastKeySize)
	}
}

func loadAndFindValueOffsetRangeScan(fileNumber int, summaryOffset uint64, minKey string, lastKey string) int64 {
	f, err := os.Open(INDEX_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return -1
	}
	defer f.Close()

	var lastReadOffset int64

	for {
		_, seekErr := f.Seek(int64(summaryOffset), io.SeekStart)
		if seekErr != nil {
			fmt.Println("Error seeking in file:", seekErr)
			return -1
		}

		keySizeBytes := make([]byte, 8)
		_, readErr := f.Read(keySizeBytes)
		if readErr == io.EOF {
			break
		} else if readErr != nil {
			fmt.Println("Error reading key size:", readErr)
			return -1
		}
		keySize := binary.BigEndian.Uint64(keySizeBytes)

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			fmt.Println("Error reading key:", readErr)
			return -1
		}
		foundKey := string(keyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			fmt.Println("Error reading offset:", readErr)
			return -1
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if minKey >= foundKey {
			lastReadOffset = offset
		} else {
			return lastReadOffset
		}

		if foundKey == lastKey {
			break
		}

		summaryOffset += 16 + keySize
	}

	return lastReadOffset
}

func loadRecordRangeScan(fileNumber int, minKey string, valueOffset uint64) *record.Record {
	f, err := os.Open(DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		fmt.Println("Error opening data file:", err)
		return nil
	}
	defer f.Close()

	for {
		_, seekErr := f.Seek(int64(valueOffset), io.SeekStart)
		if seekErr != nil {
			fmt.Println("Error seeking in data file:", seekErr)
			return nil
		}

		headerBytes := make([]byte, 29)
		_, readErr := f.Read(headerBytes)
		if readErr == io.EOF {
			return nil
		} else if readErr != nil {
			fmt.Println("Error reading header:", readErr)
			return nil
		}

		crc32 := binary.BigEndian.Uint32(headerBytes[0:4])
		timestamp := int64(binary.BigEndian.Uint64(headerBytes[4:12]))
		tombstone := headerBytes[12] != 0
		keySize := int64(binary.BigEndian.Uint64(headerBytes[13:21]))
		valueSize := int64(binary.BigEndian.Uint64(headerBytes[21:29]))

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			fmt.Println("Error reading key:", readErr)
			return nil
		}
		loadedKey := string(keyBytes)

		value := make([]byte, valueSize)
		_, readErr = f.Read(value)
		if readErr != nil {
			fmt.Println("Error reading value:", readErr)
			return nil
		}

		// checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, minKey, value)
		// if checkCrc32 != crc32 {
		// 	valueOffset += 29 + uint64(keySize) + uint64(valueSize)
		// 	continue
		// }

		if loadedKey >= minKey {
			return record.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, loadedKey, value)
		}

		valueOffset += 29 + uint64(keySize) + uint64(valueSize)
	}
}
