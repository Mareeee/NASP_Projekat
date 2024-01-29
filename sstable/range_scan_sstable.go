package sstable

import (
	"encoding/binary"
	"errors"
	"io"
	"main/config"
	"main/record"
	"os"
	"strconv"
)

func RangeScan(minKey, maxKey string) ([]*record.Record, error) {
	if minKey >= maxKey {
		return nil, errors.New("Min key has to be lower than Max key!")
	}

	cfg := new(config.Config)
	config.LoadConfig(cfg)

	var resultRecords []*record.Record

	for i := cfg.NumberOfSSTables; i > 0; i-- {
		_, err := LoadSSTable(i)
		if err != nil {
			return nil, err
		}

		lastKey, offset, err := loadAndFindIndexOffsetRangeScan(i, minKey)
		if err != nil {
			return nil, err
		}

		valueOffset, err := loadAndFindValueOffsetRangeScan(i, uint64(offset), minKey, lastKey)
		if err != nil {
			return nil, err
		}

		minKeyOffset, err := findMinKeyOffset(i, minKey, maxKey, uint64(valueOffset))
		if err != nil {
			return nil, err
		} else if minKeyOffset == -1 {
			return nil, nil
		}

		record, err := loadRecordRangeScan(i, minKey, maxKey, uint64(minKeyOffset))
		if err != nil {
			return nil, err
		}

		for record != nil {
			if !record.Tombstone {
				resultRecords = append(resultRecords, record)
			}

			minKeyOffset += int64(len(record.ToBytes()))
			record, err = loadRecordRangeScan(i, minKey, maxKey, uint64(minKeyOffset))
			if err != nil {
				return nil, err
			}
		}
	}

	return resultRecords, nil
}

func loadAndFindIndexOffsetRangeScan(fileNumber int, minKey string) (string, int64, error) {
	f, err := os.Open(config.SUMMARY_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return "", -1, err
	}
	defer f.Close()

	var initialOffset int64 = 0

	for {
		_, seekErr := f.Seek(initialOffset, io.SeekStart)
		if seekErr != nil {
			return "", -1, seekErr
		}

		firstKeySizeBytes := make([]byte, 8)
		_, readErr := f.Read(firstKeySizeBytes)
		if readErr == io.EOF {
			return "", -1, readErr
		} else if readErr != nil {
			return "", -1, readErr
		}
		firstKeySize := binary.BigEndian.Uint64(firstKeySizeBytes)

		firstKeyBytes := make([]byte, firstKeySize)
		_, readErr = f.Read(firstKeyBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		firstKey := string(firstKeyBytes)

		lastKeySizeBytes := make([]byte, 8)
		_, readErr = f.Read(lastKeySizeBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		lastKeySize := binary.BigEndian.Uint64(lastKeySizeBytes)

		lastKeyBytes := make([]byte, lastKeySize)
		_, readErr = f.Read(lastKeyBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		lastKey := string(lastKeyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			return "", -1, readErr
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if minKey >= firstKey && minKey <= lastKey {
			return lastKey, offset, nil
		}

		initialOffset += 24 + int64(firstKeySize) + int64(lastKeySize)
	}
}

func loadAndFindValueOffsetRangeScan(fileNumber int, summaryOffset uint64, minKey string, lastKey string) (int64, error) {
	f, err := os.Open(config.INDEX_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return -1, err
	}
	defer f.Close()

	var lastReadOffset int64

	for {
		_, seekErr := f.Seek(int64(summaryOffset), io.SeekStart)
		if seekErr != nil {
			return -1, seekErr
		}

		keySizeBytes := make([]byte, 8)
		_, readErr := f.Read(keySizeBytes)
		if readErr == io.EOF {
			return -1, readErr
		} else if readErr != nil {
			return -1, readErr
		}
		keySize := binary.BigEndian.Uint64(keySizeBytes)

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			return -1, readErr
		}
		foundKey := string(keyBytes)

		offsetBytes := make([]byte, 8)
		_, readErr = f.Read(offsetBytes)
		if readErr != nil {
			return -1, readErr
		}
		offset := int64(binary.BigEndian.Uint64(offsetBytes))

		if minKey >= foundKey {
			lastReadOffset = offset
		} else {
			return lastReadOffset, nil
		}

		if foundKey == lastKey {
			break
		}

		summaryOffset += 16 + keySize
	}

	return lastReadOffset, nil
}

func loadRecordRangeScan(fileNumber int, minKey string, maxKey string, valueOffset uint64) (*record.Record, error) {
	f, err := os.Open(config.DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, seekErr := f.Seek(int64(valueOffset), io.SeekStart)
	if seekErr != nil {
		return nil, seekErr
	}

	headerBytes := make([]byte, 29)
	_, readErr := f.Read(headerBytes)
	if readErr == io.EOF {
		return nil, readErr
	} else if readErr != nil {
		return nil, readErr
	}

	crc32 := binary.BigEndian.Uint32(headerBytes[0:4])
	timestamp := int64(binary.BigEndian.Uint64(headerBytes[4:12]))
	tombstone := headerBytes[12] != 0
	keySize := int64(binary.BigEndian.Uint64(headerBytes[13:21]))
	valueSize := int64(binary.BigEndian.Uint64(headerBytes[21:29]))

	keyBytes := make([]byte, keySize)
	_, readErr = f.Read(keyBytes)
	if readErr != nil {
		return nil, readErr
	}
	loadedKey := string(keyBytes)

	value := make([]byte, valueSize)
	_, readErr = f.Read(value)
	if readErr != nil {
		return nil, readErr
	}

	checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, loadedKey, value)
	if checkCrc32 != crc32 {
		valueOffset += 29 + uint64(keySize) + uint64(valueSize)
		return nil, errors.New("CRC doesn't match!")
	}

	if loadedKey >= minKey && loadedKey <= maxKey {
		return record.LoadRecord(crc32, timestamp, tombstone, keySize, valueSize, loadedKey, value), nil
	}

	return nil, nil
}

func findMinKeyOffset(fileNumber int, minKey string, maxKey string, valueOffset uint64) (int64, error) {
	f, err := os.Open(config.DATA_FILE_PATH + strconv.Itoa(fileNumber) + ".db")
	if err != nil {
		return -1, err
	}
	defer f.Close()

	for {
		_, seekErr := f.Seek(int64(valueOffset), io.SeekStart)
		if seekErr != nil {
			return -1, seekErr
		}

		headerBytes := make([]byte, 29)
		_, readErr := f.Read(headerBytes)
		if readErr == io.EOF {
			return -1, readErr
		} else if readErr != nil {
			return -1, readErr
		}

		crc32 := binary.BigEndian.Uint32(headerBytes[0:4])
		timestamp := int64(binary.BigEndian.Uint64(headerBytes[4:12]))
		tombstone := headerBytes[12] != 0
		keySize := int64(binary.BigEndian.Uint64(headerBytes[13:21]))
		valueSize := int64(binary.BigEndian.Uint64(headerBytes[21:29]))

		keyBytes := make([]byte, keySize)
		_, readErr = f.Read(keyBytes)
		if readErr != nil {
			return -1, readErr
		}
		loadedKey := string(keyBytes)

		value := make([]byte, valueSize)
		_, readErr = f.Read(value)
		if readErr != nil {
			return -1, readErr
		}

		checkCrc32 := record.CalculateCRC(timestamp, tombstone, keySize, valueSize, loadedKey, value)
		if checkCrc32 != crc32 {
			valueOffset += 29 + uint64(keySize) + uint64(valueSize)
			continue
		}

		if loadedKey < minKey {
			valueOffset += 29 + uint64(keySize) + uint64(valueSize)
			continue
		} else if loadedKey > maxKey {
			return -1, nil
		} else {
			return int64(valueOffset), nil
		}
	}
}
