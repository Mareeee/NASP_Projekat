package config

import (
	"encoding/json"
	"os"
)

const (
	ALL_CONFIG_FILE_PATH = "config/config.json"
	SEGMENT_FILE_PATH    = "data/wal/wal_"
	SSTABLE_DIRECTORY    = "data/sstable"
	DATA_FILE_PATH       = "data/sstable/"
	INDEX_FILE_PATH      = "data/sstable/sstable_index_"
	SUMMARY_FILE_PATH    = "data/sstable/sstable_summary_"
	FILTER_FILE_PATH     = "data/sstable/sstable_filter_"
	METADATA_FILE_PATH   = "data/sstable/sstable_metadata_"
	TOKENBUCKET_STATE    = "data/token_bucket/token_bucket_state.bin"
)

type Config struct {
	// lsm
	NumberOfLevels int `json:"NumberOfLevels"`
	MaxTabels      int `json:"MaxTables"`
	// wal
	NumberOfSegments           int `json:"NumberOfSegments"`
	LowWaterMark               int `json:"LowWaterMark"`
	SegmentSize                int `json:"SegmentSize"`
	LastSegmentNumberOfRecords int `json:"LastSegmentNumberOfRecords"`
	// skiplist
	MaxHeight int `json:"MaxHeight"`
	// sstable
	NumberOfSSTables int `json:"NumberOfSSTables"`
	IndexInterval    int `json:"IndexInterval"`
	SummaryInterval  int `json:"SummaryInterval"`
	// tokenBucket
	Capacity uint64 `json:"Capacity"`
	Rate     uint64 `json:"Rate"`
}

/* Ucitava WalOptions iz config JSON fajla */
func (c *Config) LoadJson() error {
	jsonData, err := os.ReadFile(ALL_CONFIG_FILE_PATH)
	if err != nil {
		return err
	}
	json.Unmarshal(jsonData, &c)
	return nil
}

/* Upisuje WalOptions u config JSON fajl */
func (c *Config) WriteJson() error {
	jsonData, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	os.WriteFile(ALL_CONFIG_FILE_PATH, jsonData, 0644)
	return nil
}
