package config

import (
	"encoding/json"
	"os"
)

const (
	ALL_CONFIG_FILE_PATH       = "config/config.json"
	SEGMENT_FILE_PATH          = "data/wal/wal_"
	SSTABLE_DIRECTORY          = "data/sstable/"
	TOKENBUCKET_STATE          = "data/token_bucket/token_bucket_state.bin"
	CMS_FILE_PATH              = "data/cms/cms.bin"
	HLL_FILE_PATH              = "data/hll/hll.bin"
	HLL_MIN_PRECISION          = 4
	HLL_MAX_PRECISION          = 16
	CONFIG_NUMBER_OF_LEVELS    = 5
	CONFIG_MAX_TABLES          = 4
	CONFIG_NUMBER_OF_SEGMENTS  = 0
	CONFIG_SEGMENT_SIZE        = 3
	CONFIG_LAST_SEGMENT_SIZE   = 0
	CONFIG_MAX_HEIGHT          = 5
	CONFIG_NUMBER_OF_SSTABLES  = 0
	CONFIG_INDEX_INTERVAL      = 2
	CONFIG_SUMMARY_INTERVAL    = 2
	CONFIG_CAPACITY            = 10
	CONFIG_RATE                = 2
	CONFIG_MAX_SIZE            = 9
	CONFIG_MEMTABLE_STRUCTURE  = "skiplist"
	CONFIG_NUMBER_OF_MEMTABLES = 2
	CONFIG_CACHE_MAX_SIZE      = 3
)

type Config struct {
	// lsm
	NumberOfLevels int `json:"NumberOfLevels"`
	MaxTabels      int `json:"MaxTables"`
	// wal
	NumberOfSegments int `json:"NumberOfSegments"`
	SegmentSize      int `json:"SegmentSize"`
	LastSegmentSize  int `json:"LastSegmentSize"`
	// skiplist
	MaxHeight int `json:"MaxHeight"`
	// sstable
	NumberOfSSTables int `json:"NumberOfSSTables"`
	IndexInterval    int `json:"IndexInterval"`
	SummaryInterval  int `json:"SummaryInterval"`
	// tokenBucket
	Capacity uint64 `json:"Capacity"`
	Rate     uint64 `json:"Rate"`
	// bTree
	M int `json:"M"`
	// memtable
	MaxSize           int    `json:"MaxSize"`
	MemtableStructure string `json:"MemtableStructure"`
	NumberOfMemtables int    `json:"NumberOfMemtables"`
	// cache
	CacheMaxSize int `json:"CacheMaxSize"`
}

func (cfg *Config) checkValidity() {

	if cfg.NumberOfLevels < 0 {
		cfg.NumberOfLevels = CONFIG_NUMBER_OF_LEVELS
	}

	if cfg.MaxTabels < 0 {
		cfg.MaxTabels = CONFIG_MAX_TABLES
	}

	if cfg.NumberOfSegments < 0 {
		cfg.NumberOfSegments = CONFIG_NUMBER_OF_SEGMENTS
	}

	if cfg.SegmentSize < 0 {
		cfg.SegmentSize = CONFIG_SEGMENT_SIZE
	}

	if cfg.LastSegmentSize < 0 {
		cfg.LastSegmentSize = CONFIG_LAST_SEGMENT_SIZE
	}

	if cfg.MaxHeight < 0 {
		cfg.MaxHeight = CONFIG_MAX_HEIGHT
	}

	if cfg.NumberOfSSTables < 0 {
		cfg.NumberOfSSTables = CONFIG_NUMBER_OF_SSTABLES
	}

	if cfg.IndexInterval < 0 {
		cfg.IndexInterval = CONFIG_INDEX_INTERVAL
	}

	if cfg.SummaryInterval < 0 {
		cfg.SummaryInterval = CONFIG_SUMMARY_INTERVAL
	}

	if cfg.Capacity < uint64(0) {
		cfg.Capacity = CONFIG_CAPACITY
	}

	if cfg.Rate < uint64(0) {
		cfg.Rate = CONFIG_RATE
	}

	if cfg.MaxSize < 0 {
		cfg.MaxSize = CONFIG_MAX_SIZE
	}

	if cfg.MemtableStructure != "skiplist" && cfg.MemtableStructure != "btree" {
		cfg.MemtableStructure = CONFIG_MEMTABLE_STRUCTURE
	}

	if cfg.NumberOfMemtables < 0 {
		cfg.NumberOfMemtables = CONFIG_NUMBER_OF_MEMTABLES
	}

	if cfg.CacheMaxSize < 0 {
		cfg.CacheMaxSize = CONFIG_CACHE_MAX_SIZE
	}
}

func LoadConfig(cfg *Config) error {
	jsonFile, err := os.ReadFile(ALL_CONFIG_FILE_PATH)
	// ako nema fajla, postavlja na default vrednosti
	if err != nil {
		cfg.NumberOfLevels = CONFIG_NUMBER_OF_LEVELS
		cfg.MaxTabels = CONFIG_MAX_TABLES
		cfg.NumberOfSegments = CONFIG_NUMBER_OF_SEGMENTS
		cfg.SegmentSize = CONFIG_SEGMENT_SIZE
		cfg.LastSegmentSize = CONFIG_LAST_SEGMENT_SIZE
		cfg.MaxHeight = CONFIG_MAX_HEIGHT
		cfg.NumberOfSSTables = CONFIG_NUMBER_OF_SSTABLES
		cfg.IndexInterval = CONFIG_INDEX_INTERVAL
		cfg.SummaryInterval = CONFIG_SUMMARY_INTERVAL
		cfg.Capacity = CONFIG_CAPACITY
		cfg.Rate = CONFIG_RATE
		cfg.MaxSize = CONFIG_MAX_SIZE
		cfg.MemtableStructure = CONFIG_MEMTABLE_STRUCTURE
		cfg.NumberOfMemtables = CONFIG_NUMBER_OF_MEMTABLES
		cfg.CacheMaxSize = CONFIG_CACHE_MAX_SIZE
	} else {
		err = json.Unmarshal(jsonFile, &cfg)
		if err != nil {
			return err
		}
		cfg.checkValidity()
	}

	return nil
}

/* Upisuje opcije u config JSON fajl */
func (c *Config) WriteConfig() error {
	jsonData, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	os.WriteFile(ALL_CONFIG_FILE_PATH, jsonData, 0644)
	return nil
}
