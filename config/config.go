package config

import (
	"encoding/json"
	"os"
)

const (
	ALL_CONFIG_FILE_PATH       = "config/config.json"
	WAL_DIRECTORY              = "data/wal/"
	SEGMENT_FILE_PATH          = "data/wal/wal_"
	SSTABLE_DIRECTORY          = "data/sstable/"
	TOKENBUCKET_STATE          = "data/token_bucket/token_bucket_state.bin"
	CMS_FILE_PATH              = "data/cms/cms.bin"
	HLL_FILE_PATH              = "data/hll/hll.bin"
	KEY_DICTIONARY_FILE_PATH   = "data/keyDictionary/keyDictionary.bin"
	HLL_MIN_PRECISION          = 4
	HLL_MAX_PRECISION          = 16
	CONFIG_NUMBER_OF_LEVELS    = 5
	CONFIG_MAX_TABLES          = 4
	CONFIG_SEGMENT_SIZE        = 3
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
	CONFIG_COMPACT_BY          = "byte"
	CONFIG_MAX_BYTES_SSTABLES  = 128
	CONFIG_COMPACT_TYPE        = "size_tiered"
	CONFIG_COMPRESS            = false
	CONFIG_M                   = 4
)

type Config struct {
	// lsm
	NumberOfLevels   int    `json:"NumberOfLevels"`
	MaxTabels        int    `json:"MaxTables"`
	CompactBy        string `json:"CompactBy"`
	MaxBytesSSTables int    `json:"MaxBytesSSTables"`
	CompactType      string `json:"CompactType"`
	// wal
	SegmentSize int `json:"SegmentSize"`
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
	//other
	Compress bool `json:"Compress"`
}

func (cfg *Config) checkValidity() {

	if cfg.NumberOfLevels < 0 {
		cfg.NumberOfLevels = CONFIG_NUMBER_OF_LEVELS
	}

	if cfg.MaxTabels < 0 {
		cfg.MaxTabels = CONFIG_MAX_TABLES
	}

	if cfg.SegmentSize < 0 {
		cfg.SegmentSize = CONFIG_SEGMENT_SIZE
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

	if cfg.CompactBy != "byte" && cfg.CompactBy != "amount" {
		cfg.CompactBy = CONFIG_COMPACT_BY
	}

	if cfg.MaxBytesSSTables < 0 {
		cfg.MaxBytesSSTables = CONFIG_MAX_BYTES_SSTABLES
	}

	if cfg.CompactType != "size_tiered" && cfg.CompactType != "level" {
		cfg.CompactType = CONFIG_COMPACT_TYPE
	}

	if cfg.Compress != false && cfg.Compress != true {
		cfg.Compress = CONFIG_COMPRESS
	}

	if cfg.M < 4 {
		cfg.M = CONFIG_M
	}

}

func LoadConfig(cfg *Config) error {
	jsonFile, err := os.ReadFile(ALL_CONFIG_FILE_PATH)
	// ako nema fajla, postavlja na default vrednosti
	if err != nil {
		cfg.NumberOfLevels = CONFIG_NUMBER_OF_LEVELS
		cfg.MaxTabels = CONFIG_MAX_TABLES
		cfg.SegmentSize = CONFIG_SEGMENT_SIZE
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
		cfg.CompactBy = CONFIG_COMPACT_BY
		cfg.MaxBytesSSTables = CONFIG_MAX_BYTES_SSTABLES
		cfg.CompactType = CONFIG_COMPACT_TYPE
		cfg.Compress = CONFIG_COMPRESS
		cfg.M = CONFIG_M
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
