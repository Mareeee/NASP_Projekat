package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"main/bloom-filter"
	"main/cache"
	"main/cms"
	"main/config"
	hll "main/hyperloglog"
	"main/lsm"
	"main/memtable"
	"main/record"
	"main/simhash"
	"main/sstable"
	tokenbucket "main/tokenBucket"
	"main/wal"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Engine struct {
	config                config.Config
	Cache                 cache.Cache
	Wal                   wal.Wal
	Tbucket               tokenbucket.TokenBucket
	all_memtables         []*memtable.Memtable
	active_memtable_index int
	KeyDictionary         map[int]string
}

// inicijalno pravljenje svih struktura
func (e *Engine) Engine() {
	err := config.LoadConfig(&e.config)
	if err != nil {
		e.config.WriteConfig()
	}

	// DESERIALIZE KEY DICT
	// posto lsm nije struktura, zvacemo ga iz package-a
	// e.KeyDictionary = make(map[int]string)
	e.Cache = *cache.NewCache(e.config)
	wal, _ := wal.LoadWal(e.config.SegmentSize)
	e.Wal = *wal
	e.Tbucket = *tokenbucket.LoadTokenBucket(e.config)
	e.all_memtables = memtable.LoadAllMemtables(e.config)
	e.active_memtable_index = 0

	e.recover()
	if e.config.Compress {
		f, _ := os.Open(config.KEY_DICTIONARY_FILE_PATH)
		defer f.Close()

		keyDictionaryBytes, _ := io.ReadAll(f)
		if len(keyDictionaryBytes) == 0 {
			e.KeyDictionary = make(map[int]string)
		} else {
			keyDictionary, _ := e.DeserializeMap(keyDictionaryBytes)
			e.KeyDictionary = keyDictionary
		}
	} else {
		e.KeyDictionary = nil
	}
}

func (e *Engine) Put(key string, value []byte, deleted bool) error {
	recordToAdd := e.Wal.AddRecord(key, value, deleted)
	e.addRecordToMemtable(*recordToAdd)
	e.Cache.Set(key, *recordToAdd)
	return nil
}

func (e *Engine) Get(key string) *record.Record {
	var record *record.Record
	// going through memtable
	i := e.active_memtable_index
	//is active memtable empty, if it is try previous
	if e.all_memtables[i].CurrentSize == 0 {
		previous_index := e.active_memtable_index - 1
		//we made a full circle
		if previous_index < 0 {
			previous_index = e.config.NumberOfMemtables - 1
		}
		i = previous_index
	}
	for j := 0; j < e.config.NumberOfMemtables; j++ {
		//we haven't found a record with the given key
		if e.all_memtables[i].CurrentSize == 0 {
			break
		}

		record = e.all_memtables[i].Search(key)
		//we found record with the key
		if record != nil && !record.Tombstone {
			return record
		}

		//going to next memtable
		i = i - 1
		if i < 0 {
			i = e.config.NumberOfMemtables - 1
		}
	}
	//going through cache
	record, found := e.Cache.Get(key)
	//we found it in cache
	if found && !record.Tombstone {
		return record
	}

	//going through sstable
	record, _ = sstable.Search(key, &e.KeyDictionary)
	//we found it in sstable
	if record != nil && !record.Tombstone {
		return record
	}

	//we haven't found a record with the given key or it's already been deleted
	return nil
}

func (e *Engine) Delete(key string) error {
	record := e.Get(key)
	if record == nil {
		return errors.New("record not found or unable to be deleted")
	}
	e.Put(record.Key, record.Value, true)
	return nil
}

// bloomfilter options

func (e *Engine) BloomFilterCreateNewInstance(key string, expectedElements int, falsePositiveRate float64) {
	if expectedElements > 100 || expectedElements < 0 || falsePositiveRate > 95.0 || falsePositiveRate < 0 {
		bloomFilter := bloom.NewBloomFilter(100, 95.0)
		value := bloomFilter.ToBytes()
		e.Put("bf_"+key, value, false)
	} else {
		bloomFilter := bloom.NewBloomFilter(expectedElements, falsePositiveRate)
		value := bloomFilter.ToBytes()
		e.Put("bf_"+key, value, false)
	}
}

func (e *Engine) BloomFilterAddElement(key, element string) {
	bloom_record := e.Get(key)
	if bloom_record != nil && strings.HasPrefix(key, "bf_") {
		bloomFilter := *bloom.FromBytes(bloom_record.Value)
		bloomFilter.AddElement(element)
		value := bloomFilter.ToBytes()
		e.Put(key, value, false)
	}
}

func (e *Engine) BloomFilterCheckElement(key, element string) {
	bloom_record := e.Get(key)
	if bloom_record != nil && strings.HasPrefix(key, "bf_") {
		bloomFilter := *bloom.FromBytes(bloom_record.Value)
		fmt.Println(bloomFilter.CheckElement(element))
	}
}

// hyperloglog options

func (e *Engine) HLLCreateNewInstance(key string, p int) {
	if p > 10 || p < 0 {
		hloglog := hll.NewHyperLogLog(4)
		data := hloglog.ToBytes()
		e.Put("hll_"+key, data, false)
	} else {
		hloglog := hll.NewHyperLogLog(uint8(p))
		data := hloglog.ToBytes()
		e.Put("hll_"+key, data, false)
	}
}

func (e *Engine) HLLDeleteInstance(key string) {
	if strings.HasPrefix(key, "hll_") {
		e.Delete(key)
	} else {
		fmt.Println("Such HyperLogLog doesn't exist")
	}
}

func (e *Engine) HLLAddElement(keyhll, key string) {
	record := e.Get(keyhll)
	//hyperloglog not found
	if record != nil && strings.HasPrefix(keyhll, "hll_") {
		data := record.Value
		hloglog := hll.LoadHLL(data)
		//adding a key
		hloglog.AddElement(key)
		e.Put(keyhll, hloglog.ToBytes(), false)
	} else {
		fmt.Println("Such HyperLogLog doesn't exist")
	}
}

func (e *Engine) HLLCardinality(key string) {
	record := e.Get(key)
	//hyperloglog not found
	if record != nil && strings.HasPrefix(key, "hll_") {
		data := record.Value
		hloglog := hll.LoadHLL(data)
		estimation := hloglog.Estimate()
		fmt.Println("The estimation of unique element is: ", estimation)
	} else {
		fmt.Println("Such HyperLogLog doesn't exist")
	}
}

// 	cms options

func (e *Engine) CMSCreateNewInstance(key string, epsilon, delta float64) {
	if epsilon > 1 || epsilon < 0 || delta > 1 || delta < 0 {
		cms := cms.NewCountMinSketch(0.1, 0.1)
		e.Put("cms_"+key, cms.ToBytes(), false)
	} else {
		cms := cms.NewCountMinSketch(epsilon, delta)
		e.Put("cms_"+key, cms.ToBytes(), false)
	}
}

func (e *Engine) CMSAddElement(key, value string) {
	record := e.Get(key)
	if record != nil && strings.HasPrefix(key, "cms_") {
		cms := *cms.LoadCMS(record.Value)
		cms.AddElement(value)
		e.Put(record.Key, cms.ToBytes(), false)
	}
}

func (e *Engine) CMSCheckFrequency(key, value string) {
	record := e.Get(key)
	if record != nil && strings.HasPrefix(key, "cms_") {
		cms := *cms.LoadCMS(record.Value)
		fmt.Println(cms.NumberOfRepetitions(value))
	}
}

// simhash options

func (e *Engine) CalculateFingerprintSimHash(key string, text string) error {
	fingerprint := simhash.CalculateFingerprint(text)
	value := simhash.ToBytes(fingerprint)
	err := e.Put("sh_"+key, value, false)
	if err != nil {
		return err
	}
	fmt.Println("fingerprint = " + string(value))

	return nil
}

func (e *Engine) CalculateHammingDistanceSimHash(key1, key2 string) error {
	record1 := e.Get(key1)
	if record1 == nil {
		return errors.New("key not found")
	}
	record2 := e.Get(key2)
	if record2 == nil {
		return errors.New("key not found")
	}

	fingerprint1 := simhash.LoadFromBytes(record1.Value)
	fingerprint2 := simhash.LoadFromBytes(record2.Value)
	hamming := simhash.HammingDistance(fingerprint1, fingerprint2)
	fmt.Println("Hamming distance = " + fmt.Sprint(hamming))

	return nil
}

func (e *Engine) recover() error {
	all_records, err := e.Wal.IndependentLoadAllRecords()
	if err != nil {
		return err
	}

	for i := len(all_records) - 1; i >= 0; i-- {
		e.addRecordToMemtable(all_records[i])
	}

	return nil
}

func (e *Engine) addRecordToMemtable(recordToAdd record.Record) {
	successful := e.all_memtables[e.active_memtable_index].Insert(recordToAdd)
	if !successful {
		// poveca pokazivac na aktivnu memtablelu i podeli po modulu da bi mogli da se pozicioniramo u listi
		e.active_memtable_index = (e.active_memtable_index + 1) % e.config.NumberOfMemtables

		if e.all_memtables[e.active_memtable_index].CurrentSize == e.config.MaxSize {
			memSize := e.all_memtables[e.active_memtable_index].SizeOfRecordsInWal
			all_records := e.all_memtables[e.active_memtable_index].Flush()
			e.Wal.DeleteWalSegmentsEngine(memSize)
			sstable.NewSSTable(all_records, &e.config, 1, &e.KeyDictionary)
			lsm.Compact(&e.config, &e.KeyDictionary)
			e.all_memtables[e.active_memtable_index] = memtable.MemtableConstructor(e.config)
		}
		e.all_memtables[e.active_memtable_index].Insert(recordToAdd)
	}
}

func (e *Engine) PrefixScan(prefix string, pageNumber, pageSize int) []record.Record {
	var page []record.Record
	currentPage := 1

	sstables := allSSTables()
	var sstablesOffsets []int
	memtables := e.all_memtables
	var memtableIndexes []int

	i := 0
	for i < len(memtables) && len(memtables) != 0 {
		record, index := memtable.FindFirstPrefixMemtable(*memtables[i], prefix, e.config.MemtableStructure)
		if record != nil {
			page = append(page, *record)
			memtableIndexes = append(memtableIndexes, index)
			i++
		} else {
			memtables = append(memtables[:i], memtables[i+1:]...)
		}
	}

	i = 0
	for i < len(sstables) && len(sstables) != 0 {
		record, offset, _ := sstable.FindFirstPrefixSSTable(sstables[i][0], sstables[i][1], prefix)
		if record != nil {
			page = append(page, *record)
			sstablesOffsets = append(sstablesOffsets, offset)
			i++
		} else {
			sstables = append(sstables[:i], sstables[i+1:]...)
		}
	}

	if len(memtables) == 0 && len(sstables) == 0 {
		return nil
	}

	page = sortAndRemoveSame(page)

	if pageNumber == 1 && len(page) >= pageSize {
		return page[:pageSize]
	}

	for len(memtables) != 0 && len(sstables) != 0 {
		i := 0
		for i < len(memtables) && len(memtables) != 0 {
			record, index := memtable.GetNextPrefixMemtable(*memtables[i], prefix, memtableIndexes[i], e.config.MemtableStructure)
			if record != nil {
				page = append(page, *record)
				memtableIndexes = append(memtableIndexes, index)
				i++
			} else {
				memtables = append(memtables[:i], memtables[i+1:]...)
			}
		}

		i = 0
		for i < len(sstables) && len(sstables) != 0 {
			record, offset, _ := sstable.GetNextPrefixSSTable(sstables[i][0], sstables[i][1], prefix, int64(sstablesOffsets[i]))
			if record != nil {
				page = append(page, *record)
				sstablesOffsets = append(sstablesOffsets, offset)
				i++
			} else {
				sstables = append(sstables[:i], sstables[i+1:]...)
			}
		}

		page = sortAndRemoveSame(page)

		if len(page) >= pageSize && currentPage == pageNumber {
			break
		} else if len(page) >= pageSize && currentPage < pageNumber {
			currentPage++
			page = page[pageSize:]
		} else {
			continue
		}

	}

	if len(page) <= pageSize {
		return page
	} else {
		return page[:pageSize]
	}
}

func (e *Engine) RangeScan(minKey, maxKey string, pageNumber, pageSize int) []record.Record {
	var page []record.Record
	currentPage := 1

	sstables := allSSTables()
	var sstablesOffsets []int
	memtables := e.all_memtables
	var memtableIndexes []int

	i := 0
	for i < len(memtables) && len(memtables) != 0 {
		record, index := memtable.FindMinRangeScanMemtable(*memtables[i], minKey, maxKey, e.config.MemtableStructure)
		if record != nil {
			page = append(page, *record)
			memtableIndexes = append(memtableIndexes, index)
			i++
		} else {
			memtables = append(memtables[:i], memtables[i+1:]...)
		}
	}

	i = 0
	for i < len(sstables) && len(sstables) != 0 {
		record, offset, _ := sstable.FindMinKeyRangeScanSSTable(sstables[i][0], sstables[i][1], minKey, maxKey)
		if record != nil {
			page = append(page, *record)
			sstablesOffsets = append(sstablesOffsets, offset)
			i++
		} else {
			sstables = append(sstables[:i], sstables[i+1:]...)
		}
	}

	if len(memtables) == 0 && len(sstables) == 0 {
		return nil
	}

	page = sortAndRemoveSame(page)

	if pageNumber == 1 && len(page) >= pageSize {
		return page[:pageSize]
	}

	for len(memtables) != 0 && len(sstables) != 0 {
		i := 0
		for i < len(memtables) && len(memtables) != 0 {
			record, index := memtable.GetNextMinRangeScanMemtable(*memtables[i], minKey, maxKey, memtableIndexes[i], e.config.MemtableStructure)
			if record != nil {
				page = append(page, *record)
				memtableIndexes = append(memtableIndexes, index)
				i++
			} else {
				memtables = append(memtables[:i], memtables[i+1:]...)
			}
		}

		i = 0
		for i < len(sstables) && len(sstables) != 0 {
			record, offset, _ := sstable.GetNextMinRangeScanSSTable(sstables[i][0], sstables[i][1], minKey, maxKey, int64(sstablesOffsets[i]))
			if record != nil {
				page = append(page, *record)
				sstablesOffsets = append(sstablesOffsets, offset)
				i++
			} else {
				sstables = append(sstables[:i], sstables[i+1:]...)
			}
		}

		page = sortAndRemoveSame(page)

		if len(page) >= pageSize && currentPage == pageNumber {
			break
		} else if len(page) >= pageSize && currentPage < pageNumber {
			currentPage++
			page = page[pageSize:]
		} else {
			continue
		}

	}

	if len(page) <= pageSize {
		return page
	} else {
		return page[:pageSize]
	}
}

func sortAndRemoveSame(page []record.Record) []record.Record {
	sort.Slice(page, func(i, j int) bool {
		if page[i].Key != page[j].Key {
			return page[i].Key < page[j].Key
		}
		return page[i].Timestamp > page[j].Timestamp
	})

	var result []record.Record
	seen := make(map[string]bool)
	for _, r := range page {
		if !seen[r.Key] {
			seen[r.Key] = true
			result = append(result, r)
		}
	}

	return result
}

func allSSTables() [][]int {
	var data [][]int
	files, _ := os.ReadDir(config.SSTABLE_DIRECTORY)

	for _, file := range files {
		if strings.Contains(file.Name(), "sstable_data") {
			sstable_tokens := strings.Split(file.Name(), "_")
			level, _ := strconv.Atoi(sstable_tokens[1])
			index, _ := strconv.Atoi(strings.Split(sstable_tokens[4], ".")[0])
			data = append(data, []int{level, index})

			sort.Slice(data, func(i, j int) bool {
				return data[i][1] < data[j][1]
			})
		}
	}

	return data
}

func (e *Engine) SerializeMap(m map[int]string) ([]byte, error) {
	return json.Marshal(m)
}

func (e *Engine) DeserializeMap(data []byte) (map[int]string, error) {
	var m map[int]string
	err := json.Unmarshal(data, &m)
	return m, err
}
