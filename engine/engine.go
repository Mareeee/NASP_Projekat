package engine

import (
	"bufio"
	"errors"
	"fmt"
	"main/bloom-filter"
	"main/cache"
	"main/cms"
	"main/config"
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
	"time"
)

type Engine struct {
	config                config.Config
	Cache                 cache.Cache
	Wal                   wal.Wal
	Tbucket               tokenbucket.TokenBucket
	all_memtables         []memtable.Memtable
	active_memtable_index int
}

// inicijalno pravljenje svih struktura
func (e *Engine) Engine() {
	err := config.LoadConfig(&e.config)
	if err != nil {
		e.config.WriteConfig()
	}

	// posto lsm nije struktura, zvacemo ga iz package-a
	e.Cache = *cache.NewCache(e.config)
	wal, _ := wal.LoadWal(e.config)
	e.Wal = *wal
	e.Tbucket = *tokenbucket.LoadTokenBucket(e.config)
	e.all_memtables = *memtable.LoadAllMemtables(e.config)
	e.active_memtable_index = 0

	// TODO: Uradi brisanje WAL-a
	e.recover()
}

func (e *Engine) Put(key string, value []byte) error {
	err := e.Wal.AddRecord(key, value, false)
	if err != nil {
		return errors.New("failed wal insert")
	}
	recordToAdd := record.NewRecord(key, value, false)
	e.AddRecordToMemtable(*recordToAdd)
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
	record, _ = sstable.Search(key)
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

	e.Wal.AddRecord(record.Key, record.Value, true)
	index := e.active_memtable_index
	deletedInMemtable := false
	for i := 0; i < e.config.NumberOfMemtables; i++ {
		if e.all_memtables[index].CurrentSize == 0 {
			break
		}

		found := e.all_memtables[index].Search(record.Key)
		if found != nil {
			e.all_memtables[index].Delete(*record)
			deletedInMemtable = true
			break
		}
		index = (index - 1) % e.config.NumberOfMemtables
	}

	if !deletedInMemtable {
		record.Tombstone = true
		e.all_memtables[e.active_memtable_index].Insert(*record)
		e.Cache.Set(key, *record)
		return nil
	}

	e.Cache.Set(key, *record)
	return nil
}

func (e *Engine) BloomFilterCreateNewInstance(key string) {
	bloomFilter := bloom.NewBloomFilterMenu(100, 95.0)
	value := bloomFilter.ToBytes()
	fmt.Println(len(value))
	e.Put("bf_"+key, value)
}

func (e *Engine) BloomFilterAddElement(key, element string) {
	bloom_record := e.Get(key)
	if bloom_record != nil {
		bloomFilter := *bloom.FromBytes(bloom_record.Value)
		bloomFilter.AddElement(element)
		value := bloomFilter.ToBytes()
		e.Put(key, value)
	}
}

func (e *Engine) BloomFilterCheckElement(key, element string) {
	bloom_record := e.Get(key)
	if bloom_record != nil {
		bloomFilter := *bloom.FromBytes(bloom_record.Value)
		fmt.Println(bloomFilter.CheckElement(element))
	}
}

func (e *Engine) CalculateFingerprintSimHash(key string, text string) error {
	fingerprint := simhash.CalculateFingerprint(text)
	value := simhash.ToBytes(fingerprint)
	err := e.Put(key, value)
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
	fmt.Println("hamming distance = " + fmt.Sprint(hamming))

	return nil
}

func (e *Engine) recover() error {
	all_records, err := e.Wal.IndependentLoadAllRecords()
	if err != nil {
		return err
	}

	for i := len(all_records) - 1; i >= 0; i-- {
		e.AddRecordToMemtable(all_records[i])
	}

	return nil
}

func (e *Engine) CmsUsage() {
	fmt.Println("1 - Create new cms.")
	fmt.Println("2 - Add new element")
	fmt.Println("3 - Delete cms")
	fmt.Println("4 - Check frequency of element")

	fmt.Print("Input option: ")
	optionScanner := bufio.NewScanner(os.Stdin)
	optionScanner.Scan()
	option := optionScanner.Text()
	if e.Tbucket.Take() {
		switch option {
		case "1":
			fmt.Print("Input key: ")
			keyScanner := bufio.NewScanner(os.Stdin)
			keyScanner.Scan()
			key := optionScanner.Text()
			cms := new(cms.CountMinSketch)
			cms.NewCountMinSketch(0.1, 0.1)
			e.Put("cms_"+key, cms.ToBytes())

		case "2":
			fmt.Print("Input key: ")
			keyScanner := bufio.NewScanner(os.Stdin)
			keyScanner.Scan()
			key := optionScanner.Text()
			record := e.Get(key)
			if record != nil && strings.HasPrefix(key, "cms_") {
				cms := *cms.LoadCMS(record.Value)
				fmt.Print("Input value: ")
				valueScanner := bufio.NewScanner(os.Stdin)
				valueScanner.Scan()
				value := valueScanner.Text()
				cms.AddElement(value)
				e.Put(record.Key, cms.ToBytes())
			}

		case "3":
			fmt.Print("Input key: ")
			keyScanner := bufio.NewScanner(os.Stdin)
			keyScanner.Scan()
			key := optionScanner.Text()
			e.Delete(key)
		case "4":
			fmt.Print("Input key: ")
			keyScanner := bufio.NewScanner(os.Stdin)
			keyScanner.Scan()
			key := optionScanner.Text()
			record := e.Get(key)
			if record != nil && strings.HasPrefix(key, "cms_") {
				cms := *cms.LoadCMS(record.Value)
				fmt.Print("Input value: ")
				valueScanner := bufio.NewScanner(os.Stdin)
				valueScanner.Scan()
				value := valueScanner.Text()
				fmt.Println(cms.NumberOfRepetitions(value))
				e.Put(record.Key, cms.ToBytes())
			}
		}
	} else {
		fmt.Println("Rate limit exceeded. Waiting...")
		time.Sleep(time.Second)
	}
}

func (e *Engine) AddRecordToMemtable(recordToAdd record.Record) {
	successful := e.all_memtables[e.active_memtable_index].Insert(recordToAdd)
	if !successful {
		// poveca pokazivac na aktivnu memtablelu i podeli po modulu da bi mogli da se pozicioniramo u listi
		e.active_memtable_index = (e.active_memtable_index + 1) % e.config.NumberOfMemtables

		if e.all_memtables[e.active_memtable_index].CurrentSize == e.config.MaxSize {
			memSize := e.all_memtables[e.active_memtable_index].SizeOfRecordsInWal
			all_records := e.all_memtables[e.active_memtable_index].Flush()
			e.Wal.DeleteWalSegmentsEngine(memSize)
			sstable.NewSSTable(all_records, &e.config, 1)
			e.all_memtables[e.active_memtable_index] = *memtable.MemtableConstructor(e.config)
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
		record, index := memtable.FindFirstPrefixMemtable(memtables[i], prefix, e.config.MemtableStructure)
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
			record, index := memtable.GetNextPrefixMemtable(memtables[i], prefix, memtableIndexes[i], e.config.MemtableStructure)
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

	return page[:pageSize]
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
			index, _ := strconv.Atoi(sstable_tokens[4])
			data = append(data, []int{level, index})

			sort.Slice(data, func(i, j int) bool {
				return data[i][1] < data[j][1]
			})
		}
	}

	return data
}
