package engine

import (
	"errors"
	"fmt"
	"main/bloom-filter"
	"main/cache"
	"main/config"
	"main/lsm"
	"main/memtable"
	"main/record"
	"main/simhash"
	"main/sstable"
	tokenbucket "main/tokenBucket"
	"main/wal"
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
	for {
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
	all_records, err := e.Wal.LoadAllRecords()
	if err != nil {
		return err
	}

	for i := len(all_records) - 1; i >= 0; i-- {
		e.AddRecordToMemtable(*all_records[i])
	}

	return nil
}

func (e *Engine) AddRecordToMemtable(recordToAdd record.Record) {
	successful := e.all_memtables[e.active_memtable_index].Insert(recordToAdd)
	if !successful {
		// poveca pokazivac na aktivnu memtablelu i podeli po modulu da bi mogli da se pozicioniramo u listi
		e.active_memtable_index = (e.active_memtable_index + 1) % e.config.NumberOfMemtables

		if e.all_memtables[e.active_memtable_index].CurrentSize == e.config.MaxSize {
			all_records := e.all_memtables[e.active_memtable_index].Flush()
			sstable.NewSSTable(all_records, &e.config, 1)
			uradilo := lsm.Compact(&e.config, "sizeTiered")
			if uradilo {
				fmt.Println("Radi")
			}
			e.all_memtables[e.active_memtable_index] = *memtable.MemtableConstructor(e.config)
		}
	}
}
