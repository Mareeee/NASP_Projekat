package engine

import (
	"errors"
	"main/cache"
	"main/config"
	"main/memtable"
	"main/record"
	"main/sstable"
	tokenbucket "main/tokenBucket"
	"main/wal"
)

type Engine struct {
	config                config.Config
	cache                 cache.Cache
	sstable               sstable.SSTable
	wal                   wal.Wal
	record                record.Record
	tbucket               tokenbucket.TokenBucket
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
	e.cache = *cache.NewCache(e.config.MaxSize)
	wal, _ := wal.LoadWal(e.config)
	e.wal = *wal
	e.tbucket = *tokenbucket.LoadTokenBucket(e.config)
	e.all_memtables = *memtable.LoadAllMemtables(e.config)
	e.active_memtable_index = 0

	e.recover()

	for i := 0; i < len(e.all_memtables); i++ {
		e.all_memtables[i].PrintMemtableRecords()
	}
}

func (e *Engine) Put(key string, value []byte) error {
	err := e.wal.AddRecord(key, value, false)
	if err != nil {
		return errors.New("Failed wal insert")
	}
	recordToAdd := record.NewRecord(key, value, false)
	e.AddRecordToMemtable(*recordToAdd)
	e.cache.Set(key, *recordToAdd)
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
			previous_index = e.config.NumberOfMemtables
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
		if record != nil {
			return record
		}

		//going to next memtable
		i = i - 1
		if i < 0 {
			i = e.config.NumberOfMemtables
		}
	}

	//going through cache
	record, found := e.cache.Get(key)
	//we found it in cache
	if found {
		return record
	}

	//going through sstable
	record, _ = sstable.Search(key)
	//we found it in sstable
	if record != nil {
		return record
	}

	//we haven't found a record with the given key
	return nil
}

func (e *Engine) Delete(key string) {
	value := []byte("d")
	record := record.NewRecord(key, value, true)

	e.wal.AddRecord(key, value, true)
	e.AddRecordToMemtable(*record)

	e.cache.Set(key, *record)
}

func (e *Engine) recover() error {
	all_records, err := e.wal.LoadAllRecords()
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
			sstable.NewSSTable(all_records, 1)
			e.all_memtables[e.active_memtable_index] = *memtable.MemtableConstructor(e.config)
		}
	}
}
