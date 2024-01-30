package engine

import (
	"bufio"
	"errors"
	"fmt"
	"main/bloom-filter"
	"main/cache"
	"main/cms"
	"main/config"
	hll "main/hyperloglog"
	"main/lsm"
	"main/memtable"
	"main/record"
	"main/sstable"
	tokenbucket "main/tokenBucket"
	"main/wal"
	"os"
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

func (e *Engine) BloomFilterOptions() {
	fmt.Println("\n[1]	Create new instance")
	fmt.Println("[2]	Delete instance")
	fmt.Println("[3]	Add element")
	fmt.Println("[4]	Search element")

	optionScanner := bufio.NewScanner(os.Stdin)
	optionScanner.Scan()
	option := optionScanner.Text()

	if e.Tbucket.Take() {
		switch option {
		case "1":
			bloomFilter := bloom.NewBloomFilterMenu(100, 95.0)
			value := bloomFilter.ToBytes()
			key, _ := e.UserInput(false)
			e.Put(key, value)
		case "2":
			key, _ := e.UserInput(false)
			e.Delete(key)
		case "3":
			fmt.Println("Enter instance of bloomfilter: ")
			key_bf, _ := e.UserInput(false)
			bloom_record := e.Get(key_bf)
			if bloom_record != nil {
				fmt.Println("Enter element: ")
				key, _ := e.UserInput(false)
				bloomFilter := *bloom.FromBytes(bloom_record.Value)
				bloomFilter.AddElement(key)
				value := bloomFilter.ToBytes()
				e.Put(key, value)
			}
		case "4":
			fmt.Println("Enter instance of bloomfilter: ")
			key_bf, _ := e.UserInput(false)
			bloom_record := e.Get(key_bf)
			if bloom_record != nil {
				fmt.Println("Enter element: ")
				key, _ := e.UserInput(false)
				bloomFilter := *bloom.FromBytes(bloom_record.Value)
				fmt.Println(bloomFilter.CheckElement(key))
				value := bloomFilter.ToBytes()
				e.Put(key, value)
			}
		default:
			fmt.Println("Invalid option!")
		}
	} else {
		fmt.Println("Rate limit exceeded. Waiting...")
		time.Sleep(time.Second)
	}
}

func (e *Engine) UserInput(inputValueAlso bool) (string, []byte) {
	fmt.Print("Input key: ")
	keyScanner := bufio.NewScanner(os.Stdin)
	keyScanner.Scan()
	key := keyScanner.Text()
	if key == "" {
		fmt.Println("invalid key")
		return "", nil
	}
	if inputValueAlso {
		fmt.Print("Input value: ")
		valueScanner := bufio.NewScanner(os.Stdin)
		valueScanner.Scan()
		value := valueScanner.Text()
		return key, []byte(value)
	} else {
		return key, nil
	}
}

func (e *Engine) HLLMenu() {
	for {
		fmt.Println("[1]	Create new instance")
		fmt.Println("[2]	Delete already existing instance")
		fmt.Println("[3]	Adding a new element into an instance")
		fmt.Println("[4]	Provera kardiniliteta")
		fmt.Println("[X]	EXIT")
		optionScanner := bufio.NewScanner(os.Stdin)
		optionScanner.Scan()
		option := optionScanner.Text()
		if e.Tbucket.Take() {
			switch option {
			case "1":
				fmt.Println("Choose name for you HyperLogLog: ")
				//adding record with key which is name of hyperloglog and value is
				//hyperloglog in binary
				key, _ := e.UserInput(false)
				hloglog := hll.NewHyperLogLog(4)
				data := hloglog.ToBytes()
				e.Put("hll_"+key, data)
			case "2":
				fmt.Println("Choose the name of HyperLogLog you want to delete: ")
				key, _ := e.UserInput(false)
				e.Delete(key)
			case "3":
				fmt.Println("Choose the name of HyperLogLog you want to add element to: ")
				key, _ := e.UserInput(false)
				record := e.Get(key)
				//hyperloglog not found
				if record == nil {
					fmt.Println("hyperloglog doesnt exist")
					continue
				}
				data := record.Value
				hloglog := hll.LoadingHLL(data)
				//adding a key
				fmt.Println("Choose the key you want to add: ")
				key, _ = e.UserInput(false)
				hloglog.AddElement(key)
				e.Put(key, hloglog.ToBytes())
			case "4":
				fmt.Println("Choose the name of HyperLogLog you want to add element to: ")
				key, _ := e.UserInput(false)
				record := e.Get(key)
				//hyperloglog not found
				if record == nil {
					fmt.Println("hyperloglog doesnt exist")
					continue
				}
				data := record.Value
				hloglog := hll.LoadingHLL(data)
				estimation := hloglog.Estimate()
				fmt.Println("The estimation of unique element is: ", estimation)
			case "X":
				return
			default:
				fmt.Println("Invalid option!")
			}
		} else {
			fmt.Println("Rate limit exceeded. Waiting...")
			time.Sleep(time.Second)
		}
	}
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
