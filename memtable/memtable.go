package memtable

import (
	"encoding/json"
	"main/record"
	"main/skiplist"
	"os"
)

type Memtable struct {
	skiplist    *skiplist.SkipList // promeniti da moze da bude i btree
	currentSize int
	options     MemtableOptions
}

type MemtableOptions struct {
	MaxSize           int    `json:"MaxSize"` // u prezentaciji oko 4MB
	MemtableStructure string `json:"MemtableStructure"`
}

func (mt *Memtable) MemtableConstructor() {
	mt.currentSize = 0
	mt.options.LoadJson()
	if mt.options.MemtableStructure == "skiplist" {
		mt.skiplist = skiplist.NewSkipList()
	} else {
		mt.skiplist = nil
	}
}

func (mt *Memtable) Insert(record record.Record) {
	if mt.currentSize < mt.options.MaxSize {
		_, found := mt.skiplist.Search(record.Key)
		if found {
			mt.Update(record)
		} else {
			mt.skiplist.Insert(record)
		}
		mt.currentSize += 1
	} else {
		mt.Flush()
	}
}

func (mt *Memtable) Update(record record.Record) {
	node, found := mt.skiplist.Search(record.Key)
	if found == true {
		node.Record.Tombstone = false
	}
}

func (mt *Memtable) Delete(record record.Record) {
	node, found := mt.skiplist.Search(record.Key)
	if found == true {
		node.Record.Tombstone = false
	}
}

func (mt *Memtable) Flush() {
	mt.currentSize = 0
	mt.skiplist = nil
	mt.skiplist = skiplist.NewSkipList()
}

/* Ucitava MemtableOptions iz config JSON fajla */
func (mto *MemtableOptions) LoadJson() {
	jsonData, _ := os.ReadFile(MEMTABLE_CONFIG_FILE_PATH)

	json.Unmarshal(jsonData, &mto)
}

/* Upisuje MemtableOptions u config JSON fajl */
func (mto *MemtableOptions) WriteJson() {
	jsonData, _ := json.MarshalIndent(mto, "", "  ")

	os.WriteFile(MEMTABLE_CONFIG_FILE_PATH, jsonData, 0644)
}
