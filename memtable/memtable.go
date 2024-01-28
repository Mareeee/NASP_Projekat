package memtable

import (
	"encoding/json"
	btree "main/bTree"
	"main/record"
	"main/skiplist"
	"os"
	"time"
)

type Memtable struct {
	skiplist    *skiplist.SkipList // promeniti da moze da bude i btree
	bTree       *btree.BTree
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
		mt.bTree = nil
	} else if mt.options.MemtableStructure == "btree" {
		mt.skiplist = nil
		mt.bTree = btree.NewBTree()
	}
}

func (mt *Memtable) Insert(record record.Record) {
	if mt.currentSize < mt.options.MaxSize {
		if mt.options.MemtableStructure == "skiplist" {
			_, found := mt.skiplist.Search(record.Key)
			if found {
				mt.Update(record.Key, record.Value)
			} else {
				mt.skiplist.Insert(record)
				mt.currentSize += 1
			}
		} else if mt.options.MemtableStructure == "btree" {
			//it updated the value if the key already existed
			record.Timestamp = time.Now().Unix()
			found := mt.bTree.SearchForInsertion(record.Key, record)
			if !found {
				mt.bTree.Insert(record.Key, record)
				mt.currentSize += 1
			}
		}
	} else {
		mt.Flush()
		mt.Insert(record)
	}
}

func (mt *Memtable) Update(key string, value []byte) {
	if mt.options.MemtableStructure == "skiplist" {
		node, found := mt.skiplist.Search(key)
		if found {
			node.Record.Timestamp = time.Now().Unix()
			node.Record.Value = value
			node.Record.ValueSize = int64(len(value))
		}
	}
}

func (mt *Memtable) Delete(record record.Record) {
	if mt.options.MemtableStructure == "skiplist" {
		node, found := mt.skiplist.Search(record.Key)
		if found {
			node.Record.Tombstone = true
			node.Record.Timestamp = time.Now().Unix()
		}
	}
	if mt.options.MemtableStructure == "btree" {
		record.Tombstone = true
		record.Timestamp = time.Now().Unix()
		mt.bTree.SearchForInsertion(record.Key, record)
	}
}

func (mt *Memtable) Flush() []record.Record {
	var elements []record.Record
	mt.currentSize = 0
	mt.skiplist = nil
	mt.bTree = nil
	if mt.options.MemtableStructure == "skiplist" {
		mt.skiplist = skiplist.NewSkipList()
		elements = mt.skiplist.GetRecords()
	} else if mt.options.MemtableStructure == "btree" {
		mt.bTree = btree.NewBTree()
		elements = mt.bTree.ValuesInOrderTraversal()
	}
	return elements
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
