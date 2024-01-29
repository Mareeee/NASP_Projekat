package memtable

import (
	"fmt"
	btree "main/bTree"
	"main/config"
	"main/record"
	"main/skiplist"
	"time"
)

type Memtable struct {
	skiplist    *skiplist.SkipList
	bTree       *btree.BTree
	CurrentSize int
	config      config.Config
}

func MemtableConstructor(config config.Config) *Memtable {
	mt := new(Memtable)
	mt.CurrentSize = 0
	mt.config = config
	if mt.config.MemtableStructure == "skiplist" {
		mt.skiplist = skiplist.NewSkipList()
		mt.bTree = nil
	} else if mt.config.MemtableStructure == "btree" {
		mt.skiplist = nil
		mt.bTree = btree.NewBTree()
	}
	return mt
}

func LoadAllMemtables(config config.Config) *[]Memtable {
	var allMemtables []Memtable
	for i := 0; i < config.NumberOfMemtables; i++ {
		allMemtables = append(allMemtables, *MemtableConstructor(config))
	}
	return &allMemtables
}

func (mt *Memtable) Search(key string) *record.Record {
	var record *record.Record
	if mt.config.MemtableStructure == "skiplist" {
		node, _ := mt.skiplist.Search(key)
		if node != nil {
			record = &node.Record
		} else {
			record = nil
		}
	} else if mt.config.MemtableStructure == "btree" {
		record = mt.bTree.SearchForValue(key)
	}
	return record
}

func (mt *Memtable) Insert(record record.Record) bool {
	if mt.CurrentSize < mt.config.MaxSize {
		if mt.config.MemtableStructure == "skiplist" {
			_, found := mt.skiplist.Search(record.Key)
			if found {
				mt.Update(record.Key, record.Value)
			} else {
				mt.skiplist.Insert(record)
				mt.CurrentSize += 1
			}
		} else if mt.config.MemtableStructure == "btree" {
			//it updated the value if the key already existed
			found := mt.bTree.SearchForInsertion(record.Key, record)
			if !found {
				mt.bTree.Insert(record.Key, record)
				mt.CurrentSize += 1
			}
		}
	} else {
		return false
	}
	return true
}

func (mt *Memtable) Update(key string, value []byte) {
	if mt.config.MemtableStructure == "skiplist" {
		node, found := mt.skiplist.Search(key)
		if found {
			node.Record.Timestamp = time.Now().Unix()
			node.Record.Value = value
			node.Record.ValueSize = int64(len(value))
		}
	}
}

func (mt *Memtable) Delete(record record.Record) {
	if mt.config.MemtableStructure == "skiplist" {
		node, found := mt.skiplist.Search(record.Key)
		if found {
			node.Record.Tombstone = true
			node.Record.Timestamp = time.Now().Unix()
		}
	}
	if mt.config.MemtableStructure == "btree" {
		record.Tombstone = true
		record.Timestamp = time.Now().Unix()
		mt.bTree.SearchForInsertion(record.Key, record)
	}
}

func (mt *Memtable) PrintMemtableRecords() {
	allRecords := mt.skiplist.GetRecords()
	for i := 0; i < len(allRecords); i++ {
		fmt.Println(allRecords[i])
	}
}

func (mt *Memtable) Flush() []record.Record {
	var elements []record.Record
	mt.CurrentSize = 0
	mt.skiplist = nil
	mt.bTree = nil
	if mt.config.MemtableStructure == "skiplist" {
		mt.skiplist = skiplist.NewSkipList()
		elements = mt.skiplist.GetRecords()
	} else if mt.config.MemtableStructure == "btree" {
		mt.bTree = btree.NewBTree()
		elements = mt.bTree.ValuesInOrderTraversal()
	}
	return elements
}
