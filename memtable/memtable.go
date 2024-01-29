package memtable

import (
	btree "main/bTree"
	"main/config"
	"main/record"
	"main/skiplist"
	"time"
)

type Memtable struct {
	skiplist    *skiplist.SkipList
	bTree       *btree.BTree
	currentSize int
	config      config.Config
}

func (mt *Memtable) MemtableConstructor() {
	mt.currentSize = 0
	config.LoadConfig(&mt.config)
	if mt.config.MemtableStructure == "skiplist" {
		mt.skiplist = skiplist.NewSkipList()
		mt.bTree = nil
	} else if mt.config.MemtableStructure == "btree" {
		mt.skiplist = nil
		mt.bTree = btree.NewBTree()
	}
}

func (mt *Memtable) Insert(record record.Record) {
	if mt.currentSize < mt.config.MaxSize {
		if mt.config.MemtableStructure == "skiplist" {
			_, found := mt.skiplist.Search(record.Key)
			if found {
				mt.Update(record.Key, record.Value)
			} else {
				mt.skiplist.Insert(record)
				mt.currentSize += 1
			}
		} else if mt.config.MemtableStructure == "btree" {
			//it updated the value if the key already existed
			found := mt.bTree.SearchForInsertion(record.Key, record)
			if !found {
				mt.bTree.Insert(record.Key, record)
				mt.currentSize += 1
			}
		}
	}
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

func (mt *Memtable) Flush() []record.Record {
	var elements []record.Record
	mt.currentSize = 0
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
