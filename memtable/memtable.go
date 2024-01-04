package memtable

import (
	"encoding/json"
	"fmt"
	"main/record"
	"main/skiplist"
	"os"
)

type Memtable struct {
	MemtableSize      uint64 `json:"memtable_size"`
	MemtableStructure string `json:"memtable_structure"`
	SkipList          skiplist.SkipList
	records           []record.Record // obrisi kasnije
}

func (mt *Memtable) MemtableConstructor() {
	mt.records = make([]record.Record, 10)
}

func (mt *Memtable) LoadMemtable() {
	data, _ := os.ReadFile("memtable/data/memtable.json")
	json.Unmarshal(data, &mt) // upisuje podatke iz json-a u memtable
}

func (mt *Memtable) ReadMemtable() {
	for i := 0; i < len(mt.records); i++ {
		fmt.Println(mt.records[i])
	}
}

func (mt *Memtable) AddRecord(record record.Record) {
	mt.SkipList.Insert(record)
}

func (mt *Memtable) DeleteMemtable() {

}

func (mt *Memtable) Flush() { // Flush to SSTable

}

func (mt *Memtable) Sort() {

}
