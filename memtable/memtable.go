package memtable

import (
	"fmt"
)

type Memtable struct {
	records []Record
}

func (mt *Memtable) MemtableConstructor() {
	mt.records = make([]Record, 128)
}

func (mt *Memtable) loadMemtable() {

}

func (mt *Memtable) ReadMemtable() {
	for i := 0; i < len(mt.records); i++ {
		fmt.Println(mt.records[i])
	}
}

func (mt *Memtable) UpdateMemtable() {

}

func (mt *Memtable) DeleteMemtable() {

}

func (mt *Memtable) Flush() { // Flush to SSTable

}

func (mt *Memtable) Sort() {

}
