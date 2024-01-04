package skiplist

import (
	"encoding/json"
	"errors"
	"main/record"
	"math/rand"
	"os"
)

type Node struct {
	record record.Record
	next   []*Node
}

func newNode(record record.Record, level int) *Node {
	return &Node{
		record: record,
		next:   make([]*Node, level+1),
	}
}

type SkipListOptions struct {
	MaxHeight int `json:"MaxHeight"`
}

type SkipList struct {
	options SkipListOptions
	head    *Node
	level   int // trenutni broj nivoa
}

func NewSkipList() *SkipList {
	sl := new(SkipList)
	sl.options.LoadJson()
	record := new(record.Record)
	sl.head = newNode(*record, sl.options.MaxHeight)
	sl.level = 1

	return sl
}

func (sl *SkipList) Search(key string) (*Node, bool) {
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].record.Key < key {
			current = current.next[i]
		}
	}

	return current.next[0], current.next[0] != nil && current.next[0].record.Key == key
}

func (sl *SkipList) Insert(record record.Record) {
	level := sl.roll()
	new := newNode(record, level)
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].record.Key < record.Key {
			current = current.next[i]
		}
		new.next[i] = current.next[i]
		current.next[i] = new
	}
}

func (sl *SkipList) Delete(key string) (bool, error) {
	nodeToDel, found := sl.Search(key)
	if !found {
		return false, errors.New("Node doesn't exist")
	}
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].record.Key < key {
			current = current.next[i]
		}
		current.next[i] = nodeToDel.next[i]
	}
	return true, nil
}

func (sl *SkipList) roll() int {
	level := 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= sl.options.MaxHeight {
			return level
		}
	}
	return level
}

/* Ucitava WalOptions iz config JSON fajla */
func (o *SkipListOptions) LoadJson() {
	jsonData, _ := os.ReadFile(SKIPLIST_CONFIG_FILE_PATH)

	json.Unmarshal(jsonData, &o)
}

/* Upisuje WalOptions u config JSON fajl */
func (o *SkipListOptions) WriteJson() {
	jsonData, _ := json.MarshalIndent(o, "", "  ")

	os.WriteFile(SKIPLIST_CONFIG_FILE_PATH, jsonData, 0644)
}
