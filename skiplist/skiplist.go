package skiplist

import (
	"errors"
	"main/config"
	"main/record"
	"math/rand"
)

type Node struct {
	Record record.Record
	next   []*Node
}

func newNode(record record.Record, level int) *Node {
	return &Node{
		Record: record,
		next:   make([]*Node, level+1),
	}
}

type SkipList struct {
	config config.Config
	head   *Node
	level  int // trenutni broj nivoa
}

func NewSkipList() *SkipList {
	sl := new(SkipList)
	config.LoadConfig(&sl.config)
	record := new(record.Record)
	sl.head = newNode(*record, sl.config.MaxHeight)
	sl.level = 1

	return sl
}

func (sl *SkipList) Search(key string) (*Node, bool) {
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].Record.Key < key {
			current = current.next[i]
		}
	}

	return current.next[0], current.next[0] != nil && current.next[0].Record.Key == key
}

func (sl *SkipList) Insert(record record.Record) {
	level := sl.roll()
	new := newNode(record, level)
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].Record.Key < record.Key {
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
		for current.next[i] != nil && current.next[i].Record.Key < key {
			current = current.next[i]
		}
		current.next[i] = nodeToDel.next[i]
	}
	return true, nil
}

func (sl *SkipList) roll() int {
	level := 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= sl.config.MaxHeight {
			return level
		}
	}
	return level
}

func (sl *SkipList) GetRecords() []record.Record {
	var elements []record.Record

	current := sl.head.next[0]

	for current != nil {
		elements = append(elements, current.Record)
		current = current.next[0]
	}

	return elements
}
