package skiplist

import (
	"errors"
	"math/rand"
)

type Node struct {
	value int
	next  []*Node
}

func (node Node) GetValue() int {
	return node.value
}

func (node *Node) newNode(value, level int) {
	node.value = value
	node.next = make([]*Node, level+1) // next u i-tom redu
}

type SkipList struct {
	maxHeight int // ukupno nivoa skip liste
	head      *Node
	level     int // trenutni broj nivoa
}

func (sl *SkipList) NewSkipList(maxHeight int) {
	sl.maxHeight = maxHeight
	sl.head = new(Node)
	sl.head.newNode(0, maxHeight)
	sl.level = 1
}

func (sl *SkipList) Search(value int) (*Node, bool) {
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
	}

	return current.next[0], current.next[0] != nil && current.next[0].value == value
}

func (sl *SkipList) Insert(value int) {
	new := new(Node)
	level := sl.roll()
	new.newNode(value, level)
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
		new.next[i] = current.next[i]
		current.next[i] = new
	}
}

func (sl *SkipList) Delete(value int) (bool, error) {
	nodeToDel, found := sl.Search(value)
	if !found {
		return false, errors.New("Node doesn't exist")
	}
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
		current.next[i] = nodeToDel.next[i]
	}
	return true, nil
}

func (s *SkipList) roll() int {
	level := 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}
