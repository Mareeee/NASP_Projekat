package skiplist

import (
	"main/record"
	"math/rand"
	"strings"
)

const (
	maxLevel = 16 // Adjust as needed based on your requirements
)

type Node struct {
	key     string
	value   interface{}
	forward []*Node
}

type SkipList struct {
	header *Node
	level  int
}

func newNode(key string, value interface{}, level int) *Node {
	return &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, level),
	}
}

func randomLevel() int {
	level := 1
	for rand.Intn(2) == 1 && level < maxLevel {
		level++
	}
	return level
}

func NewSkipList() *SkipList {
	return &SkipList{
		header: newNode("", nil, maxLevel),
		level:  1,
	}
}

func (sl *SkipList) Insert(record record.Record) {
	key := record.Key
	value := record

	update := make([]*Node, maxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && compareKeys(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	newLevel := randomLevel()

	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	newNode := newNode(key, value, newLevel)

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

// compareKeys function for comparing two keys
func compareKeys(key1, key2 string) int {
	// Compare keys as strings
	return strings.Compare(key1, key2)
}
