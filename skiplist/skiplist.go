package skiplist

import (
	"fmt"
	"math/rand"
)

type Node struct {
	value int
	next  []*Node
}

func (node *Node) NodeConstructor(value, level int) {
	node.value = value
	node.next = make([]*Node, level)
}

type SkipList struct {
	maxHeight int // broj nivoa skip liste
	head      *Node
}

func (sl *SkipList) SkipListConstructor(maxHeight int) {
	sl.maxHeight = maxHeight
	sl.head = new(Node)
	sl.head.NodeConstructor(0, maxHeight)
}

func (sl *SkipList) Insert(value int) {
	newNode := new(Node)
	newNode.NodeConstructor(value, sl.roll())
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

func main() {
	s := SkipList{maxHeight: 3}
	for i := 0; i < 10; i++ {
		fmt.Println(s.roll())
	}
}
