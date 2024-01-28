package btree

import (
	"main/config"
	"main/record"
)

// used for every key value pair in a node
type KeyValuePair struct {
	key   string        //key
	value record.Record //value of the key
}

// used for every node in bTree
type BTreeNode struct {
	pairs    []KeyValuePair //array of pairs of keys and values in the node
	children []*BTreeNode   //array of pointers to the children of the node
	isLeaf   bool           //is the node a leaf
}

// btree
type BTree struct {
	root *BTreeNode //pointer to the beginning of a bTree, to the root node
	m    int        //max num or children m, max num of keys m-1,
	//every node except root should have at least m/2 children, m/2-1 keys
}

// for creating a new bTree node
func NewBTreeNode(isLeaf bool) *BTreeNode {
	return &BTreeNode{
		pairs:    make([]KeyValuePair, 0),
		children: make([]*BTreeNode, 0),
		isLeaf:   isLeaf,
	}
}

// for creating a new btree
func NewBTree() *BTree {
	cfg := new(config.Config)
	config.LoadConfig(cfg)
	return &BTree{
		root: NewBTreeNode(true), //root is leaf when we first create bTree
		m:    cfg.M,              //choosing the num of keys and pointers to the children in a node
	}
}

// search for a key in the B-tree
func (btree *BTree) SearchForInsertion(key string, value record.Record) bool {
	return btree.searchForInsertion(btree.root, key, value)
}

// recursive function to search for a key in a B-tree node
func (btree *BTree) searchForInsertion(node *BTreeNode, key string, value record.Record) bool {
	if node == nil {
		return false
	}

	// find the appropriate position to search for the key
	i := 0
	for i < len(node.pairs) && key > node.pairs[i].key {
		i++
	}

	//if the key is found, return true
	if i < len(node.pairs) && key == node.pairs[i].key {
		node.pairs[i].value = value
		return true
	}

	//if the node is a leaf, the key is not in the tree
	if node.isLeaf {
		return false
	}

	//recursively search in the appropriate child
	return btree.searchForInsertion(node.children[i], key, value)
}

// insert a key-value pair into the B-tree
func (btree *BTree) Insert(key string, value record.Record) {
	//start the insertion from the root if the key doesn't exist in the B-tree
	btree.insert(nil, btree.root, 0, key, value)
}

// recursive function to insert a key-value pair into a B-tree node
func (btree *BTree) insert(parent *BTreeNode, node *BTreeNode, parentIndex int, key string, value record.Record) {
	//find the appropriate position to insert the key-value pair
	i := 0
	for i < len(node.pairs) && key > node.pairs[i].key {
		i++
	}

	//if the node is a leaf, insert the key-value pair
	if node.isLeaf {
		//insert the key-value pair at the appropriate position
		node.pairs = append(node.pairs, KeyValuePair{})
		copy(node.pairs[i+1:], node.pairs[i:])
		node.pairs[i] = KeyValuePair{key, value}
	} else {
		//recursively insert into the appropriate child
		btree.insert(node, node.children[i], i, key, value)
	}

	//adjust the current node if it needs splitting
	if len(node.pairs) > btree.m-1 {
		btree.splitNode(parent, node, parentIndex)
	}
}

// split a B-tree node and insert the median key into the parent
func (btree *BTree) splitNode(parent, node *BTreeNode, index int) {
	//the median index and key-value pair
	medianIndex := len(node.pairs) / 2
	medianPair := node.pairs[medianIndex]

	//create a new node for the right half
	rightNode := NewBTreeNode(node.isLeaf)
	rightNode.pairs = append(rightNode.pairs, node.pairs[medianIndex+1:]...)
	node.pairs = node.pairs[:medianIndex]

	//if the node is not a leaf, adjust the children as well
	if !node.isLeaf {
		rightNode.children = append(rightNode.children, node.children[medianIndex+1:]...)
		node.children = node.children[:medianIndex+1]
	}

	//if the node is the root, create a new root
	if node == btree.root {
		newRoot := NewBTreeNode(false)
		newRoot.pairs = append(newRoot.pairs, medianPair)
		newRoot.children = append(newRoot.children, node, rightNode)
		btree.root = newRoot
	} else {
		//insert the median key-value pair into the parent
		parent.pairs = append(parent.pairs, KeyValuePair{})
		copy(parent.pairs[index+1:], parent.pairs[index:])
		parent.pairs[index] = medianPair

		//update the parent's children pointers
		parent.children = append(parent.children, nil)
		copy(parent.children[index+2:], parent.children[index+1:])
		parent.children[index+1] = rightNode
	}
}

// returns a sorted list of values from the B-tree
func (btree *BTree) ValuesInOrderTraversal() []record.Record {
	result := make([]record.Record, 0)
	btree.valuesInOrderTraversal(btree.root, &result)
	return result
}

// recursive function for in-order traversal of the B-tree to collect values
func (btree *BTree) valuesInOrderTraversal(node *BTreeNode, result *[]record.Record) {
	if node == nil {
		return
	}

	//traverse the left subtree
	for i := 0; i < len(node.children); i++ {
		btree.valuesInOrderTraversal(node.children[i], result)
		//if this is an internal node, append the value
		if i < len(node.pairs) {
			*result = append(*result, node.pairs[i].value)
		}
	}

	//if this is a leaf node, append the remaining values
	if node.isLeaf {
		for _, pair := range node.pairs {
			*result = append(*result, pair.value)
		}
	}
}
