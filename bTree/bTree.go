package btree

//used for every key value pair in a node
type KeyValuePair struct {
	key   int    //key
	value []byte //value of the key
}

//used for every node in bTree
type BTreeNode struct {
	pairs    []KeyValuePair //array of pairs of keys and values in the node
	children []*BTreeNode   //array of pointers to the children of the node
	isLeaf   bool           //is the node a leaf
}

//btree
type BTree struct {
	root *BTreeNode //pointer to the beginning of a bTree, to the root node
	m    int        //max num or children m, max num of keys m-1,
	//every node except root should have at least m/2 children, m/2-1 keys
}

//for creating a new bTree node
func NewBTreeNode(isLeaf bool) *BTreeNode {
	return &BTreeNode{
		pairs:    make([]KeyValuePair, 0),
		children: make([]*BTreeNode, 0),
		isLeaf:   isLeaf,
	}
}

//for creating a new btree
func NewBTree(m int) *BTree {
	return &BTree{
		root: NewBTreeNode(true), //root is leaf when we first create bTree
		m:    m,                  //choosing the num of keys and pointers to the children in a node
	}
}

//searching the bTree for the key starting from the root node
func (bTree *BTree) Search(key int) (bool, int) {
	return bTree.searchFromNode(bTree.root, key)
}

//searching the bTree for the key starting from the specific node
func (bTree *BTree) searchFromNode(node *BTreeNode, key int) (bool, int) {
	i := 0
	//search the given node for the key till we don't find a key
	//that is same or bigger than our key, or till we don;t come to the end of the node
	for i < len(node.pairs) && key > node.pairs[i].key {
		i++
	}

	//if the key is found
	if i < len(node.pairs) && key == node.pairs[i].key {
		return true, i
	} else if node.isLeaf {
		//if the node we just searched is leaf
		//the key doesn't exist in the bTree
		return false, -1
	} else {
		//if we didn't find the key but we didn't come to the end of
		//the bTree, we go deeper into the bTree
		return bTree.searchFromNode(node.children[i], key)
	}
}

//inserting a new key value pair into the root if the
//key doesn't already exist in the bTree
func (bTree *BTree) Insert(keyValuePair KeyValuePair) {
	//we only insert new key value pair if the key isn't already
	//in the bTree
	if found, _ := bTree.Search(keyValuePair.key); found {
		//if the root has max number of keys in it
		//we need to split it, and leave the median key as a root
		//and split other keys in half and make them the median's key children
		if len(bTree.root.pairs) == bTree.m-1 {
			newRoot := &BTreeNode{isLeaf: false}
			bTree.root = newRoot
			newRoot.children = append(newRoot.children, bTree.root)
			bTree.splitChild(newRoot, 0)
			bTree.insertNonFull(newRoot, keyValuePair)
		}
		bTree.insertNonFull(bTree.root, keyValuePair)
	}
}

//splitting nodes in half and sending the median key up to parent
func (bTree *BTree) splitChild(parent *BTreeNode, index int) {
	child := parent.children[index]              //the child node of the parent node which we want to split
	newChild := &BTreeNode{isLeaf: child.isLeaf} //new right child of the median key

	//adding the median key value pair to the parent node
	parent.pairs = append(parent.pairs[:index], KeyValuePair{})
	copy(parent.pairs[index+1:], parent.pairs[index:])
	parent.pairs[index] = child.pairs[bTree.m/2-1]

	//adding the right child node to the parent node
	//it will be to the right from where we added the key
	parent.children = append(parent.children[:index+1], nil)
	copy(parent.children[index+2:], parent.children[index+1:])
	parent.children[index+1] = newChild

	//move the right part of the child node to the right child
	newChild.pairs = append(newChild.pairs, child.pairs[bTree.m/2:]...)
	child.pairs = child.pairs[:bTree.m/2-1]

	//if the child which has been split isn't a leaf we need to
	//split their children
	if !child.isLeaf {
		//taking the right children
		newChild.children = append(newChild.children, child.children[bTree.m/2:]...)
		//taking the left children
		child.children = child.children[:bTree.m/2]
	}
}

//finding a place where to insert the key value pair
func (bTree *BTree) insertNonFull(node *BTreeNode, keyValuePair KeyValuePair) {
	i := len(node.pairs) - 1 //helps us find the position where we need to insert the pair

	//if the node is a leaf, insert the key value pair at the position we found
	if node.isLeaf {
		//adding space for one more key value pair
		node.pairs = append(node.pairs, KeyValuePair{})
		//inserting at the correct position
		for i >= 0 && keyValuePair.key < node.pairs[i].key {
			node.pairs[i+1] = node.pairs[i]
			i--
		}
		node.pairs[i+1] = keyValuePair
	} else {
		//find the position of the child node we want to go in
		for i >= 0 && keyValuePair.key < node.pairs[i].key {
			i--
		}
		i++

		//if the child is full we split it
		if len(node.children[i].pairs) == bTree.m-1 {
			bTree.splitChild(node, i)
			//if the key we want to insert is larger than
			//the key that is newly added to the node after splitting
			//we need to move to the next child node that
			//points to keys that are bigger than the key which was newly added
			if keyValuePair.key > node.pairs[i].key {
				i++
			}
		}

		//we go recursively till we don't insert the pair into a leaf node
		bTree.insertNonFull(node.children[i], keyValuePair)
	}
}

//delete a key from the bTree
func (bTree *BTree) Delete(key int) {
	//we only delete a key value pair if the key is in the bTree
	if found, _ := bTree.Search(key); found {
		//if the bTree has no pairs
		if len(bTree.root.pairs) == 0 {
			return
		}
		//calling deletion function which goes through every node
		//in key's path and we start with the root node
		bTree.deleteFromNode(bTree.root, key)
		//if there is no pairs in root and root isn't a leaf node it means
		//that the last root pair merged with it's children,
		//so we make that node the new root
		if len(bTree.root.pairs) == 0 && !bTree.root.isLeaf {
			bTree.root = bTree.root.children[0]
		}
	}
}

//delete a key from specific node
func (bTree *BTree) deleteFromNode(node *BTreeNode, key int) {
	//looking if the key is in this node
	index := 0
	found := false
	for index < len(node.pairs) && key > node.pairs[index].key {
		index++
	}
	//if the key is found
	if index < len(node.pairs) && key == node.pairs[index].key {
		found = true
	}

	if found {
		//case 1: key is in the node and node is a leaf
		if node.isLeaf {
			bTree.deleteKeyFromLeaf(node, index)
		} else {
			//case 2: key is in an internal node
			bTree.deleteKeyFromInternalNode(node, index)
		}
	} else {
		//key isn't found in the node
		if node.isLeaf {
			//key isn't found in the bTree
			return
		}
		//if the child of the current node in which the key is possibly in
		//has less than the minimal number of keys, we need to fix that child
		isLastChild := index == len(node.pairs)
		//recursively deleting
		bTree.deleteFromNode(node.children[index], key)
		//we fix the children recursively
		//if nodes in which deletion went through
		//have less than the minimum number of keys
		if len(node.children[index].pairs) < bTree.m/2-1 {
			bTree.fixChild(node, index, isLastChild)
		}
	}
}

//delete a key from a leaf node
func (bTree *BTree) deleteKeyFromLeaf(node *BTreeNode, index int) {
	//case 1: deleting a key from a leaf node
	copy(node.pairs[index:], node.pairs[index+1:])
	node.pairs = node.pairs[:len(node.pairs)-1]
}

//delete a key from an internal node
func (bTree *BTree) deleteKeyFromInternalNode(node *BTreeNode, index int) {
	//all of the cases for case 2
	//case 2.1: left child has more than the minimum number of keys
	//so we find the predecessor key in the left sub tree
	//put it where the key was which we wanted to delete
	//and recursively delete predecessor from the old node
	if len(node.children[index].pairs) >= bTree.m/2 {
		//getting the predecessor and putting it into the parent node
		predecessor := bTree.getPredecessor(node.children[index])
		node.pairs[index] = predecessor
		//deleting the predecessor from the old child node
		bTree.deleteFromNode(node.children[index], predecessor.key)
	} else if len(node.children[index+1].pairs) >= bTree.m/2 {
		//case 2.2: right child has more than the minumum number of keys
		//so we find the successor key in the right sub tree
		//put it where the key was which we wanted to delete
		//and recursively delete successor from the old node
		//getting the successor and putting it into the parent node
		successor := bTree.getSuccessor(node.children[index+1])
		node.pairs[index] = successor
		//deleting the successor from the old child node
		bTree.deleteFromNode(node.children[index+1], successor.key)
	} else {
		//case 2.3: if neither of two children have more than minimum number
		//of keys, we merge the two child nodes and the pair we want to delete
		//from the parent node into one child node
		//we recursively delete the pair that was previously in the parent node
		keyToDelete := node.pairs[index].key
		bTree.mergeChildren(node, index)
		bTree.deleteFromNode(node.children[index], keyToDelete)
	}
}

//finding the rightmost pair in the left subtree of a given node
func (bTree *BTree) getPredecessor(node *BTreeNode) KeyValuePair {
	//moving to the rightmost node until we reach a leaf
	for !node.isLeaf {
		node = node.children[len(node.children)-1]
	}
	//the rightmost pair in the rightmost leaf node is the predecessor
	return node.pairs[len(node.pairs)-1]
}

//finding the leftmost pair in the right subtree of a given node
func (bTree *BTree) getSuccessor(node *BTreeNode) KeyValuePair {
	//moving to the leftmost node until we reach a leaf
	for !node.isLeaf {
		node = node.children[0]
	}
	//the leftmost pair in the leftmost leaf node is the successor
	return node.pairs[0]
}

//merging the left and right children of a pair into a single node
func (bTree *BTree) mergeChildren(node *BTreeNode, index int) {
	//left and right children of the pair in the parent node
	child := node.children[index]
	sibling := node.children[index+1]

	//move the parent pair into the child node
	child.pairs = append(child.pairs, node.pairs[index])
	//move the (right)sibling node into the the left sibling(child)
	child.pairs = append(child.pairs, sibling.pairs...)

	//if the children aren't leaves, connect their children
	if !child.isLeaf {
		child.children = append(child.children, sibling.children...)
	}

	//remove the key and the sibling from the parent node
	copy(node.pairs[index:], node.pairs[index+1:])
	node.pairs = node.pairs[:len(node.pairs)-1]
	copy(node.children[index+1:], node.children[index+2:])
	node.children = node.children[:len(node.children)-1]
}

//fixing the child at index if it has fewer than the minimum number of keys
func (bTree *BTree) fixChild(node *BTreeNode, index int, isLastChild bool) {
	//if the previous child has more than the minimum number of keys
	//we take the rightmost key from it, put it in the parent node
	//and the key from the parent node goes down as the first pair
	//of the child node
	if index > 0 && len(node.children[index-1].pairs) >= bTree.m/2 {
		bTree.rotateRight(node, index)
	} else if !isLastChild && len(node.children[index+1].pairs) >= bTree.m/2 {
		//if the next child has more than the minimum number of keys
		//we take the leftmost key from it, put it in the parent node
		//and the key from the parent node goes down as the last pair
		//of the child node
		bTree.rotateLeft(node, index)
	} else {
		//if the child isn't the last child merge it with it's next sibling
		//and if it is the last child merge it with it's previous sibling
		if index != len(node.pairs) {
			bTree.mergeChildren(node, index)
		} else {
			bTree.mergeChildren(node, index-1)
		}
	}
}

//rotating keys from left to right
func (bTree *BTree) rotateRight(node *BTreeNode, index int) {
	//right and left children of the pair in the parent node
	child := node.children[index]
	sibling := node.children[index-1]

	//moving the pair from parent to the child
	child.pairs = append([]KeyValuePair{node.pairs[index-1]}, child.pairs...)

	//moving the last key from the left child(sibling) to the parent node
	node.pairs[index-1] = sibling.pairs[len(sibling.pairs)-1]
	sibling.pairs = sibling.pairs[:len(sibling.pairs)-1]

	//if the children aren't leaves, move the last child from the sibling to the child
	if !child.isLeaf {
		child.children = append([]*BTreeNode{sibling.children[len(sibling.children)-1]}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}
}

//rotating keys from right to left
func (bTree *BTree) rotateLeft(node *BTreeNode, index int) {
	//left and right children of the pair in the parent node
	child := node.children[index]
	sibling := node.children[index+1]

	//moving the pair from parent to the child
	child.pairs = append(child.pairs, node.pairs[index])

	//moving the first key from the right child(sibling) to the parent node
	node.pairs[index] = sibling.pairs[0]
	sibling.pairs = sibling.pairs[1:]

	//if the children aren't leaves, move the first child from the sibling to the child
	if !child.isLeaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}
}
