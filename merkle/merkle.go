package merkle

import (
	"crypto/sha256"
	"fmt"
)

type MerkleNode struct {
	Hash  string
	Left  *MerkleNode
	Right *MerkleNode
}

type MerkleTree struct {
	Root *MerkleNode
}

func (mt *MerkleTree) MerkleTreeConstructor(data []string) {
	mt.Root = buildMerkleTree(data)
}

func buildMerkleTree(data []string) *MerkleNode {
	var nodes []*MerkleNode
	for _, d := range data {
		node := &MerkleNode{Hash: calculateHash(d)}
		nodes = append(nodes, node)
	}
	for len(nodes) > 1 {
		var newLevel []*MerkleNode
		for i := 0; i < len(nodes); i += 2 {
			left := nodes[i]
			right := left
			if i+1 < len(nodes) {
				right = nodes[i+1]
			}
			parent := &MerkleNode{Hash: calculateHash(left.Hash + right.Hash), Left: left, Right: right}
			newLevel = append(newLevel, parent)
		}
		nodes = newLevel
	}
	return nodes[0]
}

//func (n *MerkleNode) String() string {
//	return hex.EncodeToString(n.data[:])
//}

func calculateHash(data string) string {
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)
}

func PrintTree(node *MerkleNode, indent string) {
	if node != nil {
		fmt.Println(indent+"Hash:", node.Hash)
		if node.Left != nil {
			PrintTree(node.Left, indent+"  ")
		}
		if node.Right != nil {
			PrintTree(node.Right, indent+"  ")
		}
	}
}
