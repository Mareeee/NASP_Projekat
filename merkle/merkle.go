package merkle

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type MerkleTree struct {
	Root *Node
}

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func NewMerkleTree(data [][]byte) *MerkleTree {
	var nodes []*Node
	for _, d := range data {
		node := &Node{data: Hash(d)}
		nodes = append(nodes, node)
	}
	for len(nodes) > 1 {
		var newLevel []*Node
		for i := 0; i < len(nodes); i += 2 {
			left := nodes[i]
			right := left
			if i+1 < len(nodes) {
				right = nodes[i+1]
			}
			parent := &Node{
				data:  Hash(append(left.data[:], right.data[:]...)),
				left:  left,
				right: right,
			}
			newLevel = append(newLevel, parent)
		}
		nodes = newLevel
	}
	Root := &MerkleTree{Root: nodes[0]}
	return Root
}

func SerializeMerkleTree(node *Node) string {
	if node == nil {
		return ""
	}

	serialized := node.String()

	leftSerialized := SerializeMerkleTree(node.left)
	rightSerialized := SerializeMerkleTree(node.right)

	return fmt.Sprintf("%s|%s|%s", serialized, leftSerialized, rightSerialized)
}

func DeserializeMerkleTree(serialized string) *Node {
	re := regexp.MustCompile(`\|+`)
	trimmed := strings.TrimRight(re.ReplaceAllString(serialized, "|"), "|")
	return RecursivelyDeserializeMerkleTree(trimmed)
}

func RecursivelyDeserializeMerkleTree(serialized string) *Node {
	parts := strings.Split(serialized, "|")

	// Rekonstruisemo korenski cvor
	rootData, _ := hex.DecodeString(parts[0]) // <-- prebacuje u hex
	rootNode := &Node{data: [20]byte{}}       // <-- pravi node objekat
	copy(rootNode.data[:], rootData)          // <-- kopira se u podatke rootNode-a

	// Brisemo prvi element, jer je on root za taj deo stabla
	parts = parts[1:]

	// Trazimo sredinu liste, da bi mogli lakse da vrsimo rekonstrukciju
	midpoint := len(parts) / 2

	// Rekurzivno rekonstruisemo cvorove, tako sto su svi levi do prve polovine liste, a desni nakon polovine liste
	if len(parts) > 0 {
		leftSubtree := DeserializeMerkleTree(strings.Join(parts[:midpoint], "|"))
		rootNode.left = leftSubtree
	}

	if len(parts) > midpoint {
		rightSubtree := DeserializeMerkleTree(strings.Join(parts[midpoint:], "|"))
		rootNode.right = rightSubtree
	}

	return rootNode
}

func CompareMerkleTrees(node1 *Node, node2 *Node) bool {
	if node1 == nil && node2 == nil {
		return true
	}

	if node1 == nil || node2 == nil {
		return false
	}

	if node1.String() != node2.String() {
		return false
	}

	// Rekurzivno poredi cvorove
	leftEqual := CompareMerkleTrees(node1.left, node2.left)
	rightEqual := CompareMerkleTrees(node1.right, node2.right)

	// Rekurzivno vracanje True False u zavisnosti da li su podstabla jednaka
	return leftEqual && rightEqual
}

func PrintTree(node *Node, indent string) {

	if node == nil {
		return
	}

	// Ispisi čvor na trenutnom nivou
	fmt.Println(indent + node.String())

	// Rekurzivno ispisi levo i desno podstablo
	PrintTree(node.left, indent+"  ")
	PrintTree(node.right, indent+"  ")
}

func (mt *MerkleTree) WriteToBinFile(filePath string) {
	serializedTree := SerializeMerkleTree(mt.Root)
	os.WriteFile(filePath, []byte(serializedTree), 0644)
}

func ReadFromBinFile(filePath string) string {
	serialized_merkle_tree, _ := os.ReadFile(filePath)
	return string(serialized_merkle_tree)
}
