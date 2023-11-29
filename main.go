package main

import (
	"fmt"
	"main/merkle"
)

func main() {
	data := []string{"A", "B", "C", "D"}
	mt := new(merkle.MerkleTree)
	mt.MerkleTreeConstructor(data)
	merkle.PrintTree(mt.Root, "")
	fmt.Println("Root Hash:", mt.Root.Hash)
}
