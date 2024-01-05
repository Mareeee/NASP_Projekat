package main

import (
	"fmt"
	"main/merkle"
)

func main() {
	data := [][]byte{[]byte("mare"), []byte("dare"), []byte("care")}
	merkleRoot := merkle.NewMerkleTree(data)

	serijalizovano := merkle.SerializeMerkleTree(merkleRoot.Root)
	deserialized := merkle.DeserializeMerkleTree(serijalizovano)

	fmt.Println("\n[Original]")
	merkle.PrintTree(merkleRoot.Root, "---")

	fmt.Println("\n[Deserijalizovano] ")
	merkle.PrintTree(deserialized, "  ")

	// merkleRoot.WriteToBinFile("Merkle1")
	// fmt.Println(merkle.ReadFromBinFile("data/merkle/Merkle1.bin"))

	fmt.Println("\nDa li su isti??", merkle.CompareMerkleTrees(deserialized, merkleRoot.Root))
}
