package main

import (
	"fmt"
)

func main() {
	fmt.Println("Napredni Algoritmi i Strukture Podataka!")
	bloom := new(bloom.BloomFilter)
	// bloom.BloomFilterConstructor(69, 95.0)
	// bloom.AddElement("BajoJajo")
	// fmt.Println(bloom.CheckElement("JajoBajo"))
	// fmt.Println(bloom.CheckElement("BajoJajo"))
	// bloom.WriteToBinFile()
	bloom.LoadBloomFilter()
	fmt.Println(bloom.CheckElement("JajoBajo"))
	fmt.Println(bloom.CheckElement("BajoJajo"))
}
