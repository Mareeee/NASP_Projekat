package main

import (
	"fmt"
	"main/cms"
)

func main() {
	fmt.Println("Napredni Algoritmi i Strukture Podataka!")

	count := new(cms.CountMinSketch)
	// count.CountMinSketchConstructor(0.1, 0.9)

	// count.AddElement("Zoki")
	// count.AddElement("Zoki")

	// count.WriteToBinFile()
	count.LoadCMS()

	fmt.Printf("Number of elements: %d", count.NumberOfRepetitions("Zoki"))
}
