package main

import (
	"fmt"
	"main/simhash"
)

func main() {
	fmt.Println("Napredni Algoritmi i Strukture Podataka!")
	words := simhash.RemoveStopWords("Like all forests, the wooded stretches of the Arctic sometimes catch on fire.")
	fmt.Println(words)
}
