package main

import (
	"fmt"
	hll "main/hyperloglog"
)

func main() {
	fmt.Println("Napredni Algoritmi i Strukture Podataka!")
	hyll := new(hll.HLL)
	hyll.HyperLogLogConstructor(10)
	hyll.AddElement("Boban")
	hyll.AddElement("Coban")
	fmt.Println(hyll.Estimate())
}
