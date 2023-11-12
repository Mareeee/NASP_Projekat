package main

import (
	"fmt"
	"main/simhash"
)

func main() {
	// ovde sam proveravao da li su hesevi iste velicine, nisu, to moze praviti problem
	fmt.Println(simhash.GetHashAsString([]byte("hello world!")))
	fmt.Println(simhash.GetHashAsString([]byte("dobar")))
	fmt.Println(simhash.GetHashAsString([]byte("napredni")))
}
