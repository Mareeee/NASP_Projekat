package main

import (
	"fmt"
	"main/memtable"
	"main/record"
)

func main() {
	fmt.Println("Napredni Algoritmi i Strukture Podataka!")
	mem_object := new(memtable.Memtable)
	mem_object.LoadMemtable()

	record_object := new(record.Record)
	record_object.NewRecord("David", "Stakic")
	// record_object.NewRecord("David", "Stakic")

	mem_object.AddRecord(*record_object)
}
