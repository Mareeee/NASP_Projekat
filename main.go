package main

import (
	"fmt"
	"main/memtable"
	"main/record"
)

func main() {
	fmt.Println("Napredni Algoritmi i Strukture Podataka!")
	mem_object := new(memtable.Memtable)
	mem_object.MemtableConstructor()

	record_object1 := record.NewRecord("David1", []byte{72})
	record_object2 := record.NewRecord("David2", []byte{101})
	record_object3 := record.NewRecord("David3", []byte{108})
	record_object4 := record.NewRecord("David3", []byte{109})
	record_object5 := record.NewRecord("David5", []byte{111})
	record_object6 := record.NewRecord("David6", []byte{32})
	record_object7 := record.NewRecord("David7", []byte{71})
	record_object8 := record.NewRecord("David5", []byte{112})
	record_object9 := record.NewRecord("David9", []byte{33})
	record_object10 := record.NewRecord("David4", []byte{113})
	record_object11 := record.NewRecord("David8", []byte{114})
	record_object12 := record.NewRecord("David10", []byte{115})

	mem_object.Insert(*record_object1)
	mem_object.Insert(*record_object2)
	mem_object.Insert(*record_object3)
	mem_object.Insert(*record_object4)
	mem_object.Delete(*record_object2)
	mem_object.Delete(*record_object3)
	mem_object.Insert(*record_object5)
	mem_object.Insert(*record_object6)
	mem_object.Insert(*record_object7)
	mem_object.Insert(*record_object8)
	mem_object.Delete(*record_object7)
	mem_object.Delete(*record_object8)
	mem_object.Insert(*record_object9)
	mem_object.Insert(*record_object10)
	mem_object.Insert(*record_object11)
	mem_object.Insert(*record_object12)
}
