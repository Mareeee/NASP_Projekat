package main

import (
	"main/wal"
)

func main() {
	wal_object := wal.LoadWal()
	wal_object.AddRecord("Marko", []byte("Senta"))
}
