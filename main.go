package main

import (
	"main/config"
	"main/lsm"
	"main/sstable"
	"main/wal"
)

func main() {
	// meni := menu.Menu{}
	// meni.Start()
	cfg := new(config.Config)
	config.LoadConfig(cfg)
	wal, _ := wal.LoadWal(*cfg)
	// wal.AddRecord("Mare", []byte("Senta"), false)
	// wal.AddRecord("Gic", []byte("Kula"), false)
	// wal.AddRecord("David", []byte("Stakic"), false)
	// wal.AddRecord("Roksi", []byte("Koksi"), false)
	// wal.AddRecord("Vlado", []byte("Kralj"), false)

	rekordi, _ := wal.IndependentLoadAllRecords()
	sstable.NewSSTable(rekordi, cfg, 1)
	lsm.SizeTiered(cfg)
}
