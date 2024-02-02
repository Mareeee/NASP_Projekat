package main

import (
	"main/config"
	"main/engine"
	"main/test"
)

func main() {
	engine := new(engine.Engine)
	engine.Engine()
	cfg := new(config.Config)
	config.LoadConfig(cfg)
	// wal, _ := wal.LoadWal(cfg)
	// wal.AddRecord("Mare", []byte("Care"), false)
	// wal.AddRecord("Dare", []byte("Kare"), false)
	// wal.AddRecord("Vlado", []byte("Kralj"), false)
	// wal.AddRecord("David", []byte("Stakara"), false)
	// wal.AddRecord("Roksi", []byte("Koksi"), false)
	records := test.GenerateRandomRecords(5)
	for i := 0; i < len(records); i++ {
		engine.Put(records[i].Key, records[i].Value, false)
	}
	// meni := menu.Menu{}
	// meni.Start()
	// rekordi, _ := wal.IndependentLoadAllRecords()

	// valueSlice := make([]record.Record, len(rekordi))
	// for i, ptr := range rekordi {
	// 	valueSlice[i] = *ptr
	// }

	// sstable.NewSSTable(rekordi, cfg, 1, &engine.KeyDictionary)
	// lsm.SizeTiered(cfg, &engine.KeyDictionary)
}
