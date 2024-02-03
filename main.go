package main

import (
	"main/engine"
	"main/test"
)

func main() {
	engine := new(engine.Engine)
	engine.Engine()
	// menu := new(menu.Menu)
	// menu.Start()
	// test.GenerateRandomRecordsForEvery50000(*engine)
	// cfg := new(config.Config)
	// config.LoadConfig(cfg)
	// wal, _ := wal.LoadWal(*cfg)
	// wal.AddRecord("Mare", []byte("Care"), false)
	records := test.GenerateRandomRecords(5)
	for i := 0; i < len(records); i++ {
		engine.Put(records[i].Key, records[i].Value, false)
	}

	// fmt.Printf("engine.Get(\"0\"): %v\n", engine.Get("0"))

	// meni := menu.Menu{}
	// meni.Start()
	// rekordi, _ := wal.LoadAllRecords()

	// valueSlice := make([]record.Record, len(rekordi))
	// for i, ptr := range rekordi {
	// 	valueSlice[i] = *ptr
	// }

	// sstable.NewSSTable(valueSlice, cfg, 1)
	// lsm.SizeTiered()
}
