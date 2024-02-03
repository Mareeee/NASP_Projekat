package main

import (
	"main/engine"
	"strconv"
)

func main() {
	engine := new(engine.Engine)
	engine.Engine()
	// engine := new(engine.Engine)
	// engine.Engine()
	for i := 0; i < 150; i++ {
		engine.Put(strconv.Itoa(i), []byte("ksljfsdsdfsadfsad"), false)
	}
	// test.GenerateRandomRecordsForEvery50000(*engine)
	// cfg := new(config.Config)
	// config.LoadConfig(cfg)
	// wal, _ := wal.LoadWal(*cfg)
	// wal.AddRecord("Mare", []byte("Care"), false)
	// records := test.GenerateRandomRecords(5)
	// for i := 0; i < len(records); i++ {
	//     engine.Put(records[i].Key, records[i].Value, false)
	// }
	// meni := menu.Menu{}
	// meni.Start()
	// rekordi, _ := wal.LoadAllRecords()

	// valueSlice := make([]record.Record, len(rekordi))
	// for i, ptr := range rekordi {
	//     valueSlice[i] = *ptr
	// }

	// sstable.NewSSTable(valueSlice, cfg, 1)
	// lsm.SizeTiered()
}
