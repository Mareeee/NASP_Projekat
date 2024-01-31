package main

import (
	"main/engine"
	"main/test"
)

func main() {
	engine := new(engine.Engine)
	engine.Engine()
	records := test.GenerateRandomRecords(5)
	for i := 0; i < len(records); i++ {
		engine.Put(records[i].Key, records[i].Value)
	}
	// meni := menu.Menu{}
	// meni.Start()
}
