package main

import (
	"fmt"
	"main/config"
	"main/engine"
	"os"
)

func main() {
	engine := new(engine.Engine)
	engine.Engine()

	// menu := new(menu.Menu)
	// menu.Start()
	// test.GenerateRandomRecordsForEvery50000(*engine)
	cfg := new(config.Config)
	config.LoadConfig(cfg)

	fmt.Println(engine.Get("100"))
	// test.GenerateRandomRecordsForEvery100(*engine)
	// for i := 0; i < len(records); i++ {
	// 	engine.Put(records[i].Key, records[i].Value, false)
	// }

	serializedKeyDictionary, _ := engine.SerializeMap(engine.KeyDictionary)
	f, _ := os.OpenFile(config.KEY_DICTIONARY_FILE_PATH, os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()

	f.Write(serializedKeyDictionary)
}
