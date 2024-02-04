package main

import "main/menu"

func main() {
	// engine := new(engine.Engine)
	// engine.Engine()

	// engine.Put("Mare", []byte("Senta"), false)
	// engine.Put("David", []byte("Stakic"), false)
	// engine.Put("Igor", []byte("Nikolic"), false)
	// engine.Put("Vlado", []byte("Kralj"), false)
	// engine.Put("Gic", []byte("Kula"), false)
	// engine.Put("asdf", []byte("bre"), false)
	// engine.Put("qwer", []byte("nebitno"), false)
	// engine.Put("moja", []byte("mala"), false)
	// engine.Put("tvoja", []byte("keva"), false)

	// fmt.Printf("engine.Get(\"Vlado\"): %v\n", engine.Get("Mare"))

	menu := new(menu.Menu)
	menu.Start()
	// test.GenerateRandomRecordsForEvery50000(*engine)
	// cfg := new(config.Config)
	// config.LoadConfig(cfg)

	// fmt.Println(engine.Get("320hehe"))
	// records := test.GenerateRandomRecords(5)
	// for i := 0; i < len(records); i++ {
	// 	engine.Put(records[i].Key, records[i].Value, false)
	// }
	// test.GenerateRandomRecordsForEvery100(*engine)
	// for i := 0; i < len(records); i++ {
	// 	engine.Put(records[i].Key, records[i].Value, false)
	// }

	// serializedKeyDictionary, _ := engine.SerializeMap(engine.KeyDictionary)
	// f, _ := os.OpenFile(config.KEY_DICTIONARY_FILE_PATH, os.O_CREATE|os.O_WRONLY, 0644)
	// defer f.Close()

	// f.Write(serializedKeyDictionary)
}
