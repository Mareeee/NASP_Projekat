package main

import "main/engine"

func main() {
	engine := new(engine.Engine)
	engine.Engine()
	// engine.Put("1", []byte("dskflsdj"), false)
	// engine.Put("lksdjflkds", []byte("dskflsdj"), false)
	// engine.Put("fdasf", []byte("dskflsdj"), false)
	// engine.Put("lksflkds", []byte("dskflsdj"), false)
	// engine.Put("hgfhfgh", []byte("dskflsdj"), false)
	// engine.Put("lksdfsfs", []byte("dskflsdj"), false)
	// engine.Put("hghg", []byte("dskflsdj"), false)
	// engine.Put("lksaaaads", []byte("dskflsdj"), false)
	// engine.Put("3", []byte("dskflsdj"), false)
	// engine.Put("lksdiuiukds", []byte("dskflsdj"), false)
	// engine.Put("reuyyruw", []byte("dskflsdj"), false)
	// engine.Put("lksqwqeds", []byte("dskflsdj"), false)
	// engine.Put("zzz", []byte("dskflsdj"), false)
	// engine.Put("bbdjflkds", []byte("dskflsdj"), false)
	// engine.Put("rrr", []byte("dskflsdj"), false)
	// engine.Put("lksooods", []byte("dskflsdj"), false)
	// engine.Put("tt", []byte("dskflsdj"), false)
	// engine.Put("lppplkds", []byte("dskflsdj"), false)
	// engine.Put("vnvnnv", []byte("dskflsdj"), false)
	// engine.Put("llalasaslkds", []byte("dskflsdj"), false)
	// engine.Put("tetete", []byte("dskflsdj"), false)
	// engine.Put("wqwqlkds", []byte("dskflsdj"), false)
	// engine.Put("aaaaaaaaaaa", []byte("dskflsdj"), false)
	// engine.Put("333333333", []byte("dskflsdj"), false)
	engine.PrefixScan("lks", 1, 3)

	// menu := new(menu.Menu)
	// menu.Start()
	// test.GenerateRandomRecordsForEvery50000(*engine)
	// cfg := new(config.Config)
	// config.LoadConfig(cfg)

	// fmt.Println(engine.Get("100"))
	// // test.GenerateRandomRecordsForEvery100(*engine)
	// // for i := 0; i < len(records); i++ {
	// // 	engine.Put(records[i].Key, records[i].Value, false)
	// // }

	// serializedKeyDictionary, _ := engine.SerializeMap(engine.KeyDictionary)
	// f, _ := os.OpenFile(config.KEY_DICTIONARY_FILE_PATH, os.O_CREATE|os.O_WRONLY, 0644)
	// defer f.Close()

	// f.Write(serializedKeyDictionary)
}
