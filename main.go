package main

import (
	"main/menu"
)

func main() {
	menu := new(menu.Menu)
	menu.Start()

	// TEST
	// engine := new(engine.Engine)
	// engine.Engine()
	// test.GenerateRandomRecordsForEvery100(*engine)
	// test.GenerateRandomRecordsForEvery50000(*engine)
}
