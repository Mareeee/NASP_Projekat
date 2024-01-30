package menu

import (
	"bufio"
	"fmt"
	"main/engine"
	"os"
	"time"
)

type Menu struct {
}

func (m *Menu) print() {
	fmt.Println("\n[1]	PUT")
	fmt.Println("[2]	GET")
	fmt.Println("[3]	DELETE")
	fmt.Println("[4]	CountMinSketch options")
	fmt.Println("[5]	BloomFilter options")
	fmt.Println("[6]	HyperLogLog options")
	fmt.Println("[X]	EXIT")
	fmt.Print(">> ")
}

func (m *Menu) Start() {
	engine := engine.Engine{} // preko ovoga pozivamo funkcije
	engine.Engine()

	for {
		m.print()
		optionScanner := bufio.NewScanner(os.Stdin)
		optionScanner.Scan()
		option := optionScanner.Text()

		if engine.Tbucket.Take() {
			switch option {
			case "1":
				key, value := engine.UserInput(true)
				engine.Put(key, value)
			case "2":
				key, _ := engine.UserInput(false)
				record := engine.Get(key)
				fmt.Println(record)
			case "3":
				key, _ := engine.UserInput(false)
				err := engine.Delete(key)
				if err != nil {
					fmt.Println(err)
				}
			case "4":
				// cms
			case "5":
				engine.BloomFilterOptions()
			case "6":
				// hll
			case "X":
				os.Exit(0)
			default:
				fmt.Println("Invalid option!")
			}
		} else {
			fmt.Println("Rate limit exceeded. Waiting...")
			time.Sleep(time.Second)
		}

	}
}
