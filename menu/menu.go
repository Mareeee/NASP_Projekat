package menu

import (
	"bufio"
	"fmt"
	"main/engine"
	hll "main/hyperloglog"
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
				fmt.Println("Rad sa HyperLogLog-om: ")
				hllMenu()
			case "X":
				os.Exit(0)
			case "x":
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

func hllMenu() {
	engine := engine.Engine{}
	engine.Engine()
	fmt.Println("[1]	Create new instance")
	fmt.Println("[2]	Delete already existing instance")
	fmt.Println("[3]	Adding a new element into an instance")
	fmt.Println("[4]	Provera kardiniliteta")
	fmt.Println("[X]	EXIT")
	for {
		optionScanner := bufio.NewScanner(os.Stdin)
		optionScanner.Scan()
		option := optionScanner.Text()
		switch option {
		case "1":
			fmt.Println("Choose name for you HyperLogLog: ")
			//adding record with key which is name of hyperloglog and value is
			//hyperloglog in binary
			key, _ := engine.UserInput(false)
			hloglog := hll.NewHyperLogLog(4)
			data := hloglog.ToBytes()
			engine.Put("hll_"+key, data)
		case "2":
			fmt.Println("Choose the name of HyperLogLog you want to delete: ")
			key, _ := engine.UserInput(false)
			engine.Delete(key)
		case "3":
			fmt.Println("Choose the name of HyperLogLog you want to add element to: ")
			keyhll, _ := engine.UserInput(false)
			record := engine.Get(keyhll)
			//hyperloglog not found
			if record == nil {
				continue
			}
			data := record.Value
			hloglog := hll.LoadingHLL(data)
			//adding a key
			fmt.Println("Choose the key you want to add: ")
			key, _ := engine.UserInput(false)
			hloglog.AddElement(key)
			engine.Put(keyhll, hloglog.ToBytes())
		case "4":
			fmt.Println("Choose the name of HyperLogLog you want to add element to: ")
			key, _ := engine.UserInput(false)
			record := engine.Get(key)
			//hyperloglog not found
			if record == nil {
				continue
			}
			data := record.Value
			hloglog := hll.LoadingHLL(data)
			estimation := hloglog.Estimate()
			fmt.Println("The estimation of unique element is: ", estimation)
		case "X":
			return
		default:
			fmt.Println("Invalid option!")
		}
	}
}
