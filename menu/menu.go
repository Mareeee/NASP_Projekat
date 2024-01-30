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
	engine engine.Engine
}

func (m *Menu) print() {
	fmt.Println("\n[1]	PUT")
	fmt.Println("[2]	GET")
	fmt.Println("[3]	DELETE")
	fmt.Println("[4]	CountMinSketch options")
	fmt.Println("[5]	BloomFilter options")
	fmt.Println("[6]	HyperLogLog options")
	fmt.Println("[7]	SimHash options")
	fmt.Println("[X]	EXIT")
	fmt.Print(">> ")
}

func (m *Menu) Start() {
	m.engine.Engine()

	for {
		m.print()
		optionScanner := bufio.NewScanner(os.Stdin)
		optionScanner.Scan()
		option := optionScanner.Text()

		if m.engine.Tbucket.Take() {
			switch option {
			case "1":
				key, value := UserInput(true)
				m.engine.Put(key, value)
			case "2":
				key, _ := UserInput(false)
				record := m.engine.Get(key)
				fmt.Println(record)
			case "3":
				key, _ := UserInput(false)
				err := m.engine.Delete(key)
				if err != nil {
					fmt.Println(err)
				}
			case "4":
				m.engine.CmsUsage()
			case "5":
				m.BloomFilterOptions()
			case "6":
				fmt.Println("Rad sa HyperLogLog-om: ")
				hllMenu()
			case "7":
				m.SimHashOptions()
			case "X":
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
			key, _ := UserInput(false)
			hloglog := hll.NewHyperLogLog(4)
			data := hloglog.ToBytes()
			engine.Put("hll_"+key, data)
		case "2":
			fmt.Println("Choose the name of HyperLogLog you want to delete: ")
			key, _ := UserInput(false)
			engine.Delete(key)
		case "3":
			fmt.Println("Choose the name of HyperLogLog you want to add element to: ")
			keyhll, _ := UserInput(false)
			record := engine.Get(keyhll)
			//hyperloglog not found
			if record == nil {
				continue
			}
			data := record.Value
			hloglog := hll.LoadingHLL(data)
			//adding a key
			fmt.Println("Choose the key you want to add: ")
			key, _ := UserInput(false)
			hloglog.AddElement(key)
			engine.Put(keyhll, hloglog.ToBytes())
		case "4":
			fmt.Println("Choose the name of HyperLogLog you want to add element to: ")
			key, _ := UserInput(false)
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

func (m *Menu) SimHashOptions() {
	fmt.Println("\n[1]	Calculate fingerprint for text")
	fmt.Println("[2]	Calculate hamming distance for two texts")

	optionScanner := bufio.NewScanner(os.Stdin)
	optionScanner.Scan()
	option := optionScanner.Text()

	if m.engine.Tbucket.Take() {
		switch option {
		case "1":
			key, _ := UserInput(false)
			fmt.Print("Input text: ")
			textScanner := bufio.NewScanner(os.Stdin)
			textScanner.Scan()
			text := textScanner.Text()
			err := m.engine.CalculateFingerprintSimHash(key, text)
			if err != nil {
				fmt.Println(err)
			}
		case "2":
			fmt.Print("Input key1: ")
			keyScanner := bufio.NewScanner(os.Stdin)
			keyScanner.Scan()
			key1 := keyScanner.Text()
			if key1 == "" {
				fmt.Println("invalid key")
				return
			}

			fmt.Print("Input key2: ")
			keyScanner = bufio.NewScanner(os.Stdin)
			keyScanner.Scan()
			key2 := keyScanner.Text()
			if key2 == "" {
				fmt.Println("invalid key")
				return
			}

			err := m.engine.CalculateHammingDistanceSimHash(key1, key2)
			if err != nil {
				fmt.Println(err)
			}
		default:
			fmt.Println("Invalid option!")
		}
	} else {
		fmt.Println("Rate limit exceeded. Waiting...")
		time.Sleep(time.Second)
	}
}

func (m *Menu) BloomFilterOptions() {
	fmt.Println("\n[1]	Create new instance")
	fmt.Println("[2]	Delete instance")
	fmt.Println("[3]	Add element")
	fmt.Println("[4]	Search element")

	optionScanner := bufio.NewScanner(os.Stdin)
	optionScanner.Scan()
	option := optionScanner.Text()

	if m.engine.Tbucket.Take() {
		switch option {
		case "1":
			key, _ := UserInput(false)
			m.engine.BloomFilterCreateNewInstance(key)
		case "2":
			key, _ := UserInput(false)
			m.engine.Delete(key)
		case "3":
			fmt.Println("Enter instance of bloomfilter: ")
			key_bf, _ := UserInput(false)
			fmt.Println("Enter element: ")
			element, _ := UserInput(false)
			m.engine.BloomFilterAddElement(key_bf, element)
		case "4":
			fmt.Println("Enter instance of bloomfilter: ")
			key_bf, _ := UserInput(false)
			element, _ := UserInput(false)
			m.engine.BloomFilterCheckElement(key_bf, element)
		default:
			fmt.Println("Invalid option!")
		}
	} else {
		fmt.Println("Rate limit exceeded. Waiting...")
		time.Sleep(time.Second)
	}
}

func UserInput(inputValueAlso bool) (string, []byte) {
	fmt.Print("Input key: ")
	keyScanner := bufio.NewScanner(os.Stdin)
	keyScanner.Scan()
	key := keyScanner.Text()
	if key == "" {
		fmt.Println("invalid key")
		return "", nil
	}
	if inputValueAlso {
		fmt.Print("Input value: ")
		valueScanner := bufio.NewScanner(os.Stdin)
		valueScanner.Scan()
		value := valueScanner.Text()
		return key, []byte(value)
	} else {
		return key, nil
	}
}
