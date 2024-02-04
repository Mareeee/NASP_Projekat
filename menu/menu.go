package menu

import (
	"bufio"
	"fmt"
	"main/engine"
	"os"
	"strconv"
	"strings"
	"time"
)

type Menu struct {
	engine engine.Engine
	reader *bufio.Reader
}

func (m *Menu) print() {
	fmt.Println("\n======== MENU ========")
	fmt.Println("[1]	PUT")
	fmt.Println("[2]	GET")
	fmt.Println("[3]	DELETE")
	fmt.Println("[4]	CountMinSketch options")
	fmt.Println("[5]	BloomFilter options")
	fmt.Println("[6]	HyperLogLog options")
	fmt.Println("[7]	SimHash options")
	fmt.Println("[8] 	Prefix Scan")
	fmt.Println("[9] 	Range Scan")
	fmt.Println("[10]	Prefix Iterator")
	fmt.Println("[11]	Range Iterator")
	fmt.Println("[X]	EXIT")
	fmt.Println("======================")
	fmt.Print(">> ")
}

func (m *Menu) Start() {
	m.engine.Engine()
	m.reader = bufio.NewReader(os.Stdin)

	for {
		m.print()
		optionScanner := bufio.NewScanner(os.Stdin)
		optionScanner.Scan()
		option := optionScanner.Text()

		if m.engine.Tbucket.Take() {
			switch option {
			case "1":
				key, value := m.InputKeyValue(true)
				m.engine.Put(key, value, false)
			case "2":
				key, _ := m.InputKeyValue(false)
				record := m.engine.Get(key)
				if record == nil {
					fmt.Println("Record not found.")
				} else {
					fmt.Println("Value: " + string(record.Value))
				}
			case "3":
				key, _ := m.InputKeyValue(false)
				err := m.engine.Delete(key)
				if err != nil {
					fmt.Println(err)
				}
			case "4":
				m.CMSOptions()
			case "5":
				m.BloomFilterOptions()
			case "6":
				m.HLLOptions()
			case "7":
				m.SimHashOptions()
			case "8":
				m.PrefixScan()
			case "9":
				m.RangeScan()
			case "10":
				m.PrefixIterator()
			case "11":
				m.RangeIterator()
			case "X":
				record := m.engine.Tbucket.ToBytes()
				m.engine.Put("tb_", record, false)
				os.Exit(0)
			case "x":
				record := m.engine.Tbucket.ToBytes()
				m.engine.Put("tb_", record, false)
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

func (m *Menu) HLLOptions() {
	fmt.Println("\n======= HLL MENU =======")
	fmt.Println("[1]	Create Instance")
	fmt.Println("[2]	Delete Instance")
	fmt.Println("[3]	Add Key")
	fmt.Println("[4]	Check Cardinality For Key")
	fmt.Println("========================")
	fmt.Print(">>")

	option := m.InputString()

	if m.engine.Tbucket.Take() {
		switch option {
		case "1":
			fmt.Print("Choose key for new HyperLogLog: ")
			key := m.InputString()
			fmt.Print("Input p for your HyperLogLog: ")
			p := m.InputInt()
			m.engine.HLLCreateNewInstance(key, p)
		case "2":
			fmt.Print("Input key of HyperLogLog you want to delete: ")
			key := m.InputString()
			m.engine.HLLDeleteInstance(key)
		case "3":
			fmt.Print("Input key of HyperLogLog you want to add element to: ")
			keyhll := m.InputString()
			fmt.Print("Input key you want to add: ")
			key := m.InputString()
			m.engine.HLLAddElement(keyhll, key)
		case "4":
			fmt.Print("Input key of HyperLogLog you want to see the estimation for: ")
			key := m.InputString()
			m.engine.HLLCardinality(key)
		default:
			fmt.Println("Invalid option!")
		}
	} else {
		fmt.Println("Rate limit exceeded. Waiting...")
		time.Sleep(time.Second)
	}
}

func (m *Menu) SimHashOptions() {
	fmt.Println("\n======= SIMHASH MENU =======")
	fmt.Println("[1]	Calculate Fingerprint For Text")
	fmt.Println("[2]	Calculate Hamming Distance For Two Texts")
	fmt.Println("============================")
	fmt.Print(">> ")

	option := m.InputString()

	if m.engine.Tbucket.Take() {
		switch option {
		case "1":
			fmt.Print("Choose key for new SimHash: ")
			key := m.InputString()
			fmt.Print("Input text for which you want to calculate fingerprint: ")
			text := m.InputString()

			err := m.engine.CalculateFingerprintSimHash(key, text)
			if err != nil {
				fmt.Println(err)
			}
		case "2":
			fmt.Print("Input key for first SimHash: ")
			key1 := m.InputString()
			if key1 == "" {
				fmt.Println("invalid key")
				return
			}

			fmt.Print("Input key for second SimHash: ")
			key2 := m.InputString()
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
	fmt.Println("\n===== BLOOMFILTER MENU =====")
	fmt.Println("[1]	Create Instance")
	fmt.Println("[2]	Delete Instance")
	fmt.Println("[3]	Add Key")
	fmt.Println("[4]	Check Presence Of Key")
	fmt.Println("============================")
	fmt.Print(">> ")

	option := m.InputString()

	if m.engine.Tbucket.Take() {
		switch option {
		case "1":
			fmt.Print("Choose key for new BloomFilter: ")
			key := m.InputString()
			fmt.Print("Input expectedElements for your BloomFilter: ")
			expectedElements := m.InputInt()
			fmt.Print("Input falsePositiveRate for your BloomFilter: ")
			falsePositiveRate := float64(m.InputInt())
			m.engine.BloomFilterCreateNewInstance(key, expectedElements, falsePositiveRate)
		case "2":
			fmt.Print("Input key of BloomFilter you want to delete: ")
			key := m.InputString()
			if strings.HasPrefix(key, "bf_") {
				m.engine.Delete(key)
			} else {
				fmt.Println("Such BloomFilter doesn't exist")
			}
		case "3":
			fmt.Print("Input key of BloomFilter you want to add element to: ")
			key_bf := m.InputString()
			fmt.Print("Input key you want to add: ")
			key := m.InputString()
			m.engine.BloomFilterAddElement(key_bf, key)
		case "4":
			fmt.Print("Input key of BloomFilter you want to check presence of key: ")
			key_bf := m.InputString()
			fmt.Print("Input key you want to check: ")
			key := m.InputString()
			m.engine.BloomFilterCheckElement(key_bf, key)
		default:
			fmt.Println("Invalid option!")
		}
	} else {
		fmt.Println("Rate limit exceeded. Waiting...")
		time.Sleep(time.Second)
	}
}

func (m *Menu) CMSOptions() {
	fmt.Println("\n======= CMS MENU =======")
	fmt.Println("[1]	Create Instance")
	fmt.Println("[2]	Delete Instance")
	fmt.Println("[3]	Add Key")
	fmt.Println("[4]	Check Repetitions Of Key")
	fmt.Println("========================")
	fmt.Print(">>")

	option := m.InputString()

	if m.engine.Tbucket.Take() {
		switch option {
		case "1":
			fmt.Print("Choose key for your CMS: ")
			key := m.InputString()
			fmt.Print("Input epsilon for your CMS: ")
			epsilon := float64(m.InputInt())
			fmt.Print("Input delta for your CMS: ")
			delta := float64(m.InputInt())
			m.engine.CMSCreateNewInstance(key, epsilon, delta)
		case "2":
			fmt.Print("Input key of CMS you want to delete: ")
			key := m.InputString()
			m.engine.Delete(key)
		case "3":
			fmt.Println("Input key of CMS you want to add element to: ")
			key_cms := m.InputString()
			fmt.Print("Input key you want to add: ")
			key := m.InputString()
			m.engine.CMSAddElement(key_cms, key)
		case "4":
			fmt.Println("Input key of CMS you want to check key repetition: ")
			key_cms := m.InputString()
			fmt.Print("Input key you want to check: ")
			key := m.InputString()
			m.engine.CMSCheckFrequency(key_cms, key)
		default:
			fmt.Println("Invalid option!")
		}
	} else {
		fmt.Println("Rate limit exceeded. Waiting...")
		time.Sleep(time.Second)
	}
}

func (m *Menu) PrefixScan() {
	fmt.Print("Enter prefix: ")
	prefix := m.InputString()
	fmt.Print("Enter page number: ")
	pageNumber := m.InputInt()
	fmt.Print("Enter page size: ")
	pageSize := m.InputInt()

	page := m.engine.PrefixScan(strings.ToLower(prefix), pageNumber, pageSize)

	if len(page) == 0 || page == nil {
		fmt.Println("There are not records with given prefix.")
	} else {
		fmt.Printf("Page %d:\n", pageNumber)

		for i := 0; i < len(page); i++ {
			fmt.Printf("Record %d: %s\t%s\n", (i + 1), page[i].Key, page[i].Value)
		}
	}
}

func (m *Menu) RangeScan() {
	fmt.Print("Enter min key: ")
	minKey := m.InputString()
	fmt.Print("Enter max key: ")
	maxKey := m.InputString()
	fmt.Print("Enter page number: ")
	pageNumber := m.InputInt()
	fmt.Print("Enter page size: ")
	pageSize := m.InputInt()

	page := m.engine.RangeScan(minKey, maxKey, pageNumber, pageSize)

	if page == nil || len(page) == 0 {
		fmt.Println("There are not records in given range.")
	} else {
		fmt.Printf("Page %d:\n", pageNumber)

		for i := 0; i < len(page); i++ {
			fmt.Printf("Record %d: %s\t%s\n", (i + 1), page[i].Key, page[i].Value)
		}
	}
}

func (m *Menu) PrefixIterator() {
	fmt.Print("Enter prefix: ")
	prefix := m.InputString()

	input := ""
	pageNumber := 1
	for strings.ToLower(input) != "stop" {
		page := m.engine.PrefixScan(prefix, pageNumber, 1)

		if page == nil && pageNumber == 1 {
			fmt.Println("There are not records with given prefix.")
			break
		} else if page == nil {
			fmt.Println("No more records.")
			break
		} else {
			fmt.Printf("Record: %s\t%s\n", page[0].Key, page[0].Value)
		}

		fmt.Print(">> ")
		input = m.InputString()
		for strings.ToLower(input) != "next" && strings.ToLower(input) != "stop" {
			fmt.Print(">> ")
			input = m.InputString()
		}

		pageNumber++
	}
}

func (m *Menu) RangeIterator() {
	fmt.Print("Enter min key: ")
	minKey := m.InputString()
	fmt.Print("Enter max key: ")
	maxKey := m.InputString()

	input := ""
	pageNumber := 1
	for strings.ToLower(input) != "stop" {
		page := m.engine.RangeScan(minKey, maxKey, pageNumber, 1)

		if page == nil && pageNumber == 1 {
			fmt.Println("There are not records in this range.")
			break
		} else if page == nil {
			fmt.Println("No more records.")
			break
		} else {
			fmt.Printf("Record: %s\t%s\n", page[0].Key, page[0].Value)
		}

		fmt.Print(">> ")
		input = m.InputString()
		for strings.ToLower(input) != "next" && strings.ToLower(input) != "stop" {
			fmt.Print(">> ")
			input = m.InputString()
		}

		pageNumber++
	}
}

func (m *Menu) PrefixIterator2() {
	fmt.Print("Enter prefix: ")
	prefix := m.InputString()

	input := ""
	pageNumber := 1
	for {
		page := m.engine.PrefixScan(prefix, pageNumber, 10)

		if page == nil && pageNumber == 1 {
			fmt.Println("There are not records with given prefix.")
			break
		} else if page == nil {
			fmt.Println("No more records.")
			break
		} else {
			fmt.Printf("Record: %s\t%s\n", page[0].Key, page[0].Value)

			fmt.Print(">> ")
			input = m.InputString()
			for strings.ToLower(input) != "next" || strings.ToLower(input) != "stop" {
				fmt.Print(">> ")
				input = m.InputString()
			}

			if strings.ToLower(input) == "stop" {
				break
			} else {
				i := 1
				for strings.ToLower(input) == "next" {
					if i == 10 {
						break
					}

					fmt.Printf("Record: %s\t%s\n", page[i].Key, page[i].Value)
					fmt.Print(">> ")
					input = m.InputString()
					for strings.ToLower(input) != "next" || strings.ToLower(input) != "stop" {
						fmt.Print(">> ")
						input = m.InputString()
					}
					i++
				}
				if strings.ToLower(input) == "stop" {
					break
				}
			}
		}
		pageNumber++
	}
}

func (m *Menu) RangeIterator2() {
	fmt.Print("Enter min key: ")
	minKey := m.InputString()
	fmt.Print("Enter max key: ")
	maxKey := m.InputString()

	input := ""
	pageNumber := 1
	for {
		page := m.engine.RangeScan(minKey, maxKey, pageNumber, 10)

		if page == nil && pageNumber == 1 {
			fmt.Println("There are not records with given prefix.")
			break
		} else if page == nil {
			fmt.Println("No more records.")
			break
		} else {
			fmt.Printf("Record: %s\t%s\n", page[0].Key, page[0].Value)

			fmt.Print(">> ")
			input = m.InputString()
			for strings.ToLower(input) != "next" || strings.ToLower(input) != "stop" {
				fmt.Print(">> ")
				input = m.InputString()
			}

			if strings.ToLower(input) == "stop" {
				break
			} else {
				i := 1
				for strings.ToLower(input) == "next" {
					if i == 10 {
						break
					}

					fmt.Printf("Record: %s\t%s\n", page[i].Key, page[i].Value)
					fmt.Print(">> ")
					input = m.InputString()
					for strings.ToLower(input) != "next" || strings.ToLower(input) != "stop" {
						fmt.Print(">> ")
						input = m.InputString()
					}
					i++
				}
				if strings.ToLower(input) == "stop" {
					break
				}
			}
		}
		pageNumber++
	}
}

func (m *Menu) InputKeyValue(inputValueAlso bool) (string, []byte) {
	fmt.Print("Input key: ")
	key, _ := m.reader.ReadString('\n')
	key = strings.TrimSpace(key)
	if key == "" || key == "tb_" {
		fmt.Println("invalid key")
		return "", nil
	}
	if inputValueAlso {
		fmt.Print("Input value: ")
		value, _ := m.reader.ReadString('\n')
		value = strings.TrimSpace(value)
		return key, []byte(value)
	} else {
		return key, nil
	}
}

func (m *Menu) InputString() string {
	input, _ := m.reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func (m *Menu) InputInt() int {
	input := m.InputString()
	number, _ := strconv.Atoi(input)
	return number
}
