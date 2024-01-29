package menu

import (
	"bufio"
	"fmt"
	"main/engine"
	"os"
)

type Menu struct {
}

func (m *Menu) print() {
	fmt.Println("\n[1]	PUT")
	fmt.Println("[2]	GET")
	fmt.Println("[3]	DELETE")
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
		case "X":
			os.Exit(0)
		default:
			fmt.Println("Invalid option!")
		}
	}
}
