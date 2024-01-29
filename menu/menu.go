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
			//engine.Put()
		case "2":
			record := engine.Get("Mare")
			fmt.Println(record)
		case "3":
			engine.Delete("Mare")
		case "X":
			os.Exit(0)
		default:
			fmt.Println("Invalid option!")
		}
	}
}
