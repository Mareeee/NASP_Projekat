package menu

import (
	"bufio"
	"fmt"
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
	for {
		m.print()
		optionScanner := bufio.NewScanner(os.Stdin)
		optionScanner.Scan()
		option := optionScanner.Text()

		switch option {
		case "1":
			fmt.Println("PUT: ")
		case "2":
			fmt.Println("GET: ")
		case "3":
			fmt.Println("DELETE: ")
		case "X":
			fmt.Println("Exiting...")
			os.Exit(0)
		default:
			fmt.Println("Invalid option!")
		}
	}
}
