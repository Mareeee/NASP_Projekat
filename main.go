package main

import (
	"fmt"
	tBucket "main/tokenBucket"
	"time"
)

func main() {
	tb := tBucket.LoadTokenBucket()

	for i := 0; i < 15; i++ {
		if tb.Take() {
			fmt.Println("Performing action ", i+1)
		} else {
			fmt.Println("Rate limit exceeded. Waiting...")
			time.Sleep(time.Second)
		}
	}
	err := tb.WriteTBToFile("data\\tokenbucket\\token_bucket_state.bin")
	if err != nil {
		fmt.Println("Nema error-a jer smo najjajjajjajaci")
	}

	deserializedTB, _ := tBucket.ReadTBFromFile("data\\tokenbucket\\token_bucket_state.bin")

	fmt.Println(deserializedTB.Capacity)
	fmt.Println(deserializedTB.LastToken)
	fmt.Println(deserializedTB.Rate)
	fmt.Println(deserializedTB.Tokens)

	for i := 0; i < 15; i++ {
		if deserializedTB.Take() {
			fmt.Println("Performing action ", i+1)
		} else {
			fmt.Println("Rate limit exceeded. Waiting...")
			time.Sleep(time.Second)
		}
	}
}
