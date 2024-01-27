package tokenbucket

import (
	"encoding/json"
	"os"
	"time"
)

type TokenBucket struct {
	Capacity  int `json:"Capacity"`
	Rate      int `json:"Rate"`
	Tokens    int
	LastToken time.Time
}

func LoadTokenBucket() *TokenBucket {
	tb := new(TokenBucket)
	tb.LoadJson()
	tb.Tokens = tb.Capacity
	tb.LastToken = time.Now()
	return tb
}

func (tb *TokenBucket) addToken() {
	now := time.Now()
	elapsed := now.Sub(tb.LastToken)
	tokensToAdd := int(elapsed.Seconds()) * tb.Rate

	if tokensToAdd > 0 {
		tb.Tokens = tb.Tokens + tokensToAdd
		if tb.Tokens > tb.Capacity {
			tb.Tokens = tb.Capacity
		}
		tb.LastToken = now
	}
}

func (tb *TokenBucket) Take() bool {
	tb.addToken()

	if tb.Tokens > 0 {
		tb.Tokens--
		return true
	}

	return false
}

// func (tb *TokenBucket) WriteTBToFile(filepath string) error {
// 	file, err := os.Open(filepath)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	if err := binary.Write(file, binary.LittleEndian, tb); err != nil {
// 		return err
// 	}

// 	return nil
// }

// func ReadTBFromFile(filepath string) (*TokenBucket, error) {
// 	file, err := os.Open(filepath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	var tb TokenBucket

// 	if err := binary.Read(file, binary.LittleEndian, &tb); err != nil {
// 		return nil, err
// 	}

// 	return &tb, nil
// }

/* Ucitava TokenBucketOptions iz config JSON fajla */
func (tb *TokenBucket) LoadJson() {
	jsonData, _ := os.ReadFile(TOKENBUCKET_CONFIG_FILE_PATH)

	json.Unmarshal(jsonData, &tb)
}

/* Upisuje TokenBucketOptions u config JSON fajl */
func (tb *TokenBucket) WriteJson() {
	jsonData, _ := json.MarshalIndent(tb, "", "  ")

	os.WriteFile(TOKENBUCKET_CONFIG_FILE_PATH, jsonData, 0644)
}

// func main() {
// 	tb := LoadTokenBucket()

// 	for i := 0; i < 15; i++ {
// 		if tb.Take() {
// 			fmt.Println("Performing action ", i+1)
// 		} else {
// 			fmt.Println("Rate limit exceeded. Waiting...")
// 			time.Sleep(time.Second)
// 		}
// 	}
// }
