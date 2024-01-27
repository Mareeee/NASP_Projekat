package tokenbucket

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"time"
)

type TokenBucket struct {
	Capacity  uint64 `json:"Capacity"`
	Rate      uint64 `json:"Rate"`
	Tokens    uint64
	LastToken time.Time
}

func LoadTokenBucket() *TokenBucket {
	tb := new(TokenBucket)
	tb.LoadJson()
	tb.Tokens = tb.Capacity
	tb.LastToken = time.Now()
	return tb
}

func (tb *TokenBucket) AddToken() {
	now := time.Now()
	elapsed := now.Sub(tb.LastToken)
	tokensToAdd := uint64(elapsed.Seconds()) * tb.Rate

	if tokensToAdd > 0 {
		tb.Tokens = tb.Tokens + tokensToAdd
		if tb.Tokens > tb.Capacity {
			tb.Tokens = tb.Capacity
		}
		tb.LastToken = now
	}
}

func (tb *TokenBucket) WriteTBToFile(filepath string) error {

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	binary := tb.toBytes()
	file.Write(binary)

	return nil
}

func (tb *TokenBucket) toBytes() []byte {
	bufferSize := 24 + len(tb.LastToken.String())
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint64(buffer[0:8], tb.Capacity)
	binary.BigEndian.PutUint64(buffer[8:16], tb.Rate)
	binary.BigEndian.PutUint64(buffer[16:24], tb.Tokens)

	unixNano := tb.LastToken.UnixNano()

	binary.LittleEndian.PutUint64(buffer[24:32], uint64(unixNano))
	return buffer
}

func ReadTBFromFile(filepath string) (*TokenBucket, error) {
	tb := new(TokenBucket)
	f, _ := os.OpenFile(filepath, os.O_RDONLY, 0644)
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	tb.Capacity = binary.BigEndian.Uint64(data[0:8])
	tb.Rate = binary.BigEndian.Uint64(data[8:16])
	tb.Tokens = binary.BigEndian.Uint64(data[16:24])
	tb.LastToken = time.Unix(0, int64(binary.LittleEndian.Uint64(data[24:32])))

	return tb, nil
}

func (tb *TokenBucket) Take() bool {
	tb.AddToken()

	if tb.Tokens > 0 {
		tb.Tokens--
		return true
	}

	return false
}

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
