package tokenbucket

import (
	"encoding/binary"
	"main/config"
	"os"
	"time"
)

type TokenBucket struct {
	config    config.Config
	Tokens    uint64
	LastToken time.Time
}

func LoadTokenBucket(config config.Config) *TokenBucket {
	tb := new(TokenBucket)
	tb.config = config
	tb.Tokens = tb.config.Capacity
	tb.LastToken = time.Now()
	return tb
}

func (tb *TokenBucket) AddToken() {
	now := time.Now()
	elapsed := now.Sub(tb.LastToken)
	tokensToAdd := uint64(elapsed.Seconds()) * tb.config.Rate

	if tokensToAdd > 0 {
		tb.Tokens = tb.Tokens + tokensToAdd
		if tb.Tokens > tb.config.Capacity {
			tb.Tokens = tb.config.Capacity
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

	binary := tb.ToBytes()
	file.Write(binary)

	return nil
}

func (tb *TokenBucket) TBFromBytes(record []byte) *TokenBucket {
	tb.config.Capacity = binary.BigEndian.Uint64(record[0:8])
	tb.config.Rate = binary.BigEndian.Uint64(record[8:16])
	tb.Tokens = binary.BigEndian.Uint64(record[16:24])
	tb.LastToken = time.Unix(0, int64(binary.LittleEndian.Uint64(record[24:32])))

	return tb
}

func (tb *TokenBucket) ToBytes() []byte {
	bufferSize := 24 + len(tb.LastToken.String())
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint64(buffer[0:8], tb.config.Capacity)
	binary.BigEndian.PutUint64(buffer[8:16], tb.config.Rate)
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

	tb.config.Capacity = binary.BigEndian.Uint64(data[0:8])
	tb.config.Rate = binary.BigEndian.Uint64(data[8:16])
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
