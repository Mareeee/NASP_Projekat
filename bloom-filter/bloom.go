package bloom

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
)

type BloomFilter struct {
	m   uint32         // duzina bloom  filtera
	k   uint32         // broj hash funkcija
	arr []byte         // niz bajtova
	hf  []HashWithSeed // niz hash funkcija
}

func (bf *BloomFilter) BloomFilterConstructor(expectedElements int, falsePositiveRate float64) {
	bf.m = CalculateM(expectedElements, falsePositiveRate)
	bf.k = CalculateK(expectedElements, bf.m)
	bf.hf = CreateHashFunctions(bf.k)
	bf.arr = make([]byte, bf.m)
}

func (bf *BloomFilter) AddElement(key string) {
	var i uint32
	keyConverted := []byte(key)
	for i = 0; i < bf.k; i++ {
		iHashed := bf.hf[i].Hash(keyConverted) % uint64(bf.m)
		bf.arr[iHashed] = 1
	}
}

func (bf *BloomFilter) CheckElement(key string) bool {
	var i uint32
	keyConverted := []byte(key)
	for i = 0; i < bf.k; i++ {
		iHashed := bf.hf[i].Hash(keyConverted) % uint64(bf.m)
		if bf.arr[iHashed] == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) HfLength() int {
	return len(bf.hf) * 32
}

func (bf *BloomFilter) ToBytes() []byte {
	bufferSize := 8 + len(bf.arr) + bf.HfLength()
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint32(buffer[0:4], bf.m)
	copy(buffer[4:len(bf.arr)+4], bf.arr)
	binary.BigEndian.PutUint32(buffer[len(bf.arr)+4:len(bf.arr)+8], bf.k)
	offSet := len(bf.arr) + 8
	for i := 0; i < len(bf.hf); i++ {
		copy(buffer[offSet:offSet+32], bf.hf[i].Seed)
		offSet += 32
	}
	return buffer
}

func (bf *BloomFilter) WriteToBinFile() {
	data := bf.ToBytes()

	f, _ := os.OpenFile(BLOOM_FILE_PATH, os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()

	f.Write(data)
}

func (bf *BloomFilter) LoadBloomFilter() {
	f, _ := os.OpenFile(BLOOM_FILE_PATH, os.O_RDONLY, 0644)
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	bf.m = binary.BigEndian.Uint32(data[0:4])
	bf.arr = data[4 : bf.m+4]
	bf.k = binary.BigEndian.Uint32(data[bf.m+4 : bf.m+8])
	offSet := bf.m + 8
	bf.hf = make([]HashWithSeed, bf.k)
	for i := 0; i < int(bf.k); i++ {
		bf.hf[i] = HashWithSeed{Seed: data[offSet : offSet+32]}
		offSet += 32
	}
}

func Bloom() {
	fns := CreateHashFunctions(5)

	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	decoder := gob.NewDecoder(buf)

	for _, fn := range fns {
		data := []byte("hello")
		fmt.Printf("hashed value [before hash fn serialization]: %v\n", fn.Hash(data))
		err := encoder.Encode(fn)
		if err != nil {
			panic(err)
		}
		dfn := &HashWithSeed{}
		err = decoder.Decode(dfn)
		if err != nil {
			panic(err)
		}
		fmt.Printf("hashed value [after hash fn deserialization]: %v\n\n", fn.Hash(data))
	}

}
