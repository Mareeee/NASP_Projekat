package bloom

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type BloomFilter struct {
	m   uint           // duzina bloom  filtera
	k   uint           // broj hash funkcija
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
	var i uint
	keyConverted := []byte(key)
	for i = 0; i < bf.k; i++ {
		iHashed := bf.hf[i].Hash(keyConverted) % uint64(bf.m)
		bf.arr[iHashed] = 1
	}
}

func (bf *BloomFilter) CheckElement(key string) bool {
	var i uint
	keyConverted := []byte(key)
	for i = 0; i < bf.k; i++ {
		iHashed := bf.hf[i].Hash(keyConverted) % uint64(bf.m)
		if bf.arr[iHashed] == 0 {
			return false
		}
	}
	return true
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
