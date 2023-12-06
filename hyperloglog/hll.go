package hll

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

type HLL struct {
	m   uint64 //duzina niza
	p   uint8  //preciznost
	reg []uint8
}

func (hll *HLL) NewHyperLogLog(p uint8) {
	hll.p = p
	hll.m = uint64(math.Pow(float64(2), float64(hll.p)))
	hll.reg = make([]uint8, hll.m)
}

func (hll *HLL) AddElement(key string) {
	keyConverted := []byte(key)
	keyHash := Hash(keyConverted)
	bucket := firstKbits(uint64(keyHash), uint64(hll.p))
	value := uint8(trailingZeroBits(keyHash) + 1)
	if hll.reg[bucket] < value {
		hll.reg[bucket] = value
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) toBytes() []byte {
	bufferSize := 9 + len(hll.reg)
	fmt.Println(bufferSize)
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint64(buffer[0:8], hll.m)
	copy(buffer[8:9], []byte{hll.p})
	offSet := 9
	for i := 0; i < len(hll.reg); i++ {
		copy(buffer[offSet:offSet+1], []byte{hll.reg[i]})
		offSet += 1
	}
	return buffer
}

func (hll *HLL) WriteToBinFile() {
	data := hll.toBytes()

	f, _ := os.OpenFile(HLL_FILE_PATH, os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()

	f.Write(data)
}

func (hll *HLL) LoadHLL() {
	f, _ := os.OpenFile(HLL_FILE_PATH, os.O_RDONLY, 0644)
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	hll.m = binary.BigEndian.Uint64(data[0:8])

	for _, b := range data[8:9] {
		hll.p = (hll.p << 1) | uint8(b) // posto ne postoji binary.BigEndian.Uint8, shift-ovali smo vrednost za 1 bajt u levo
	}

	offSet := 9
	hll.reg = make([]uint8, hll.m)
	for i := 0; i < int(hll.m); i++ {
		for _, b := range data[offSet : offSet+1] {
			hll.reg[i] = (hll.reg[i] << 1) | uint8(b) // posto ne postoji binary.BigEndian.Uint8, shift-ovali smo vrednost za 1 bajt u levo i dodelili vrednost niza
		}
		offSet += 1
	}
}
