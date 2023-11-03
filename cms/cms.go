package cms

import (
	"fmt"
	"sort"
)

type CountMinSketch struct {
	m      uint           // duzina cms-a
	k      uint           // broj hash funkcija
	hf     []HashWithSeed // niz hash funkcija
	matrix [][]uint       // matrix bajtova
}

func (cms *CountMinSketch) CountMinSketchConstructor(epsilon float64, delta float64) {
	cms.m = CalculateM(epsilon)
	cms.k = CalculateK(delta)
	cms.hf = CreateHashFunctions(cms.k)
	cms.matrix = make([][]uint, cms.k)

	for i := range cms.matrix {
		cms.matrix[i] = make([]uint, cms.m)
	}
}

func (cms *CountMinSketch) addElement(key string) {
	var i uint
	keyConverted := []byte(key)
	for i = 0; i < cms.k; i++ {
		j := cms.hf[i].Hash(keyConverted) % uint64(cms.m)
		cms.matrix[i][j] += 1
	}
}

func (cms *CountMinSketch) numberOfRepetitions(key string) uint {
	arrOfRepetitions := make([]uint, cms.k)
	keyConverted := []byte(key)

	for i := uint(0); i < cms.k; i++ {
		j := cms.hf[i].Hash(keyConverted) % uint64(cms.m)
		arrOfRepetitions = append(arrOfRepetitions, cms.matrix[i][j])
	}

	sort.Slice(arrOfRepetitions, func(i, j int) bool {
		return arrOfRepetitions[i] > arrOfRepetitions[j]
	})

	return arrOfRepetitions[0]
}

func Cms() {
	cms := new(CountMinSketch)
	cms.CountMinSketchConstructor(0.1, 0.9)

	cms.addElement("Zoki")
	cms.addElement("Zoki")

	fmt.Printf("Number of elements: %d", cms.numberOfRepetitions("Zoki"))
}
