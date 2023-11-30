package cms

import (
	"encoding/binary"
	"os"
	"sort"
)

type CountMinSketch struct {
	m      uint32         // duzina cms-a
	k      uint32         // broj hash funkcija
	hf     []HashWithSeed // niz hash funkcija
	matrix [][]uint32     // matrix bajtova
}

func (cms *CountMinSketch) CountMinSketchConstructor(epsilon float64, delta float64) {
	cms.m = CalculateM(epsilon)
	cms.k = CalculateK(delta)
	cms.hf = CreateHashFunctions(cms.k)
	cms.matrix = make([][]uint32, cms.k)

	for i := range cms.matrix {
		cms.matrix[i] = make([]uint32, cms.m)
	}
}

func (cms *CountMinSketch) AddElement(key string) {
	keyConverted := []byte(key)
	for i := uint32(0); i < cms.k; i++ {
		j := cms.hf[i].Hash(keyConverted) % uint64(cms.m)
		cms.matrix[i][j] += 1
	}
}

func (cms *CountMinSketch) NumberOfRepetitions(key string) uint32 {
	arrOfRepetitions := make([]uint32, cms.k)
	keyConverted := []byte(key)

	for i := uint32(0); i < cms.k; i++ {
		j := cms.hf[i].Hash(keyConverted) % uint64(cms.m)
		arrOfRepetitions = append(arrOfRepetitions, cms.matrix[i][j])
	}

	sort.Slice(arrOfRepetitions, func(i, j int) bool {
		return arrOfRepetitions[i] > arrOfRepetitions[j]
	})

	return arrOfRepetitions[0]
}

func (cms *CountMinSketch) HfLength() int {
	return len(cms.hf) * 32
}

func (cms *CountMinSketch) MatrixLength() uint32 {
	return cms.m * cms.k * 4
}

func (cms *CountMinSketch) ToBytes() []byte {
	bufferSize := 8 + cms.HfLength() + int(cms.MatrixLength())
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint32(buffer[0:4], cms.m)
	binary.BigEndian.PutUint32(buffer[4:8], cms.k)
	offSet := 8
	for i := 0; i < len(cms.hf); i++ {
		copy(buffer[offSet:offSet+32], cms.hf[i].Seed)
		offSet += 32
	}
	// fmt.Println(bufferSize)
	for i := uint32(0); i < cms.k; i++ {
		for j := uint32(0); j < cms.m; j++ {
			binary.BigEndian.PutUint32(buffer[offSet:offSet+4], cms.matrix[i][j])
			// fmt.Println(offSet)
			offSet += 4
		}
	}
	return buffer
}

func (cms *CountMinSketch) WriteToBinFile() {
	data := cms.ToBytes()

	f, _ := os.OpenFile(CMS_FILE_PATH, os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()

	f.Write(data)
}

func (cms *CountMinSketch) LoadCMS() {
	f, _ := os.OpenFile(CMS_FILE_PATH, os.O_RDONLY, 0644)
	defer f.Close()

	stat, _ := f.Stat()

	data := make([]byte, stat.Size())
	f.Read(data)

	cms.m = binary.BigEndian.Uint32(data[0:4])
	cms.k = binary.BigEndian.Uint32(data[4:8])

	offSet := 8
	cms.hf = make([]HashWithSeed, cms.k)
	for i := 0; i < int(cms.k); i++ {
		cms.hf[i] = HashWithSeed{Seed: data[offSet : offSet+32]}
		offSet += 32
	}

	cms.matrix = make([][]uint32, cms.k)

	for i := range cms.matrix {
		cms.matrix[i] = make([]uint32, cms.m)
	}

	for i := uint32(0); i < cms.k; i++ {
		for j := uint32(0); j < cms.m; j++ {
			cms.matrix[i][j] = binary.BigEndian.Uint32(data[offSet : offSet+4])
			offSet += 4
		}
	}
}
