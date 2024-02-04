package simhash

import (
	"crypto/md5"
	"fmt"
	"strings"
)

const HASH_SIZE = 64

func LoadFromBytes(data []byte) []int {
	fingerprint := make([]int, HASH_SIZE)

	for i, b := range data {
		for j := 0; j < 8; j++ {
			fingerprint[i*8+j] = int((b >> (7 - j)) & 1)
		}
	}

	return fingerprint
}

func ToBytes(fingerprint []int) []byte {
	byteSlice := make([]byte, HASH_SIZE/8)

	for i := 0; i < HASH_SIZE; i++ {
		byteSlice[i/8] |= byte(fingerprint[i]) << (7 - uint(i)%8)
	}

	return byteSlice
}

func removeSpecialCharacters(s string) string {
	var result strings.Builder

	for _, char := range s {
		if strings.ContainsRune(".,?!;:-()\"+", char) { // rune vam je kao char u C i C++, mozemo dodati jos neke karaktere, ali mislim da nije potrebno
			continue
		}
		result.WriteRune(char)
	}

	return result.String()
}

func removeStopWords(text string) []string {
	stopWords := map[string]bool{ // mapa sa zaustavnim recima, ovako nam je najlakse da proverimo da li je rec zaustavna
		"a": true, "an": true, "and": true, "are": true, "as": true,
		"at": true, "be": true, "but": true, "by": true, "for": true,
		"if": true, "in": true, "into": true, "is": true, "it": true,
		"no": true, "not": true, "of": true, "on": true, "or": true,
		"such": true, "that": true, "the": true, "their": true,
		"then": true, "there": true, "these": true, "they": true,
		"this": true, "to": true, "was": true, "will": true, "with": true,
	}

	text = removeSpecialCharacters(text)

	wordsSplitted := strings.Fields(text) // splitujemo teks, strings.fields splituje po whitespace karakterima

	var cleanedWords []string

	for _, word := range wordsSplitted {
		if !stopWords[strings.ToLower(word)] { // Proveravamo da li je rec zaustavna
			cleanedWords = append(cleanedWords, strings.ToLower(word))
		}
	}

	return cleanedWords
}

// broji ponavljanja reci u tekstu
func calculateWordWeights(text []string) map[string]int {
	wordWeights := make(map[string]int)

	for _, word := range text {
		wordWeights[word]++
	}

	return wordWeights
}

func getHashAsString(data []byte) string {
	hash := md5.Sum(data)

	res := ""

	for _, b := range hash {
		res = fmt.Sprintf("%s%08b", res, b)
	}
	res = res[:64] // max 128

	return res
}

// za svaku rec racuna hash
func calculateWordHashes(wordWeights map[string]int) map[string][]int {
	wordHashes := make(map[string][]int)

	for key, _ := range wordWeights {
		wordHashes[key] = convertZerosToMinusOnes(getHashAsString([]byte(key)))
	}

	return wordHashes
}

// formira tabelu i racuna vrednost tabele (sumiramo kolone, mnozeci tezine sa vrednoscu)
func calculateTable(wordWeights map[string]int, wordHashes map[string][]int) []int {
	var calculations []int

	for i := 0; i < HASH_SIZE; i++ { // za svaku kolonu prolazimo kroz svaku rec
		value := 0
		for key, weight := range wordWeights {
			value += weight * wordHashes[key][i]
		}
		calculations = append(calculations, value)
	}

	return calculations
}

func convertToZerosAndOnes(calculations []int) []int {
	for i, _ := range calculations {
		if calculations[i] > 0 {
			calculations[i] = 1
		} else {
			calculations[i] = 0
		}
	}

	return calculations
}

// racunamo b-bitni fingerprint za ulazni set
func CalculateFingerprint(text string) []int {
	words := removeStopWords(text)
	wordWeights := calculateWordWeights(words)
	wordHashes := calculateWordHashes(wordWeights) // saljemo mapu wordWeights, jer se u words-u mogu ponavljati reci
	calculations := calculateTable(wordWeights, wordHashes)
	fingerprint := convertToZerosAndOnes(calculations)

	return fingerprint
}

func convertZerosToMinusOnes(data string) []int {
	res := make([]int, HASH_SIZE)

	for i, c := range data {
		if c == 48 { // 48 je vrednost nule u ASCII tabeli
			res[i] = -1
		} else {
			res[i] = 1
		}
	}

	return res
}

func xor(fingerprint1 []int, fingerprint2 []int) []int {
	var xorArray []int

	for i := 0; i < HASH_SIZE; i++ {
		if fingerprint1[i] == fingerprint2[i] {
			xorArray = append(xorArray, 0)
		} else {
			xorArray = append(xorArray, 1)
		}
	}

	return xorArray
}

func countOnes(xorArray []int) int {
	ones := 0

	for _, v := range xorArray {
		if v == 1 {
			ones++
		}
	}

	return ones
}

func HammingDistance(fingerprint1 []int, fingerprint2 []int) int {
	return countOnes(xor(fingerprint1, fingerprint2))
}
