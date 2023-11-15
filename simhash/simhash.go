package simhash

import (
	"crypto/md5"
	"fmt"
	"strings"
)

const HASH_SIZE = 64

func RemoveSpecialCharacters(s string) string {
	var result strings.Builder
	for _, char := range s {
		if strings.ContainsRune(".,?!;:-()\"+", char) { // rune vam je kao char u C i C++, mozemo dodati jos neke karaktere, ali mislim da nije potrebno
			continue
		}
		result.WriteRune(char)
	}
	return result.String()
}

func RemoveStopWords(text string) []string {
	stopWords := map[string]bool{ // mapa sa zaustavnim recima, ovako nam je najlakse da proverimo da li je rec zaustavna
		"a": true, "an": true, "and": true, "are": true, "as": true,
		"at": true, "be": true, "but": true, "by": true, "for": true,
		"if": true, "in": true, "into": true, "is": true, "it": true,
		"no": true, "not": true, "of": true, "on": true, "or": true,
		"such": true, "that": true, "the": true, "their": true,
		"then": true, "there": true, "these": true, "they": true,
		"this": true, "to": true, "was": true, "will": true, "with": true,
	}

	text = RemoveSpecialCharacters(text)

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
func CalculateWordWeights(text []string) map[string]int {
	wordWeights := make(map[string]int)

	for _, word := range text {
		wordWeights[word]++
	}

	return wordWeights
}

func GetHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%08b", res, b)
	}
	res = res[:64] // max 128
	return res
}

// za svaku rec racuna hash
func CalculateWordHashes(wordWeights map[string]int) map[string][]int {
	wordHashes := make(map[string][]int)
	for key, _ := range wordWeights {
		wordHashes[key] = ConvertZerosToMinusOnes(GetHashAsString([]byte(key)))
	}
	return wordHashes
}

// formira tabelu i racuna vrednost tabele (sumiramo kolone, mnozeci tezine sa vrednoscu)
func CalculateTable(wordWeights map[string]int, wordHashes map[string][]int) []int {
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

func ConvertToZerosAndOnes(calculations []int) []int {
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
	words := RemoveStopWords(text)
	wordWeights := CalculateWordWeights(words)
	wordHashes := CalculateWordHashes(wordWeights) // saljemo mapu wordWeights, jer se u words-u mogu ponavljati reci
	calculations := CalculateTable(wordWeights, wordHashes)
	fingerprint := ConvertToZerosAndOnes(calculations)
	return fingerprint
}

func ConvertZerosToMinusOnes(data string) []int {
	res := make([]int, HASH_SIZE)
	for i, c := range data {
		if c == 48 {
			res[i] = -1
		} else {
			res[i] = 1
		}
	}
	return res
}

func XOR(fingerprint1 []int, fingerprint2 []int) []int {
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

func CountOnes(xorArray []int) int {
	ones := 0
	for _, v := range xorArray {
		if v == 1 {
			ones++
		}
	}
	return ones
}

// vraca broj jedinica koji predstavlja hemingovu udaljenost (vece => manje poklapanje | manje => vece poklapanje)
func HammingDistance(text1 string, text2 string) int {
	text1Fingerprint := CalculateFingerprint(text1)
	text2Fingerprint := CalculateFingerprint(text2)
	xor := XOR(text1Fingerprint, text2Fingerprint)
	return CountOnes(xor)
}
