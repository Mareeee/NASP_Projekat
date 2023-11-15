package simhash

import (
	"crypto/md5"
	"fmt"
	"strings"
)

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

func CalculateWordWeights(text []string) map[string]int {
	wordWeights := make(map[string]int)

	for _, word := range text {
		wordWeights[word]++
	}

	return wordWeights
}

func GetHashAsString(data []byte) string { // moramo ispraviti ovu funkciju tako da svi hesevi budu iste velicine, zbog tabele
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%08b", res, b)
	}
	return res
}

// ovo su funkcije koje bi trebali jos da implementiramo

func CalculateDocumentFingerPrint(text string) {
	words := RemoveStopWords(text)
	wordWeights := CalculateWordWeights(words)
	wordHashes := make([]string, len(wordWeights))
	for key, _ := range wordWeights {
		wordHashes = append(wordHashes, GetHashAsString([]byte(key)))
	}

}

func ConvertZerosToMinusOnes(data string) string {
	res := ""
	for _, c := range data {
		if c == 48 {
			res = fmt.Sprintf("%s%c", res, 45)
			res = fmt.Sprintf("%s%c", res, 49)
		} else {
			res = fmt.Sprintf("%s%c", res, c)
		}
	}
	return res
}

// func convertToZeroOrOne()
// func calculateHammingDistance()
