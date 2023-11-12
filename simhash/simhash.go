package simhash

import (
	"strings"
)

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

func RemoveStopWords(text string) string {
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

	var builder strings.Builder
	builder.WriteString(strings.Join(cleanedWords, " ")) // spajamo sve reci u string

	return builder.String()
}
