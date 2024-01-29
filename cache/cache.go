package cache

import (
	"main/record"
)

// Struktura koja reprezentuje kes
type Cache struct {
	data    map[string]record.Record
	maxSize int
}

// Kreiranje novog kesa
func NewCache(maxSize int) *Cache {
	return &Cache{
		data:    make(map[string]record.Record),
		maxSize: maxSize,
	}
}

// Dodavanje novog para kljuc-vrednost u kes
func (c *Cache) Set(key string, record record.Record) {
	// Proverava se da li je pun kes
	if len(c.data) >= c.maxSize {
		// Uklanjanje najredje upotrebljivanog(prvi u mapi)
		for k := range c.data {
			delete(c.data, k)
			break
		}
	}

	// Postavlja par kljuc-vrednsot
	c.data[key] = record
}

// Vraca vrednost iz kesa pridruzenu uz kljuc iz argumenta funkcije
func (c *Cache) Get(key string) (*record.Record, bool) {
	// Proverava da li kljuc postoji u kesu
	record, ok := c.data[key]
	return &record, ok
}
