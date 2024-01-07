package main

import (
	"fmt"
	"sync"
)

// Struktura koja reprezentuje kes
type Cache struct {
	mu      sync.Mutex
	data    map[string]interface{}
	maxSize int
}

// Kreiranje novog kesa
func NewCache(maxSize int) *Cache {
	return &Cache{
		data:    make(map[string]interface{}),
		maxSize: maxSize,
	}
}

// Dodavanje novog para kljuc-vrednost u kes
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Proverava se da li je pun kes
	if len(c.data) >= c.maxSize {
		// Uklanjanje najredje upotrebljivanog(prvi u mapi)
		for k := range c.data {
			delete(c.data, k)
			break
		}
	}

	// Postavlja par kljuc-vrednsot
	c.data[key] = value
}

// Vraca vrednost iz kesa pridruzenu uz kljuc iz argumenta funkcije
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Proverava da li kljuc postoji u kesu
	value, ok := c.data[key]
	return value, ok
}

func main() {

	cache := NewCache(3)

	cache.Set("key1", "value1")
	cache.Set("key2", "value2")
	cache.Set("key3", "value3")

	value, exists := cache.Get("key1")
	if exists {
		fmt.Println("Value for key1:", value)
	} else {
		fmt.Println("Key1 not found in the cache")
	}

	value, exists = cache.Get("key4")
	if exists {
		fmt.Println("Value for key4:", value)
	} else {
		fmt.Println("Key4 not found in the cache")
	}
}
