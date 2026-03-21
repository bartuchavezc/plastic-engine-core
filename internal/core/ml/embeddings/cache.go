package embeddings

import (
	"sync"
	"sync/atomic"
	"time"
)

// embeddingLRU is a CLOCK-eviction cache for embedding vectors with TTL expiry.
// Reads are lock-free (RLock only, no write-lock promotion).
type embeddingLRU struct {
	mu      sync.RWMutex
	entries map[string]*embLRUEntry
	keys    []string // insertion-order ring for CLOCK sweep
	hand    int
	maxSize int
	ttl     time.Duration
}

type embLRUEntry struct {
	key      string
	vec      []float32
	created  time.Time
	accessed atomic.Int32
}

func newEmbeddingLRU(maxSize, ttlSecs int) *embeddingLRU {
	return &embeddingLRU{
		entries: make(map[string]*embLRUEntry, maxSize),
		keys:    make([]string, 0, maxSize),
		maxSize: maxSize,
		ttl:     time.Duration(ttlSecs) * time.Second,
	}
}

func (c *embeddingLRU) get(term string) ([]float32, bool) {
	c.mu.RLock()
	e, ok := c.entries[term]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
	if time.Since(e.created) > c.ttl {
		c.mu.RUnlock()
		c.mu.Lock()
		if e2, ok2 := c.entries[term]; ok2 && time.Since(e2.created) > c.ttl {
			delete(c.entries, term)
			c.removeKey(term)
		}
		c.mu.Unlock()
		return nil, false
	}
	e.accessed.Store(1)
	c.mu.RUnlock()
	return e.vec, true
}

func (c *embeddingLRU) put(term string, vec []float32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.entries[term]; ok {
		existing.vec = vec
		existing.created = time.Now()
		existing.accessed.Store(1)
		return
	}

	for len(c.entries) >= c.maxSize && len(c.keys) > 0 {
		c.clockEvictOne()
	}

	e := &embLRUEntry{
		key:     term,
		vec:     vec,
		created: time.Now(),
	}
	e.accessed.Store(1)
	c.entries[term] = e
	c.keys = append(c.keys, term)
}

func (c *embeddingLRU) clockEvictOne() {
	n := len(c.keys)
	if n == 0 {
		return
	}
	for i := 0; i < 2*n; i++ {
		if c.hand >= n {
			c.hand = 0
		}
		key := c.keys[c.hand]
		e, ok := c.entries[key]
		if !ok {
			c.keys[c.hand] = c.keys[n-1]
			c.keys = c.keys[:n-1]
			n--
			if c.hand >= n && n > 0 {
				c.hand = 0
			}
			continue
		}
		if e.accessed.CompareAndSwap(1, 0) {
			c.hand++
			continue
		}
		delete(c.entries, key)
		c.keys[c.hand] = c.keys[n-1]
		c.keys = c.keys[:n-1]
		if c.hand >= len(c.keys) && len(c.keys) > 0 {
			c.hand = 0
		}
		return
	}
	if c.hand >= len(c.keys) {
		c.hand = 0
	}
	if len(c.keys) > 0 {
		key := c.keys[c.hand]
		delete(c.entries, key)
		c.keys[c.hand] = c.keys[len(c.keys)-1]
		c.keys = c.keys[:len(c.keys)-1]
		if c.hand >= len(c.keys) && len(c.keys) > 0 {
			c.hand = 0
		}
	}
}

func (c *embeddingLRU) removeKey(key string) {
	for i, k := range c.keys {
		if k == key {
			c.keys[i] = c.keys[len(c.keys)-1]
			c.keys = c.keys[:len(c.keys)-1]
			if c.hand >= len(c.keys) && len(c.keys) > 0 {
				c.hand = 0
			}
			return
		}
	}
}
