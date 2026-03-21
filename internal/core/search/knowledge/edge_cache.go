package knowledge

import (
	"sync"
	"sync/atomic"
	"time"
)

// edgeLRU is a CLOCK-eviction cache for GetEdges results.
// Reads are lock-free (RLock only, no write-lock promotion).
// Uses an atomic "accessed" bit per entry for CLOCK sweep on eviction.
type edgeLRU struct {
	mu      sync.RWMutex
	entries map[string]*lruEntry
	keys    []string // insertion-order ring for CLOCK sweep
	hand    int      // CLOCK hand position
	maxSize int
	ttl     time.Duration
}

type lruEntry struct {
	key      string
	edges    []Edge
	created  time.Time
	accessed atomic.Int32 // CLOCK bit: set on access, cleared on sweep
}

func newEdgeLRU(maxSize int) *edgeLRU {
	return &edgeLRU{
		entries: make(map[string]*lruEntry, maxSize),
		keys:    make([]string, 0, maxSize),
		maxSize: maxSize,
		ttl:     60 * time.Second,
	}
}

func (c *edgeLRU) get(node string) ([]Edge, bool) {
	c.mu.RLock()
	e, ok := c.entries[node]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
	if time.Since(e.created) > c.ttl {
		c.mu.RUnlock()
		// Lazy expiry — remove under write lock
		c.mu.Lock()
		if e2, ok2 := c.entries[node]; ok2 && time.Since(e2.created) > c.ttl {
			delete(c.entries, node)
			c.removeKey(node)
		}
		c.mu.Unlock()
		return nil, false
	}
	// Mark accessed (atomic, no write lock needed)
	e.accessed.Store(1)
	c.mu.RUnlock()
	return e.edges, true
}

func (c *edgeLRU) put(node string, edges []Edge) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.entries[node]; ok {
		existing.edges = edges
		existing.created = time.Now()
		existing.accessed.Store(1)
		return
	}

	// Evict if at capacity using CLOCK sweep
	for len(c.entries) >= c.maxSize && len(c.keys) > 0 {
		c.clockEvictOne()
	}

	e := &lruEntry{
		key:     node,
		edges:   edges,
		created: time.Now(),
	}
	e.accessed.Store(1)
	c.entries[node] = e
	c.keys = append(c.keys, node)
}

// clockEvictOne runs the CLOCK hand until it finds an entry to evict.
// Must be called with c.mu held (write lock).
func (c *edgeLRU) clockEvictOne() {
	n := len(c.keys)
	if n == 0 {
		return
	}
	// Sweep at most 2*n to guarantee finding a victim
	for i := 0; i < 2*n; i++ {
		if c.hand >= n {
			c.hand = 0
		}
		key := c.keys[c.hand]
		e, ok := c.entries[key]
		if !ok {
			// Stale key — remove from ring
			c.keys[c.hand] = c.keys[n-1]
			c.keys = c.keys[:n-1]
			n--
			if c.hand >= n && n > 0 {
				c.hand = 0
			}
			continue
		}
		if e.accessed.CompareAndSwap(1, 0) {
			// Recently accessed — give it another chance
			c.hand++
			continue
		}
		// Victim found — evict
		delete(c.entries, key)
		c.keys[c.hand] = c.keys[n-1]
		c.keys = c.keys[:n-1]
		if c.hand >= len(c.keys) && len(c.keys) > 0 {
			c.hand = 0
		}
		return
	}
	// Fallback: evict the entry at current hand position
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

func (c *edgeLRU) removeKey(key string) {
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

func (c *edgeLRU) invalidate(node string) {
	c.mu.Lock()
	if _, ok := c.entries[node]; ok {
		delete(c.entries, node)
		c.removeKey(node)
	}
	c.mu.Unlock()
}
