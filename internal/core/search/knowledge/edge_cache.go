package knowledge

import (
	"sync"
	"time"
)

// edgeLRU is a simple LRU cache for GetEdges results.
// Uses sync.RWMutex for concurrent read access with single-writer invalidation.
type edgeLRU struct {
	mu       sync.RWMutex
	entries  map[string]*lruEntry
	order    lruList // doubly-linked list for eviction order
	maxSize  int
	ttl      time.Duration
}

type lruEntry struct {
	key     string
	edges   []Edge
	created time.Time
	prev    *lruEntry
	next    *lruEntry
}

// lruList is a doubly-linked list for LRU eviction.
type lruList struct {
	head *lruEntry
	tail *lruEntry
	len  int
}

func (l *lruList) pushFront(e *lruEntry) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
	l.len++
}

func (l *lruList) remove(e *lruEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
	l.len--
}

func (l *lruList) removeTail() *lruEntry {
	if l.tail == nil {
		return nil
	}
	e := l.tail
	l.remove(e)
	return e
}

func newEdgeLRU(maxSize int) *edgeLRU {
	return &edgeLRU{
		entries: make(map[string]*lruEntry, maxSize),
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
		// Expired — remove under write lock
		c.mu.Lock()
		// Re-check after upgrading lock
		if e2, ok2 := c.entries[node]; ok2 && time.Since(e2.created) > c.ttl {
			c.order.remove(e2)
			delete(c.entries, node)
		}
		c.mu.Unlock()
		return nil, false
	}
	c.mu.RUnlock()

	// Promote to front (write lock)
	c.mu.Lock()
	c.order.remove(e)
	c.order.pushFront(e)
	c.mu.Unlock()

	return e.edges, true
}

func (c *edgeLRU) put(node string, edges []Edge) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.entries[node]; ok {
		c.order.remove(existing)
		existing.edges = edges
		existing.created = time.Now()
		c.order.pushFront(existing)
		return
	}

	e := &lruEntry{
		key:     node,
		edges:   edges,
		created: time.Now(),
	}
	c.entries[node] = e
	c.order.pushFront(e)

	for c.order.len > c.maxSize {
		evicted := c.order.removeTail()
		if evicted != nil {
			delete(c.entries, evicted.key)
		}
	}
}

func (c *edgeLRU) invalidate(node string) {
	c.mu.Lock()
	if e, ok := c.entries[node]; ok {
		c.order.remove(e)
		delete(c.entries, node)
	}
	c.mu.Unlock()
}
