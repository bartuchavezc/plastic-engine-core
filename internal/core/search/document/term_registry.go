package document

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"plastic-engine-core/internal/adapters/storage/pebble"
)

// TermEntry holds metadata for a registered term.
// Note: DF (document frequency) is stored separately using the merge operator
// to enable atomic concurrent increments without read-modify-write races.
type TermEntry struct {
	TermID    string    `json:"term_id"`
	CreatedAt time.Time `json:"created_at"`
}

// termDFKey returns the key for storing document frequency separately.
// This enables atomic increments using Pebble's merge operator.
func termDFKey(field, term string) string {
	return pebble.TermRegistryKey(field, term) + ":df"
}

// TermRegistryStore defines the storage operations required by TermRegistry.
type TermRegistryStore interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	GetInt64(key string) (int64, error)
	// MergeInt64 atomically adds delta to the int64 at key.
	MergeInt64(key string, delta int64) error
}

// DefaultTermCacheSize is the default number of term entries to cache in memory.
const DefaultTermCacheSize = 10000

// TermRegistry manages the mapping from (field, term) to term_id.
// This enables O(1) term renames and efficient updates.
// Includes an in-memory LRU cache to avoid repeated Pebble reads for common terms.
type TermRegistry struct {
	store TermRegistryStore

	// In-memory cache to avoid repeated Pebble reads for common terms.
	// Uses a simple map with mutex for thread safety.
	// For high-traffic terms like "the", "a", "is", this eliminates I/O.
	cacheMu   sync.RWMutex
	cache     map[string]TermEntry
	cacheKeys []string // For LRU eviction order
	cacheSize int
}

// NewTermRegistry creates a new TermRegistry backed by the given store.
func NewTermRegistry(store TermRegistryStore) *TermRegistry {
	return NewTermRegistryWithCache(store, DefaultTermCacheSize)
}

// NewTermRegistryWithCache creates a new TermRegistry with a custom cache size.
func NewTermRegistryWithCache(store TermRegistryStore, cacheSize int) *TermRegistry {
	if cacheSize <= 0 {
		cacheSize = DefaultTermCacheSize
	}
	return &TermRegistry{
		store:     store,
		cache:     make(map[string]TermEntry, cacheSize),
		cacheKeys: make([]string, 0, cacheSize),
		cacheSize: cacheSize,
	}
}

// GetOrCreate retrieves an existing term entry or creates a new one.
// If the term doesn't exist, a new entry is created.
// Note: DF is stored separately and managed via IncrementDF/DecrementDF.
// Uses an in-memory LRU cache to avoid repeated Pebble reads for common terms.
func (r *TermRegistry) GetOrCreate(_ context.Context, field, term string) (TermEntry, error) {
	key := pebble.TermRegistryKey(field, term)

	// Fast path: check cache first (read lock)
	r.cacheMu.RLock()
	if entry, ok := r.cache[key]; ok {
		r.cacheMu.RUnlock()
		return entry, nil
	}
	r.cacheMu.RUnlock()

	// Slow path: read from Pebble
	raw, err := r.store.Get(key)
	if err == nil {
		var entry TermEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			return TermEntry{}, fmt.Errorf("decode term entry: %w", err)
		}
		// Cache the loaded entry
		r.cacheEntry(key, entry)
		return entry, nil
	}

	if !pebble.IsNotFound(err) {
		return TermEntry{}, fmt.Errorf("get term entry: %w", err)
	}

	// Create new entry (DF stored separately)
	entry := TermEntry{
		TermID:    pebble.GenerateTermID(field, term),
		CreatedAt: time.Now().UTC(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return TermEntry{}, fmt.Errorf("encode term entry: %w", err)
	}

	if err := r.store.Set(key, string(data)); err != nil {
		return TermEntry{}, fmt.Errorf("save term entry: %w", err)
	}

	// Cache the new entry
	r.cacheEntry(key, entry)

	return entry, nil
}

// cacheEntry adds an entry to the cache, evicting oldest entries if necessary.
func (r *TermRegistry) cacheEntry(key string, entry TermEntry) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// If already cached, no need to re-add
	if _, ok := r.cache[key]; ok {
		return
	}

	// Evict oldest entries if cache is full (simple FIFO eviction)
	if len(r.cacheKeys) >= r.cacheSize {
		// Evict the oldest 10% of entries
		evictCount := r.cacheSize / 10
		if evictCount < 1 {
			evictCount = 1
		}
		for i := 0; i < evictCount && len(r.cacheKeys) > 0; i++ {
			oldKey := r.cacheKeys[0]
			r.cacheKeys = r.cacheKeys[1:]
			delete(r.cache, oldKey)
		}
	}

	r.cache[key] = entry
	r.cacheKeys = append(r.cacheKeys, key)
}

// Get retrieves a term entry if it exists.
// Returns (entry, true, nil) if found, (empty, false, nil) if not found.
// Uses an in-memory cache for common terms.
func (r *TermRegistry) Get(_ context.Context, field, term string) (TermEntry, bool, error) {
	key := pebble.TermRegistryKey(field, term)

	// Fast path: check cache first (read lock)
	r.cacheMu.RLock()
	if entry, ok := r.cache[key]; ok {
		r.cacheMu.RUnlock()
		return entry, true, nil
	}
	r.cacheMu.RUnlock()

	// Slow path: read from Pebble
	raw, err := r.store.Get(key)
	if err != nil {
		if pebble.IsNotFound(err) {
			return TermEntry{}, false, nil
		}
		return TermEntry{}, false, fmt.Errorf("get term entry: %w", err)
	}

	var entry TermEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return TermEntry{}, false, fmt.Errorf("decode term entry: %w", err)
	}

	// Cache the loaded entry
	r.cacheEntry(key, entry)

	return entry, true, nil
}

// GetDF retrieves the document frequency for a term.
// Returns 0 if the term has no DF recorded.
func (r *TermRegistry) GetDF(_ context.Context, field, term string) (int64, error) {
	dfKey := termDFKey(field, term)
	df, err := r.store.GetInt64(dfKey)
	if err != nil {
		if pebble.IsNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("get df: %w", err)
	}
	return df, nil
}

// IncrementDF atomically increases the document frequency for a term by 1.
// Uses Pebble's merge operator - safe for concurrent use without races.
func (r *TermRegistry) IncrementDF(_ context.Context, field, term string) error {
	dfKey := termDFKey(field, term)
	return r.store.MergeInt64(dfKey, 1)
}

// DecrementDF atomically decreases the document frequency for a term by 1.
// Uses Pebble's merge operator - safe for concurrent use without races.
// Note: The merge operator allows negative results; cleanup should handle zero/negative DF.
func (r *TermRegistry) DecrementDF(_ context.Context, field, term string) error {
	dfKey := termDFKey(field, term)
	return r.store.MergeInt64(dfKey, -1)
}

// BatchTermRegistry provides batch operations for term registration.
// It aggregates DF deltas in memory and commits them atomically using the merge operator.
type BatchTermRegistry struct {
	store    TermRegistryStore
	entries  map[string]TermEntry // key -> entry (for new terms)
	dfDeltas map[string]int64     // dfKey -> accumulated delta
}

// NewBatchTermRegistry creates a batch registry for bulk operations.
func NewBatchTermRegistry(store TermRegistryStore) *BatchTermRegistry {
	return &BatchTermRegistry{
		store:    store,
		entries:  make(map[string]TermEntry),
		dfDeltas: make(map[string]int64),
	}
}

// GetOrCreate retrieves or creates a term, caching in the batch.
func (b *BatchTermRegistry) GetOrCreate(_ context.Context, field, term string) (TermEntry, error) {
	key := pebble.TermRegistryKey(field, term)

	// Check batch cache first
	if entry, ok := b.entries[key]; ok {
		return entry, nil
	}

	// Try to load from store
	raw, err := b.store.Get(key)
	if err == nil {
		var entry TermEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			return TermEntry{}, fmt.Errorf("decode term entry: %w", err)
		}
		b.entries[key] = entry
		return entry, nil
	}

	if !pebble.IsNotFound(err) {
		return TermEntry{}, fmt.Errorf("get term entry: %w", err)
	}

	// Create new entry (DF stored separately)
	entry := TermEntry{
		TermID:    pebble.GenerateTermID(field, term),
		CreatedAt: time.Now().UTC(),
	}
	b.entries[key] = entry

	return entry, nil
}

// IncrementDF accumulates a +1 delta for the term's DF.
// The actual increment happens atomically on Commit using the merge operator.
func (b *BatchTermRegistry) IncrementDF(field, term string) error {
	dfKey := termDFKey(field, term)
	b.dfDeltas[dfKey]++
	return nil
}

// DecrementDF accumulates a -1 delta for the term's DF.
// The actual decrement happens atomically on Commit using the merge operator.
func (b *BatchTermRegistry) DecrementDF(field, term string) error {
	dfKey := termDFKey(field, term)
	b.dfDeltas[dfKey]--
	return nil
}

// Commit persists all cached entries and DF deltas to the store.
// DF deltas are applied atomically using the merge operator.
func (b *BatchTermRegistry) Commit() error {
	// Persist new term entries
	for key, entry := range b.entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode term entry: %w", err)
		}
		if err := b.store.Set(key, string(data)); err != nil {
			return fmt.Errorf("save term entry %s: %w", key, err)
		}
	}

	// Apply DF deltas atomically using merge operator
	for dfKey, delta := range b.dfDeltas {
		if delta == 0 {
			continue
		}
		if err := b.store.MergeInt64(dfKey, delta); err != nil {
			return fmt.Errorf("merge df delta %s: %w", dfKey, err)
		}
	}

	return nil
}

