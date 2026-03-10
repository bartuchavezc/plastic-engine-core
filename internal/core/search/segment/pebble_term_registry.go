package segment

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"

	pebblestore "plastic-engine-core/internal/adapters/storage/pebble"
)

// Key namespace constants for PebbleTermRegistry.
// Using uppercase letters + \x00 as namespace separator to avoid collisions
// with user-provided field/term data (which can contain arbitrary bytes except \x00).
const (
	// ptrTermPrefix is the prefix for term→termID entries.
	// Full key: "T\x00" + field + "\x00" + term → termID string
	ptrTermPrefix = "T\x00"

	// ptrDFPrefix is the prefix for DF counter entries.
	// Full key: "D\x00" + termID → int64 via MergeInt64
	ptrDFPrefix = "D\x00"

	// ptrMetaCounter stores the current term ID counter (int64, big-endian).
	ptrMetaCounter = "M\x00counter"

	// ptrMetaTotalDocs stores the total indexed document count.
	ptrMetaTotalDocs = "M\x00total_docs"

	// ptrMetaCount stores the total number of unique terms.
	ptrMetaCount = "M\x00count"
)

// PebbleTermRegistry is a term registry backed by a Pebble LSM-tree database.
//
// Architecture:
//   - All term lookups (Get / GetOrCreate) hit Pebble directly.
//     Pebble's MemTable provides O(log N) concurrent reads/writes without
//     any user-space RWMutex. For an established vocabulary (>99% of calls),
//     the fast path is a single Pebble.Get with no user-space lock.
//   - New-term creation is serialized by a minimal newTermMu mutex, held only
//     for the microsecond it takes to atomically write one Pebble batch.
//   - DF increments use Pebble's merge operator (MergeInt64), which is
//     lock-free at the MemTable level — no user-space synchronization needed.
//   - Metadata (counter, termCount, totalDocs) is persisted to Pebble so the
//     registry survives node restarts without replaying a WAL.
//
// Compared to CacheFSTRegistry:
//   - Eliminates the global RWMutex that blocked all concurrent readers during
//     GC or write-lock upgrades.
//   - Eliminates the custom WAL, FST build/merge cycle, and dfCache mutex.
//   - Pebble's background compaction replaces the manual doMerge() goroutine.
type PebbleTermRegistry struct {
	db *pebblestore.PebbleStore

	// termCache is a read-through cache: field+"\x00"+term → termID.
	// Uses sync.Map which is optimized for the "append-mostly" pattern:
	// many concurrent reads, infrequent writes, keys never deleted.
	// Unlike RWMutex+map, sync.Map doesn't block readers during writes,
	// eliminating the contention that caused 60μs/call with 4 shards.
	termCache sync.Map // string → string

	// dfBuffer accumulates DF deltas in memory. Flushed to Pebble periodically
	// via FlushDF() or on Close/Sync. Avoids 5K+ individual Pebble.Merge calls per batch.
	dfMu     sync.Mutex
	dfBuffer map[string]int64

	// newTermMu serializes new-term creation to prevent duplicate term IDs.
	// Only acquired when a term is NOT found in cache or Pebble (the slow path).
	newTermMu sync.Mutex

	// termIDCounter is the monotonic allocator for term IDs.
	// Persisted to Pebble on every new-term write and on Close/Sync.
	termIDCounter atomic.Int64

	// totalDocs is the global document count for BM25 scoring.
	// Kept in memory for fast access; persisted to Pebble on Close/Sync.
	totalDocs atomic.Int64

	// termCount is the total number of unique terms (for GetTermCount).
	// Updated atomically on each new term creation.
	termCount atomic.Int64
}

// PebbleRegistryConfig configures the PebbleTermRegistry.
type PebbleRegistryConfig struct {
	// DataDir is the directory where the Pebble database will be created.
	// A subdirectory "pebble" will be created inside DataDir.
	DataDir string
}

// NewPebbleTermRegistry opens or creates a Pebble-backed term registry.
func NewPebbleTermRegistry(cfg PebbleRegistryConfig) (*PebbleTermRegistry, error) {
	dbPath := filepath.Join(cfg.DataDir, "pebble")
	db, err := pebblestore.NewPebbleStoreWithConfig(dbPath, pebblestore.StoreConfig{
		SyncWrites:               false, // NoSync — WAL still provides crash recovery
		MaxConcurrentCompactions: 1,     // Single compaction goroutine per registry
		MemTableSize:             16 * 1024 * 1024, // 16 MB — keeps Pebble footprint small (4 shards × 2 memtables × 16MB = 128MB total vs 512MB with 64MB)
	})
	if err != nil {
		return nil, fmt.Errorf("open pebble term registry at %s: %w", dbPath, err)
	}

	r := &PebbleTermRegistry{
		db:       db,
		dfBuffer: make(map[string]int64, 1024),
	}
	r.loadMeta()
	return r, nil
}

// termKey builds the Pebble key for a field+term lookup.
func (r *PebbleTermRegistry) termKey(field, term string) string {
	return ptrTermPrefix + field + "\x00" + term
}

// fieldPrefix returns the Pebble key prefix for all terms in a given field.
// Used for prefix scans: PrefixScan(fieldPrefix(field)) iterates all terms for the field.
func (r *PebbleTermRegistry) fieldPrefix(field string) string {
	return ptrTermPrefix + field + "\x00"
}

// dfKey builds the Pebble key for a term's DF counter.
func (r *PebbleTermRegistry) dfKey(termID string) string {
	return ptrDFPrefix + termID
}

// Get retrieves a term ID if it exists.
// Checks in-memory cache first, falls back to Pebble.
func (r *PebbleTermRegistry) Get(_ context.Context, field, term string) (string, bool) {
	cacheKey := field + "\x00" + term

	if v, ok := r.termCache.Load(cacheKey); ok {
		return v.(string), true
	}

	termID, err := r.db.Get(r.termKey(field, term))
	if err != nil {
		return "", false
	}

	r.termCache.Store(cacheKey, termID)
	return termID, true
}

// GetOrCreate retrieves an existing term ID or creates a new one.
//
// Fast path (existing terms, >99% of calls during indexing):
//   Single Pebble.Get with no user-space lock.
//
// Slow path (new terms):
//   Acquires newTermMu, double-checks, then writes an atomic Pebble batch
//   containing the term key, updated counter, and updated term count.
func (r *PebbleTermRegistry) GetOrCreate(_ context.Context, field, term string) (string, error) {
	cacheKey := field + "\x00" + term

	// Fast path: check in-memory cache (lock-free read via sync.Map).
	if v, ok := r.termCache.Load(cacheKey); ok {
		return v.(string), nil
	}

	key := r.termKey(field, term)

	// Medium path: check Pebble (term exists but not yet cached, e.g. after restart).
	if termID, err := r.db.Get(key); err == nil {
		r.termCache.Store(cacheKey, termID)
		return termID, nil
	}

	// Slow path: new term creation.
	r.newTermMu.Lock()
	defer r.newTermMu.Unlock()

	// Double-check cache (another goroutine may have created this term).
	if v, ok := r.termCache.Load(cacheKey); ok {
		return v.(string), nil
	}

	counter := r.termIDCounter.Add(1)
	termID := formatTermID(uint64(counter))
	newCount := r.termCount.Add(1)

	batch := r.db.NewBatch()
	_ = batch.Set(key, termID)
	_ = batch.SetInt64(ptrMetaCounter, counter)
	_ = batch.SetInt64(ptrMetaCount, newCount)
	if err := batch.Commit(); err != nil {
		r.termIDCounter.Add(-1)
		r.termCount.Add(-1)
		batch.Close()
		return "", fmt.Errorf("commit new term %s.%s: %w", field, term, err)
	}
	batch.Close()

	r.termCache.Store(cacheKey, termID)
	return termID, nil
}

// TermLookup is an input to GetOrCreateBatch.
type TermLookup struct {
	Field string
	Term  string
}

// GetOrCreateBatch resolves term IDs for a batch of field+term pairs.
// Much faster than calling GetOrCreate in a loop: cache misses skip
// individual Pebble reads and go directly to a single batch write.
//
// The sync.Map cache is the source of truth for all terms created since startup.
// On restart, loadMeta + warmupCache populate the cache from Pebble, so a
// sync.Map miss always means "genuinely new term" — no Pebble.Get needed.
//
// Returns a map[cacheKey]termID where cacheKey = field + "\x00" + term.
func (r *PebbleTermRegistry) GetOrCreateBatch(terms []TermLookup) (map[string]string, error) {
	result := make(map[string]string, len(terms))

	// Phase 1: check sync.Map for all terms (lock-free, ~50ns each).
	var cacheMisses []int // indices into terms slice
	for i := range terms {
		cacheKey := terms[i].Field + "\x00" + terms[i].Term
		if v, ok := r.termCache.Load(cacheKey); ok {
			result[cacheKey] = v.(string)
		} else {
			cacheMisses = append(cacheMisses, i)
		}
	}

	if len(cacheMisses) == 0 {
		return result, nil
	}

	// Phase 2: assign term IDs under lock, then persist to Pebble outside lock.
	// The lock only protects ID allocation + sync.Map population (~μs).
	// Pebble batch commit (~ms) runs outside the lock so other shards don't wait.
	r.newTermMu.Lock()

	// Double-check: another goroutine may have created some of these terms
	// while we were waiting for the lock.
	var reallyNew []int
	for _, idx := range cacheMisses {
		cacheKey := terms[idx].Field + "\x00" + terms[idx].Term
		if v, ok := r.termCache.Load(cacheKey); ok {
			result[cacheKey] = v.(string)
		} else {
			reallyNew = append(reallyNew, idx)
		}
	}

	if len(reallyNew) == 0 {
		r.newTermMu.Unlock()
		return result, nil
	}

	// Allocate all term IDs at once (atomic).
	startCounter := r.termIDCounter.Add(int64(len(reallyNew)))
	baseCounter := startCounter - int64(len(reallyNew)) + 1
	r.termCount.Add(int64(len(reallyNew)))

	// Populate sync.Map immediately so other goroutines see these terms.
	// This is the critical section — must happen before unlock.
	for i, idx := range reallyNew {
		counter := baseCounter + int64(i)
		termID := formatTermID(uint64(counter))
		cacheKey := terms[idx].Field + "\x00" + terms[idx].Term
		r.termCache.Store(cacheKey, termID)
		result[cacheKey] = termID
	}

	r.newTermMu.Unlock()

	// Persist to Pebble outside the lock. This is safe because:
	// - Term IDs are already allocated (atomic counter)
	// - sync.Map already has the mappings (other goroutines won't re-create)
	// - Pebble is thread-safe for concurrent batch commits
	// - On crash before commit, warmupCache will miss these terms, but
	//   the WAL replay will re-create them with new IDs (acceptable).
	batch := r.db.NewBatch()
	for i, idx := range reallyNew {
		counter := baseCounter + int64(i)
		termID := formatTermID(uint64(counter))
		key := r.termKey(terms[idx].Field, terms[idx].Term)
		_ = batch.Set(key, termID)
	}
	_ = batch.SetInt64(ptrMetaCounter, startCounter)
	_ = batch.SetInt64(ptrMetaCount, r.termCount.Load())

	if err := batch.Commit(); err != nil {
		batch.Close()
		return nil, fmt.Errorf("persist %d new terms: %w", len(reallyNew), err)
	}
	batch.Close()

	return result, nil
}

// CreateAlias creates a key for newTerm that resolves to the same termID as an existing term.
// Aliases share the same term ID; the term count and ID counter are not incremented.
func (r *PebbleTermRegistry) CreateAlias(field, newTerm, existingTermID string) error {
	key := r.termKey(field, newTerm)

	// Fast check without lock.
	if _, err := r.db.Get(key); err == nil {
		return nil // already exists
	}

	// Serialize with newTermMu to prevent a race between check and write.
	r.newTermMu.Lock()
	defer r.newTermMu.Unlock()

	if _, err := r.db.Get(key); err == nil {
		return nil
	}

	return r.db.Set(key, existingTermID)
}

// GetDF returns the document frequency for a term ID.
// Combines the persisted Pebble value with any buffered increments.
func (r *PebbleTermRegistry) GetDF(termID string) int64 {
	df, _ := r.db.GetInt64(r.dfKey(termID))

	r.dfMu.Lock()
	df += r.dfBuffer[termID]
	r.dfMu.Unlock()

	return df
}

// IncrementDF buffers a DF increment in memory.
// Call FlushDF() to persist buffered increments to Pebble.
func (r *PebbleTermRegistry) IncrementDF(termID string, delta int64) {
	r.dfMu.Lock()
	r.dfBuffer[termID] += delta
	r.dfMu.Unlock()
}

// IncrementDFBatch merges an entire map of DF deltas under a single lock acquisition.
// Avoids N individual Lock/Unlock cycles when processing a batch.
func (r *PebbleTermRegistry) IncrementDFBatch(deltas map[string]int64) {
	r.dfMu.Lock()
	for termID, delta := range deltas {
		r.dfBuffer[termID] += delta
	}
	r.dfMu.Unlock()
}

// FlushDF persists all buffered DF increments to Pebble in a single batch.
func (r *PebbleTermRegistry) FlushDF() error {
	r.dfMu.Lock()
	if len(r.dfBuffer) == 0 {
		r.dfMu.Unlock()
		return nil
	}
	// Swap buffer so we don't hold the lock during Pebble write.
	buf := r.dfBuffer
	r.dfBuffer = make(map[string]int64, len(buf))
	r.dfMu.Unlock()

	batch := r.db.NewBatch()
	for termID, delta := range buf {
		_ = batch.MergeInt64(r.dfKey(termID), delta)
	}
	err := batch.Commit()
	batch.Close()
	return err
}

// GetTotalDocs returns the total number of indexed documents.
func (r *PebbleTermRegistry) GetTotalDocs() int64 {
	return r.totalDocs.Load()
}

// IncrementTotalDocs increments the total document count.
func (r *PebbleTermRegistry) IncrementTotalDocs(delta int64) {
	r.totalDocs.Add(delta)
}

// GetTermsWithPrefix returns term IDs for all terms in a field that start with prefix.
func (r *PebbleTermRegistry) GetTermsWithPrefix(_ context.Context, field, prefix string) []string {
	searchPrefix := r.fieldPrefix(field) + prefix
	kvs, err := r.db.PrefixScan(searchPrefix)
	if err != nil {
		return nil
	}
	result := make([]string, len(kvs))
	for i, kv := range kvs {
		result[i] = kv.Value
	}
	return result
}

// GetTermCount returns the approximate number of unique terms in the registry.
func (r *PebbleTermRegistry) GetTermCount() int64 {
	return r.termCount.Load()
}

// Sync persists totalDocs and buffered DF increments to Pebble.
func (r *PebbleTermRegistry) Sync() error {
	_ = r.FlushDF()
	return r.db.SetInt64(ptrMetaTotalDocs, r.totalDocs.Load())
}

// Close persists state and closes the Pebble database.
func (r *PebbleTermRegistry) Close() error {
	_ = r.Sync()
	return r.db.Close()
}

// loadMeta loads persisted counters from Pebble on startup
// and warms the term cache so sync.Map misses always mean "new term".
func (r *PebbleTermRegistry) loadMeta() {
	if counter, err := r.db.GetInt64(ptrMetaCounter); err == nil {
		r.termIDCounter.Store(counter)
	}
	if count, err := r.db.GetInt64(ptrMetaCount); err == nil {
		r.termCount.Store(count)
	}
	if totalDocs, err := r.db.GetInt64(ptrMetaTotalDocs); err == nil {
		r.totalDocs.Store(totalDocs)
	}

	// Warm up term cache: load all term→termID mappings from Pebble.
	// This ensures sync.Map has every term from prior sessions, so
	// GetOrCreateBatch can skip individual Pebble.Get on cache miss.
	r.warmupCache()
}

// warmupCache loads all term→termID entries from Pebble into sync.Map.
func (r *PebbleTermRegistry) warmupCache() {
	kvs, err := r.db.PrefixScan(ptrTermPrefix)
	if err != nil {
		return
	}
	prefixLen := len(ptrTermPrefix)
	for _, kv := range kvs {
		// Key format: ptrTermPrefix + field + "\x00" + term
		// Cache key: field + "\x00" + term (strip prefix)
		if len(kv.Key) <= prefixLen {
			continue
		}
		cacheKey := kv.Key[prefixLen:]
		r.termCache.Store(cacheKey, kv.Value)
	}
}

// ListTerms returns all terms, optionally filtered by field, with pagination.
func (r *PebbleTermRegistry) ListTerms(_ context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	var searchPrefix string
	if field != "" {
		searchPrefix = r.fieldPrefix(field)
	} else {
		searchPrefix = ptrTermPrefix
	}

	kvs, err := r.db.PrefixScan(searchPrefix)
	if err != nil {
		return nil, 0, err
	}

	termPrefixLen := len(ptrTermPrefix)
	entries := make([]TermEntry, 0, len(kvs))
	for _, kv := range kvs {
		// Key format: ptrTermPrefix + field + "\x00" + term
		rest := kv.Key[termPrefixLen:]
		// Locate the separator between field and term.
		sep := -1
		for i := 0; i < len(rest); i++ {
			if rest[i] == 0 {
				sep = i
				break
			}
		}
		if sep < 0 {
			continue
		}
		entryField := rest[:sep]
		entryTerm := rest[sep+1:]
		termID := kv.Value
		entries = append(entries, TermEntry{
			Field:  entryField,
			Term:   entryTerm,
			TermID: termID,
			DF:     r.GetDF(termID),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Field != entries[j].Field {
			return entries[i].Field < entries[j].Field
		}
		return entries[i].Term < entries[j].Term
	})

	total := int64(len(entries))

	if offset > 0 && offset < len(entries) {
		entries = entries[offset:]
	} else if offset >= len(entries) {
		return nil, total, nil
	}
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, total, nil
}

// ListTermsByPrefix returns terms matching a prefix within a field.
// Results are returned in lexicographic order (Pebble's natural order).
func (r *PebbleTermRegistry) ListTermsByPrefix(_ context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	searchPrefix := r.fieldPrefix(field) + prefix

	var kvs []pebblestore.KeyValue
	var err error
	if limit > 0 {
		kvs, err = r.db.PrefixScanLimit(searchPrefix, limit)
	} else {
		kvs, err = r.db.PrefixScan(searchPrefix)
	}
	if err != nil {
		return nil, err
	}

	fieldPfxLen := len(r.fieldPrefix(field))
	entries := make([]TermEntry, 0, len(kvs))
	for _, kv := range kvs {
		if len(kv.Key) <= fieldPfxLen {
			continue
		}
		term := kv.Key[fieldPfxLen:]
		termID := kv.Value
		entries = append(entries, TermEntry{
			Field:  field,
			Term:   term,
			TermID: termID,
			DF:     r.GetDF(termID),
		})
	}

	// Pebble returns keys in lexicographic order; entries are already sorted by term.
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// ListTermsByFuzzy returns terms within a given Levenshtein distance of query.
func (r *PebbleTermRegistry) ListTermsByFuzzy(_ context.Context, field, query string, maxDistance, limit int) ([]TermEntry, error) {
	if maxDistance < 1 {
		maxDistance = 1
	}
	if maxDistance > 3 {
		maxDistance = 3
	}

	kvs, err := r.db.PrefixScan(r.fieldPrefix(field))
	if err != nil {
		return nil, err
	}

	fieldPfxLen := len(r.fieldPrefix(field))
	var entries []TermEntry
	for _, kv := range kvs {
		if len(kv.Key) <= fieldPfxLen {
			continue
		}
		term := kv.Key[fieldPfxLen:]
		if DamerauLevenshteinDistance(query, term) <= maxDistance {
			termID := kv.Value
			entries = append(entries, TermEntry{
				Field:  field,
				Term:   term,
				TermID: termID,
				DF:     r.GetDF(termID),
			})
			if limit > 0 && len(entries) >= limit {
				break
			}
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// ListTermsByRegex returns terms matching a regular expression within a field.
func (r *PebbleTermRegistry) ListTermsByRegex(_ context.Context, field, pattern string, limit int) ([]TermEntry, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}

	kvs, err := r.db.PrefixScan(r.fieldPrefix(field))
	if err != nil {
		return nil, err
	}

	fieldPfxLen := len(r.fieldPrefix(field))
	var entries []TermEntry
	for _, kv := range kvs {
		if len(kv.Key) <= fieldPfxLen {
			continue
		}
		term := kv.Key[fieldPfxLen:]
		if re.MatchString(term) {
			termID := kv.Value
			entries = append(entries, TermEntry{
				Field:  field,
				Term:   term,
				TermID: termID,
				DF:     r.GetDF(termID),
			})
			if limit > 0 && len(entries) >= limit {
				break
			}
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// Stats returns a snapshot of registry statistics.
func (r *PebbleTermRegistry) Stats() RegistryStats {
	return RegistryStats{
		TotalTermCount: r.termCount.Load(),
		TotalDocs:      r.totalDocs.Load(),
	}
}

// Compile-time assertion that PebbleTermRegistry implements TermRegistry.
var _ TermRegistry = (*PebbleTermRegistry)(nil)
