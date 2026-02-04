package segment

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/vellum"
	pebbledb "github.com/cockroachdb/pebble"
)

// PebbleFSTRegistry is a two-layer term registry optimized for high write throughput
// and fast reads. It uses Pebble as the write layer and FST as the read layer.
//
// Architecture:
//   - Pebble: Receives all new term writes (fast, LSM-based)
//   - FST: Consolidated terms for fast lookups (immutable, rebuilt periodically)
//   - Background merge: Periodically merges Pebble terms into FST
//
// This design solves the CPU/Memory trade-off problem:
//   - No unbounded memory growth (Pebble manages its own memory)
//   - No expensive FST rebuilds blocking writes
//   - Fast reads from FST (O(key_length))
//   - Fast writes to Pebble (O(1) amortized)
type PebbleFSTRegistry struct {
	// Write layer (new terms go here)
	pebble *pebbledb.DB

	// Read layer (consolidated terms)
	fst   *vellum.FST
	fstMu sync.RWMutex // Only for atomic FST swap

	// Document frequency stored in Pebble with merge operator for atomic increments
	// Key format: "df:{termID}" -> int64

	// Global stats (atomic for lock-free access)
	totalDocs atomic.Int64

	// Term ID counter
	termIDCounter atomic.Uint64

	// Merge control
	merging       atomic.Bool
	lastMergeTime atomic.Int64 // Unix timestamp
	mergeConfig   MergeConfig

	// Dirty flag for metadata (saves CPU by batching metadata writes)
	metaDirty atomic.Bool

	// Paths
	dataDir    string
	fstPath    string
	pebblePath string

	// Background worker
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// MergeConfig configures when Pebble terms are merged into FST.
type MergeConfig struct {
	// MaxPebbleSizeMB triggers merge when Pebble exceeds this size.
	// This is the primary safety trigger to prevent memory overflow.
	// Default: 64MB
	MaxPebbleSizeMB int

	// MaxPebbleTermCount triggers merge when term count exceeds this.
	// Controls cardinality growth.
	// Default: 100,000
	MaxPebbleTermCount int

	// MaxTimeSinceLastMerge triggers merge after this duration.
	// Ensures FST stays fresh for optimal read performance.
	// Default: 10 minutes
	MaxTimeSinceLastMerge time.Duration

	// MinTimeBetweenMerges prevents merges too close together.
	// Default: 30 seconds
	MinTimeBetweenMerges time.Duration

	// MergeCheckInterval is how often to check merge triggers.
	// Default: 5 seconds
	MergeCheckInterval time.Duration
}

// DefaultMergeConfig returns sensible defaults for production use.
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		MaxPebbleSizeMB:       64,
		MaxPebbleTermCount:    100_000,
		MaxTimeSinceLastMerge: 10 * time.Minute,
		MinTimeBetweenMerges:  30 * time.Second,
		MergeCheckInterval:    5 * time.Second,
	}
}

// PebbleFSTConfig configures the registry.
type PebbleFSTConfig struct {
	DataDir     string
	MergeConfig MergeConfig
}

// Key prefixes for Pebble
const (
	prefixTerm = "t:" // t:{field}\x00{term} -> termID (uint64)
	prefixDF   = "d:" // d:{termID} -> df (int64, uses merge operator)
	prefixMeta = "m:" // m:counter -> termID counter, m:totaldocs -> total docs
)

// formatTermID converts a uint64 counter to a term ID string efficiently.
// Uses strconv.FormatUint which is 3-5x faster than fmt.Sprintf.
func formatTermID(counter uint64) string {
	return "t" + strconv.FormatUint(counter, 10)
}

// parseTermID extracts the counter from a term ID string.
// Returns 0 if the format is invalid.
func parseTermID(termID string) uint64 {
	if len(termID) < 2 || termID[0] != 't' {
		return 0
	}
	counter, _ := strconv.ParseUint(termID[1:], 10, 64)
	return counter
}

// NewPebbleFSTRegistry creates a new Pebble+FST term registry.
func NewPebbleFSTRegistry(config PebbleFSTConfig) (*PebbleFSTRegistry, error) {
	if config.MergeConfig.MaxPebbleSizeMB == 0 {
		config.MergeConfig = DefaultMergeConfig()
	}

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	pebblePath := filepath.Join(config.DataDir, "terms_pebble")
	fstPath := filepath.Join(config.DataDir, "terms.fst")

	// Open Pebble with merge operator for atomic DF increments
	pebbleOpts := &pebbledb.Options{
		Merger: &pebbledb.Merger{
			Name: "Int64AddMerger",
			Merge: func(key, value []byte) (pebbledb.ValueMerger, error) {
				return &int64Merger{sum: decodeInt64(value)}, nil
			},
		},
		// Optimize for write-heavy workload
		MemTableSize:                64 * 1024 * 1024, // 64MB memtable
		MaxConcurrentCompactions:    func() int { return 2 },
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       12,
		LBaseMaxBytes:               256 * 1024 * 1024,
		MaxOpenFiles:                500,
		MemTableStopWritesThreshold: 4,
	}

	pebble, err := pebbledb.Open(pebblePath, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	r := &PebbleFSTRegistry{
		pebble:      pebble,
		dataDir:     config.DataDir,
		fstPath:     fstPath,
		pebblePath:  pebblePath,
		mergeConfig: config.MergeConfig,
		closeCh:     make(chan struct{}),
	}

	// Load existing FST if present
	if err := r.loadFST(); err != nil {
		// FST doesn't exist yet, that's OK for fresh start
	}

	// Load metadata from Pebble
	if err := r.loadMeta(); err != nil {
		// Fresh start, metadata will be created
	}

	// Start background merge worker
	r.wg.Add(1)
	go r.mergeWorker()

	return r, nil
}

// int64Merger implements Pebble's ValueMerger for atomic int64 addition.
type int64Merger struct {
	sum int64
}

func (m *int64Merger) MergeNewer(value []byte) error {
	m.sum += decodeInt64(value)
	return nil
}

func (m *int64Merger) MergeOlder(value []byte) error {
	m.sum += decodeInt64(value)
	return nil
}

func (m *int64Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return encodeInt64(m.sum), nil, nil
}

func encodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

func decodeInt64(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// makeTermKey creates the Pebble key for a term.
func makeTermKey(field, term string) []byte {
	// Format: "t:{field}\x00{term}"
	key := make([]byte, 0, 2+len(field)+1+len(term))
	key = append(key, prefixTerm...)
	key = append(key, field...)
	key = append(key, 0) // null separator
	key = append(key, term...)
	return key
}

// parseTermKey extracts field and term from a Pebble key.
func parseTermKey(key []byte) (field, term string, ok bool) {
	if len(key) < 3 || string(key[:2]) != prefixTerm {
		return "", "", false
	}
	rest := key[2:]
	for i := 0; i < len(rest); i++ {
		if rest[i] == 0 {
			return string(rest[:i]), string(rest[i+1:]), true
		}
	}
	return "", "", false
}

// makeDFKey creates the Pebble key for document frequency.
func makeDFKey(termID string) []byte {
	return []byte(prefixDF + termID)
}

// Get retrieves a term ID if it exists.
func (r *PebbleFSTRegistry) Get(ctx context.Context, field, term string) (string, bool) {
	// 1. Check FST first (most terms are here)
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()

	if fst != nil {
		key := field + "\x00" + term
		if val, exists, _ := fst.Get([]byte(key)); exists {
			return formatTermID(val), true
		}
	}

	// 2. Check Pebble (recent terms)
	pebbleKey := makeTermKey(field, term)
	val, closer, err := r.pebble.Get(pebbleKey)
	if err == nil {
		defer closer.Close()
		return string(val), true
	}

	return "", false
}

// GetOrCreate retrieves an existing term or creates a new one.
func (r *PebbleFSTRegistry) GetOrCreate(ctx context.Context, field, term string) (string, error) {
	// Fast path: check if exists
	if termID, found := r.Get(ctx, field, term); found {
		return termID, nil
	}

	// Slow path: create new term in Pebble
	pebbleKey := makeTermKey(field, term)

	// Double-check with Pebble (another goroutine might have created it)
	val, closer, err := r.pebble.Get(pebbleKey)
	if err == nil {
		defer closer.Close()
		return string(val), nil
	}

	// Create new term ID using optimized formatting
	counter := r.termIDCounter.Add(1)
	termID := formatTermID(counter)

	// Write to Pebble
	if err := r.pebble.Set(pebbleKey, []byte(termID), pebbledb.NoSync); err != nil {
		return "", fmt.Errorf("write term to pebble: %w", err)
	}

	// Initialize DF to 0 using merge operator (faster than Set for this case)
	dfKey := makeDFKey(termID)
	r.pebble.Merge(dfKey, encodeInt64(0), pebbledb.NoSync)

	// Mark metadata as dirty (will be saved periodically, not on every term!)
	// This is a CRITICAL optimization: avoids Pebble write on every new term
	r.metaDirty.Store(true)

	return termID, nil
}

// CreateAlias creates a term alias pointing to an existing term ID.
func (r *PebbleFSTRegistry) CreateAlias(field, newTerm, existingTermID string) error {
	// Check if alias already exists
	if _, found := r.Get(context.Background(), field, newTerm); found {
		return nil // Already exists
	}

	// Create alias in Pebble
	pebbleKey := makeTermKey(field, newTerm)
	return r.pebble.Set(pebbleKey, []byte(existingTermID), pebbledb.NoSync)
}

// GetDF returns the document frequency for a term.
func (r *PebbleFSTRegistry) GetDF(termID string) int64 {
	dfKey := makeDFKey(termID)
	val, closer, err := r.pebble.Get(dfKey)
	if err != nil {
		return 0
	}
	defer closer.Close()
	return decodeInt64(val)
}

// IncrementDF atomically increments the document frequency.
func (r *PebbleFSTRegistry) IncrementDF(termID string, delta int64) {
	dfKey := makeDFKey(termID)
	// Use merge operator for atomic increment (no read-modify-write race)
	r.pebble.Merge(dfKey, encodeInt64(delta), pebbledb.NoSync)
}

// GetTotalDocs returns the total indexed documents.
func (r *PebbleFSTRegistry) GetTotalDocs() int64 {
	return r.totalDocs.Load()
}

// IncrementTotalDocs increments the total document count.
func (r *PebbleFSTRegistry) IncrementTotalDocs(delta int64) {
	r.totalDocs.Add(delta)
}

// GetTermsWithPrefix returns term IDs matching a prefix.
func (r *PebbleFSTRegistry) GetTermsWithPrefix(ctx context.Context, field, prefix string) []string {
	entries, _ := r.ListTermsByPrefix(ctx, field, prefix, 0)
	termIDs := make([]string, len(entries))
	for i, e := range entries {
		termIDs[i] = e.TermID
	}
	return termIDs
}

// GetTermCount returns the total number of terms.
func (r *PebbleFSTRegistry) GetTermCount() int64 {
	var count int64

	// Count from FST
	r.fstMu.RLock()
	if r.fst != nil {
		count = int64(r.fst.Len())
	}
	r.fstMu.RUnlock()

	// Count from Pebble (approximate via iterator)
	iter, err := r.pebble.NewIter(&pebbledb.IterOptions{
		LowerBound: []byte(prefixTerm),
		UpperBound: []byte(prefixTerm + "\xff"),
	})
	if err == nil {
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
	}

	return count
}

// Sync forces a sync to disk.
func (r *PebbleFSTRegistry) Sync() error {
	return r.pebble.Flush()
}

// Close closes the registry.
func (r *PebbleFSTRegistry) Close() error {
	// Stop background worker
	close(r.closeCh)
	r.wg.Wait()

	// Final merge before closing
	r.doMerge()

	// Save metadata
	r.saveMeta()

	// Close Pebble
	if r.pebble != nil {
		r.pebble.Close()
	}

	// Close FST
	r.fstMu.Lock()
	if r.fst != nil {
		r.fst.Close()
	}
	r.fstMu.Unlock()

	return nil
}

// ListTerms returns all terms, optionally filtered by field.
func (r *PebbleFSTRegistry) ListTerms(ctx context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	var entries []TermEntry
	seen := make(map[string]bool)

	// Collect from FST
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()

	if fst != nil {
		iter, err := fst.Iterator(nil, nil)
		if err == nil {
			for err == nil {
				key, val := iter.Current()
				f, t := parseFSTKey(string(key))
				if field == "" || f == field {
					termID := formatTermID(val)
					entryKey := f + "\x00" + t
					if !seen[entryKey] {
						seen[entryKey] = true
						entries = append(entries, TermEntry{
							Field:  f,
							Term:   t,
							TermID: termID,
							DF:     r.GetDF(termID),
						})
					}
				}
				err = iter.Next()
			}
		}
	}

	// Collect from Pebble
	pebbleIter, err := r.pebble.NewIter(&pebbledb.IterOptions{
		LowerBound: []byte(prefixTerm),
		UpperBound: []byte(prefixTerm + "\xff"),
	})
	if err == nil {
		defer pebbleIter.Close()
		for pebbleIter.First(); pebbleIter.Valid(); pebbleIter.Next() {
			f, t, ok := parseTermKey(pebbleIter.Key())
			if !ok {
				continue
			}
			if field == "" || f == field {
				entryKey := f + "\x00" + t
				if !seen[entryKey] {
					seen[entryKey] = true
					termID := string(pebbleIter.Value())
					entries = append(entries, TermEntry{
						Field:  f,
						Term:   t,
						TermID: termID,
						DF:     r.GetDF(termID),
					})
				}
			}
		}
	}

	// Sort
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Field != entries[j].Field {
			return entries[i].Field < entries[j].Field
		}
		return entries[i].Term < entries[j].Term
	})

	total := int64(len(entries))

	// Apply pagination
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

// ListTermsByPrefix returns terms matching a prefix.
func (r *PebbleFSTRegistry) ListTermsByPrefix(ctx context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	var entries []TermEntry
	seen := make(map[string]bool)
	searchKey := field + "\x00" + prefix

	// Search FST
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()

	if fst != nil {
		iter, err := fst.Iterator([]byte(searchKey), nil)
		if err == nil {
			for err == nil {
				key, val := iter.Current()
				if !bytes.HasPrefix(key, []byte(searchKey)) {
					break
				}
				f, t := parseFSTKey(string(key))
				entryKey := f + "\x00" + t
				if !seen[entryKey] {
					seen[entryKey] = true
					termID := formatTermID(val)
					entries = append(entries, TermEntry{
						Field:  f,
						Term:   t,
						TermID: termID,
						DF:     r.GetDF(termID),
					})
				}
				if limit > 0 && len(entries) >= limit {
					break
				}
				err = iter.Next()
			}
		}
	}

	// Search Pebble
	pebblePrefix := makeTermKey(field, prefix)
	// Create a copy for UpperBound to avoid slice aliasing corruption
	fieldPrefixLen := len(pebblePrefix) - len(prefix)
	upperBound := make([]byte, fieldPrefixLen+1)
	copy(upperBound, pebblePrefix[:fieldPrefixLen])
	upperBound[fieldPrefixLen] = 0xff
	pebbleIter, err := r.pebble.NewIter(&pebbledb.IterOptions{
		LowerBound: pebblePrefix,
		UpperBound: upperBound,
	})
	if err == nil {
		defer pebbleIter.Close()
		for pebbleIter.First(); pebbleIter.Valid(); pebbleIter.Next() {
			if !bytes.HasPrefix(pebbleIter.Key(), pebblePrefix[:len(pebblePrefix)-len(prefix)]) {
				break
			}
			f, t, ok := parseTermKey(pebbleIter.Key())
			if !ok || !bytes.HasPrefix([]byte(t), []byte(prefix)) {
				continue
			}
			entryKey := f + "\x00" + t
			if !seen[entryKey] {
				seen[entryKey] = true
				termID := string(pebbleIter.Value())
				entries = append(entries, TermEntry{
					Field:  f,
					Term:   t,
					TermID: termID,
					DF:     r.GetDF(termID),
				})
			}
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

// ListTermsByFuzzy returns terms within Levenshtein distance.
func (r *PebbleFSTRegistry) ListTermsByFuzzy(ctx context.Context, field, query string, maxDistance int, limit int) ([]TermEntry, error) {
	if maxDistance < 1 {
		maxDistance = 1
	}
	if maxDistance > 3 {
		maxDistance = 3
	}

	var entries []TermEntry
	seen := make(map[string]bool)
	fieldPrefix := field + "\x00"

	// Search FST
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()

	if fst != nil {
		iter, err := fst.Iterator([]byte(fieldPrefix), nil)
		if err == nil {
			for err == nil {
				key, val := iter.Current()
				f, t := parseFSTKey(string(key))
				if f != field {
					break
				}
				if levenshteinDistance(query, t) <= maxDistance {
					entryKey := f + "\x00" + t
					if !seen[entryKey] {
						seen[entryKey] = true
						termID := formatTermID(val)
						entries = append(entries, TermEntry{
							Field:  f,
							Term:   t,
							TermID: termID,
							DF:     r.GetDF(termID),
						})
					}
					if limit > 0 && len(entries) >= limit {
						break
					}
				}
				err = iter.Next()
			}
		}
	}

	// Search Pebble
	pebblePrefix := []byte(prefixTerm + field + "\x00")
	upperBound := make([]byte, len(pebblePrefix)+1)
	copy(upperBound, pebblePrefix)
	upperBound[len(pebblePrefix)] = 0xff
	pebbleIter, err := r.pebble.NewIter(&pebbledb.IterOptions{
		LowerBound: pebblePrefix,
		UpperBound: upperBound,
	})
	if err == nil {
		defer pebbleIter.Close()
		for pebbleIter.First(); pebbleIter.Valid(); pebbleIter.Next() {
			f, t, ok := parseTermKey(pebbleIter.Key())
			if !ok || f != field {
				continue
			}
			if levenshteinDistance(query, t) <= maxDistance {
				entryKey := f + "\x00" + t
				if !seen[entryKey] {
					seen[entryKey] = true
					termID := string(pebbleIter.Value())
					entries = append(entries, TermEntry{
						Field:  f,
						Term:   t,
						TermID: termID,
						DF:     r.GetDF(termID),
					})
				}
				if limit > 0 && len(entries) >= limit {
					break
				}
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

// ListTermsByRegex returns terms matching a regex pattern.
func (r *PebbleFSTRegistry) ListTermsByRegex(ctx context.Context, field, pattern string, limit int) ([]TermEntry, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}

	var entries []TermEntry
	seen := make(map[string]bool)
	fieldPrefix := field + "\x00"

	// Search FST
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()

	if fst != nil {
		iter, err := fst.Iterator([]byte(fieldPrefix), nil)
		if err == nil {
			for err == nil {
				key, val := iter.Current()
				f, t := parseFSTKey(string(key))
				if f != field {
					break
				}
				if re.MatchString(t) {
					entryKey := f + "\x00" + t
					if !seen[entryKey] {
						seen[entryKey] = true
						termID := formatTermID(val)
						entries = append(entries, TermEntry{
							Field:  f,
							Term:   t,
							TermID: termID,
							DF:     r.GetDF(termID),
						})
					}
					if limit > 0 && len(entries) >= limit {
						break
					}
				}
				err = iter.Next()
			}
		}
	}

	// Search Pebble
	pebblePrefix := []byte(prefixTerm + field + "\x00")
	upperBound2 := make([]byte, len(pebblePrefix)+1)
	copy(upperBound2, pebblePrefix)
	upperBound2[len(pebblePrefix)] = 0xff
	pebbleIter, err := r.pebble.NewIter(&pebbledb.IterOptions{
		LowerBound: pebblePrefix,
		UpperBound: upperBound2,
	})
	if err == nil {
		defer pebbleIter.Close()
		for pebbleIter.First(); pebbleIter.Valid(); pebbleIter.Next() {
			f, t, ok := parseTermKey(pebbleIter.Key())
			if !ok || f != field {
				continue
			}
			if re.MatchString(t) {
				entryKey := f + "\x00" + t
				if !seen[entryKey] {
					seen[entryKey] = true
					termID := string(pebbleIter.Value())
					entries = append(entries, TermEntry{
						Field:  f,
						Term:   t,
						TermID: termID,
						DF:     r.GetDF(termID),
					})
				}
				if limit > 0 && len(entries) >= limit {
					break
				}
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

// parseFSTKey extracts field and term from FST key format.
func parseFSTKey(key string) (field, term string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

// loadFST loads the FST from disk.
func (r *PebbleFSTRegistry) loadFST() error {
	fst, err := vellum.Open(r.fstPath)
	if err != nil {
		return err
	}
	r.fstMu.Lock()
	r.fst = fst
	r.fstMu.Unlock()
	return nil
}

// loadMeta loads metadata from Pebble.
func (r *PebbleFSTRegistry) loadMeta() error {
	// Load term counter
	val, closer, err := r.pebble.Get([]byte(prefixMeta + "counter"))
	if err == nil {
		defer closer.Close()
		r.termIDCounter.Store(binary.BigEndian.Uint64(val))
	}

	// Load total docs
	val, closer, err = r.pebble.Get([]byte(prefixMeta + "totaldocs"))
	if err == nil {
		defer closer.Close()
		r.totalDocs.Store(decodeInt64(val))
	}

	return nil
}

// saveMeta saves metadata to Pebble.
func (r *PebbleFSTRegistry) saveMeta() {
	// Save term counter
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, r.termIDCounter.Load())
	r.pebble.Set([]byte(prefixMeta+"counter"), buf, pebbledb.NoSync)

	// Save total docs
	r.pebble.Set([]byte(prefixMeta+"totaldocs"), encodeInt64(r.totalDocs.Load()), pebbledb.NoSync)

	// Clear dirty flag
	r.metaDirty.Store(false)
}

// mergeWorker runs the background merge job.
func (r *PebbleFSTRegistry) mergeWorker() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.mergeConfig.MergeCheckInterval)
	defer ticker.Stop()

	// Meta save ticker - save metadata every 5 seconds if dirty
	metaTicker := time.NewTicker(5 * time.Second)
	defer metaTicker.Stop()

	for {
		select {
		case <-r.closeCh:
			return
		case <-ticker.C:
			if r.shouldMerge() {
				r.doMerge()
			}
		case <-metaTicker.C:
			// Periodically save metadata if dirty (avoids save on every new term)
			if r.metaDirty.Load() {
				r.saveMeta()
			}
		}
	}
}

// shouldMerge checks if merge triggers are met.
func (r *PebbleFSTRegistry) shouldMerge() bool {
	if r.merging.Load() {
		return false
	}

	// Check cooldown
	lastMerge := r.lastMergeTime.Load()
	now := time.Now().Unix()
	if now-lastMerge < int64(r.mergeConfig.MinTimeBetweenMerges.Seconds()) {
		return false
	}

	// Check time trigger
	if now-lastMerge >= int64(r.mergeConfig.MaxTimeSinceLastMerge.Seconds()) {
		return true
	}

	// Check size trigger (approximate via disk usage)
	metrics := r.pebble.Metrics()
	sizeMB := int(metrics.DiskSpaceUsage() / (1024 * 1024))
	if sizeMB >= r.mergeConfig.MaxPebbleSizeMB {
		return true
	}

	// Check term count trigger
	termCount := r.getPebbleTermCount()
	if termCount >= r.mergeConfig.MaxPebbleTermCount {
		return true
	}

	return false
}

// getPebbleTermCount counts terms in Pebble (only new ones not in FST).
func (r *PebbleFSTRegistry) getPebbleTermCount() int {
	count := 0
	iter, err := r.pebble.NewIter(&pebbledb.IterOptions{
		LowerBound: []byte(prefixTerm),
		UpperBound: []byte(prefixTerm + "\xff"),
	})
	if err != nil {
		return 0
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count
}

// doMerge performs the Pebble -> FST merge.
func (r *PebbleFSTRegistry) doMerge() {
	if !r.merging.CompareAndSwap(false, true) {
		return // Already merging
	}
	defer r.merging.Store(false)

	r.lastMergeTime.Store(time.Now().Unix())

	// Phase 1: Collect all terms (FST + Pebble)
	// Since both are ordered, we do a merge-sort
	keyVals := make(map[string]uint64)

	// Read from existing FST
	r.fstMu.RLock()
	oldFST := r.fst
	r.fstMu.RUnlock()

	if oldFST != nil {
		iter, err := oldFST.Iterator(nil, nil)
		if err == nil {
			for err == nil {
				key, val := iter.Current()
				keyVals[string(key)] = val
				err = iter.Next()
			}
		}
	}

	// Read from Pebble (will overwrite FST entries if same key)
	pebbleIter, err := r.pebble.NewIter(&pebbledb.IterOptions{
		LowerBound: []byte(prefixTerm),
		UpperBound: []byte(prefixTerm + "\xff"),
	})
	if err != nil {
		return
	}

	var keysToDelete [][]byte
	for pebbleIter.First(); pebbleIter.Valid(); pebbleIter.Next() {
		field, term, ok := parseTermKey(pebbleIter.Key())
		if !ok {
			continue
		}
		fstKey := field + "\x00" + term

		termID := string(pebbleIter.Value())
		// Use optimized parseTermID instead of fmt.Sscanf
		counter := parseTermID(termID)

		keyVals[fstKey] = counter
		keysToDelete = append(keysToDelete, append([]byte(nil), pebbleIter.Key()...))
	}
	pebbleIter.Close()

	if len(keyVals) == 0 {
		return
	}

	// Phase 2: Build new FST (keys must be sorted)
	sortedKeys := make([]string, 0, len(keyVals))
	for k := range keyVals {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Write to temp file
	tempPath := r.fstPath + ".tmp"
	f, err := os.Create(tempPath)
	if err != nil {
		return
	}

	builder, err := vellum.New(f, nil)
	if err != nil {
		f.Close()
		os.Remove(tempPath)
		return
	}

	for _, key := range sortedKeys {
		if err := builder.Insert([]byte(key), keyVals[key]); err != nil {
			builder.Close()
			f.Close()
			os.Remove(tempPath)
			return
		}
	}

	if err := builder.Close(); err != nil {
		f.Close()
		os.Remove(tempPath)
		return
	}
	f.Close()

	// Phase 3: Open new FST
	newFST, err := vellum.Open(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return
	}

	// Phase 4: Atomic swap
	r.fstMu.Lock()
	oldFSTToClose := r.fst
	r.fst = newFST
	r.fstMu.Unlock()

	// Rename temp to final
	os.Rename(tempPath, r.fstPath)

	// Close old FST
	if oldFSTToClose != nil {
		oldFSTToClose.Close()
	}

	// Phase 5: Delete merged terms from Pebble
	batch := r.pebble.NewBatch()
	for _, key := range keysToDelete {
		batch.Delete(key, nil)
	}
	batch.Commit(pebbledb.NoSync)
	batch.Close()

	// Save metadata after merge
	r.saveMeta()
}

// Stats returns registry statistics.
func (r *PebbleFSTRegistry) Stats() PebbleFSTStats {
	r.fstMu.RLock()
	var fstCount int64
	if r.fst != nil {
		fstCount = int64(r.fst.Len())
	}
	r.fstMu.RUnlock()

	pebbleCount := r.getPebbleTermCount()

	metrics := r.pebble.Metrics()
	pebbleSizeMB := int(metrics.DiskSpaceUsage() / (1024 * 1024))

	return PebbleFSTStats{
		FSTTermCount:    fstCount,
		PebbleTermCount: int64(pebbleCount),
		TotalTermCount:  fstCount + int64(pebbleCount),
		TotalDocs:       r.totalDocs.Load(),
		PebbleSizeMB:    pebbleSizeMB,
		IsMerging:       r.merging.Load(),
	}
}

// PebbleFSTStats contains registry statistics.
type PebbleFSTStats struct {
	FSTTermCount    int64 `json:"fst_term_count"`
	PebbleTermCount int64 `json:"pebble_term_count"`
	TotalTermCount  int64 `json:"total_term_count"`
	TotalDocs       int64 `json:"total_docs"`
	PebbleSizeMB    int   `json:"pebble_size_mb"`
	IsMerging       bool  `json:"is_merging"`
}

// Compile-time check
var _ TermRegistry = (*PebbleFSTRegistry)(nil)
