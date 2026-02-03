package segment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/vellum"

	"plastic-engine-core/internal/core/search/wal"
)

// FSTTermRegistry is a term registry backed by a Finite State Transducer.
// It provides O(key_length) lookups and efficient prefix/fuzzy/regex iteration.
type FSTTermRegistry struct {
	mu sync.RWMutex

	// FST for fast lookups (immutable, rebuilt periodically)
	fst *vellum.FST

	// Pending writes (not yet in FST)
	pending map[string]uint64 // key -> encoded term info

	// Term ID to info mapping (needed for reverse lookups)
	idToInfo map[string]FSTTermInfo

	// Document frequency cache (protected by dfMu for fine-grained locking)
	dfMu    sync.RWMutex
	dfCache map[string]int64

	// Global stats (atomic for lock-free access)
	totalDocs atomic.Int64

	// WAL for durability
	wal *wal.WAL

	// Paths
	dataDir  string
	fstPath  string
	metaPath string

	// Counter for generating term IDs
	termIDCounter uint64

	// Dirty flag for rebuild
	dirty atomic.Bool

	// Rebuild in progress flag (prevents concurrent rebuilds)
	rebuilding atomic.Bool

	// Rebuild threshold (number of pending items before auto-rebuild)
	rebuildThreshold int

	// Last rebuild time (for cooldown)
	lastRebuildTime atomic.Int64 // Unix timestamp in seconds

	// Minimum time between rebuilds (cooldown)
	rebuildCooldown time.Duration
}

// FSTTermInfo stores metadata about a term.
type FSTTermInfo struct {
	TermID    string    `json:"term_id"`
	Field     string    `json:"field"`
	Term      string    `json:"term"`
	CreatedAt time.Time `json:"created_at"`
}

// FSTRegistryConfig configures the FST registry.
type FSTRegistryConfig struct {
	DataDir          string
	RebuildThreshold int           // Number of pending writes before auto-rebuild (default: 10000)
	RebuildCooldown  time.Duration // Minimum time between rebuilds (default: 30s)
}

// TermEntry represents a term in iteration results.
type TermEntry struct {
	Field  string `json:"field"`
	Term   string `json:"term"`
	TermID string `json:"term_id"`
	DF     int64  `json:"df,omitempty"` // Document frequency
}

// NewFSTTermRegistry creates a new FST-backed term registry.
func NewFSTTermRegistry(config FSTRegistryConfig) (*FSTTermRegistry, error) {
	if config.RebuildThreshold <= 0 {
		config.RebuildThreshold = 10000 // Higher default for better batch performance
	}
	if config.RebuildCooldown <= 0 {
		config.RebuildCooldown = 30 * time.Second // Minimum 30s between rebuilds
	}

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	walPath := filepath.Join(config.DataDir, "fst_registry.wal")
	w, err := wal.Open(walPath, wal.Options{
		SyncMode:   wal.SyncBatch,
		BufferSize: 32 * 1024,
	})
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	r := &FSTTermRegistry{
		pending:          make(map[string]uint64),
		idToInfo:         make(map[string]FSTTermInfo),
		dfCache:          make(map[string]int64),
		wal:              w,
		dataDir:          config.DataDir,
		fstPath:          filepath.Join(config.DataDir, "terms.fst"),
		metaPath:         filepath.Join(config.DataDir, "terms.meta"),
		rebuildThreshold: config.RebuildThreshold,
		rebuildCooldown:  config.RebuildCooldown,
	}

	// Load existing FST if present
	if err := r.loadFST(); err != nil {
		// FST doesn't exist yet, that's OK
	}

	// Load metadata
	if err := r.loadMeta(); err != nil {
		// Meta doesn't exist yet, that's OK
	}

	// Replay WAL
	if err := r.replayWAL(); err != nil {
		return nil, fmt.Errorf("replay wal: %w", err)
	}

	return r, nil
}

// makeKey creates the lookup key from field and term.
func fstMakeKey(field, term string) string {
	return field + "\x00" + term // Use null byte as separator
}

// parseKey extracts field and term from a key.
func fstParseKey(key string) (field, term string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

// encodeTermInfo encodes term info into a uint64 for FST storage.
// We store the term ID counter value, and look up full info separately.
func encodeTermInfo(counter uint64) uint64 {
	return counter
}

// generateTermID creates a unique term ID.
func (r *FSTTermRegistry) generateTermID() string {
	counter := atomic.AddUint64(&r.termIDCounter, 1)
	return fmt.Sprintf("t%d", counter)
}

// Get retrieves a term ID if it exists. O(key_length) lookup.
func (r *FSTTermRegistry) Get(_ context.Context, field, term string) (string, bool) {
	key := fstMakeKey(field, term)
	keyBytes := []byte(key)

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check pending first (recent writes)
	if counter, ok := r.pending[key]; ok {
		termID := fmt.Sprintf("t%d", counter)
		return termID, true
	}

	// Check FST
	if r.fst != nil {
		if counter, exists, _ := r.fst.Get(keyBytes); exists {
			termID := fmt.Sprintf("t%d", counter)
			return termID, true
		}
	}

	return "", false
}

// GetOrCreate retrieves an existing term or creates a new one.
func (r *FSTTermRegistry) GetOrCreate(_ context.Context, field, term string) (string, error) {
	key := fstMakeKey(field, term)
	keyBytes := []byte(key)

	// Fast path: read lock
	r.mu.RLock()
	if counter, ok := r.pending[key]; ok {
		r.mu.RUnlock()
		return fmt.Sprintf("t%d", counter), nil
	}
	if r.fst != nil {
		if counter, exists, _ := r.fst.Get(keyBytes); exists {
			r.mu.RUnlock()
			return fmt.Sprintf("t%d", counter), nil
		}
	}
	r.mu.RUnlock()

	// Slow path: write lock
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if counter, ok := r.pending[key]; ok {
		return fmt.Sprintf("t%d", counter), nil
	}
	if r.fst != nil {
		if counter, exists, _ := r.fst.Get(keyBytes); exists {
			return fmt.Sprintf("t%d", counter), nil
		}
	}

	// Create new term
	counter := atomic.AddUint64(&r.termIDCounter, 1)
	termID := fmt.Sprintf("t%d", counter)

	r.pending[key] = counter
	r.idToInfo[termID] = FSTTermInfo{
		TermID:    termID,
		Field:     field,
		Term:      term,
		CreatedAt: time.Now().UTC(),
	}
	r.dirty.Store(true)

	// Initialize DF cache (separate lock)
	r.dfMu.Lock()
	r.dfCache[termID] = 0
	r.dfMu.Unlock()

	// Log to WAL
	if r.wal != nil {
		_, _ = r.wal.AppendAlias(wal.AliasOp{
			Field:  field,
			Term:   term,
			TermID: termID,
		})
	}

	// Auto-rebuild if threshold reached (only if not already rebuilding and cooldown passed)
	pendingCount := len(r.pending)
	if pendingCount >= r.rebuildThreshold && !r.rebuilding.Load() {
		lastRebuild := r.lastRebuildTime.Load()
		now := time.Now().Unix()
		if now-lastRebuild >= int64(r.rebuildCooldown.Seconds()) {
			go r.Rebuild()
		}
	}

	return termID, nil
}

// CreateAlias creates a term alias pointing to an existing term ID.
func (r *FSTTermRegistry) CreateAlias(field, newTerm, existingTermID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Verify existing term ID
	info, ok := r.idToInfo[existingTermID]
	if !ok {
		return fmt.Errorf("term_id %s does not exist", existingTermID)
	}

	key := fstMakeKey(field, newTerm)

	// Check if already exists
	if _, ok := r.pending[key]; ok {
		return nil
	}
	if r.fst != nil {
		if _, exists, _ := r.fst.Get([]byte(key)); exists {
			return nil
		}
	}

	// Extract counter from term ID
	var counter uint64
	fmt.Sscanf(existingTermID, "t%d", &counter)

	r.pending[key] = counter
	r.dirty.Store(true)

	// Log to WAL
	if r.wal != nil {
		_, _ = r.wal.AppendAlias(wal.AliasOp{
			Field:  field,
			Term:   newTerm,
			TermID: existingTermID,
		})
	}

	// Also add to idToInfo if not exists (for the alias key)
	aliasInfo := FSTTermInfo{
		TermID:    existingTermID,
		Field:     field,
		Term:      newTerm,
		CreatedAt: info.CreatedAt,
	}
	_ = aliasInfo // Aliases share the same term ID

	return nil
}

// GetDF returns the document frequency for a term.
func (r *FSTTermRegistry) GetDF(termID string) int64 {
	r.dfMu.RLock()
	defer r.dfMu.RUnlock()
	return r.dfCache[termID]
}

// IncrementDF increments the document frequency.
func (r *FSTTermRegistry) IncrementDF(termID string, delta int64) {
	r.dfMu.Lock()
	r.dfCache[termID] += delta
	r.dfMu.Unlock()
}

// GetTotalDocs returns the total indexed documents.
func (r *FSTTermRegistry) GetTotalDocs() int64 {
	return r.totalDocs.Load()
}

// IncrementTotalDocs increments the total document count.
func (r *FSTTermRegistry) IncrementTotalDocs(delta int64) {
	r.totalDocs.Add(delta)
}

// ListTerms returns all terms, optionally filtered by field.
func (r *FSTTermRegistry) ListTerms(ctx context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	r.mu.RLock()

	var entries []TermEntry

	// Collect from FST
	if r.fst != nil {
		iter, err := r.fst.Iterator(nil, nil)
		if err == nil {
			for err == nil {
				key, counter := iter.Current()
				f, t := fstParseKey(string(key))
				if field == "" || f == field {
					termID := fmt.Sprintf("t%d", counter)
					entries = append(entries, TermEntry{
						Field:  f,
						Term:   t,
						TermID: termID,
					})
				}
				err = iter.Next()
			}
		}
	}

	// Collect from pending
	for key, counter := range r.pending {
		f, t := fstParseKey(key)
		if field == "" || f == field {
			termID := fmt.Sprintf("t%d", counter)
			// Check if already in entries from FST
			found := false
			for _, e := range entries {
				if e.Field == f && e.Term == t {
					found = true
					break
				}
			}
			if !found {
				entries = append(entries, TermEntry{
					Field:  f,
					Term:   t,
					TermID: termID,
				})
			}
		}
	}
	r.mu.RUnlock()

	// Populate DF values with separate lock
	r.dfMu.RLock()
	for i := range entries {
		entries[i].DF = r.dfCache[entries[i].TermID]
	}
	r.dfMu.RUnlock()

	// Sort by field, then term
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Field != entries[j].Field {
			return entries[i].Field < entries[j].Field
		}
		return entries[i].Term < entries[j].Term
	})

	total := int64(len(entries))

	// Apply pagination
	if offset > 0 {
		if offset >= len(entries) {
			return nil, total, nil
		}
		entries = entries[offset:]
	}
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, total, nil
}

// ListTermsByPrefix returns terms matching a prefix.
func (r *FSTTermRegistry) ListTermsByPrefix(ctx context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	r.mu.RLock()

	var entries []TermEntry
	searchPrefix := fstMakeKey(field, prefix)

	// Search FST with prefix
	if r.fst != nil {
		iter, err := r.fst.Iterator([]byte(searchPrefix), nil)
		if err == nil {
			for err == nil {
				key, counter := iter.Current()
				keyStr := string(key)

				// Check if still matches prefix
				if !bytes.HasPrefix(key, []byte(searchPrefix)) {
					break
				}

				f, t := fstParseKey(keyStr)
				termID := fmt.Sprintf("t%d", counter)
				entries = append(entries, TermEntry{
					Field:  f,
					Term:   t,
					TermID: termID,
				})

				if limit > 0 && len(entries) >= limit {
					break
				}

				err = iter.Next()
			}
		}
	}

	// Search pending
	for key, counter := range r.pending {
		if bytes.HasPrefix([]byte(key), []byte(searchPrefix)) {
			f, t := fstParseKey(key)
			termID := fmt.Sprintf("t%d", counter)

			// Check if already in entries
			found := false
			for _, e := range entries {
				if e.Field == f && e.Term == t {
					found = true
					break
				}
			}
			if !found {
				entries = append(entries, TermEntry{
					Field:  f,
					Term:   t,
					TermID: termID,
				})
			}
		}
	}
	r.mu.RUnlock()

	// Populate DF values with separate lock
	r.dfMu.RLock()
	for i := range entries {
		entries[i].DF = r.dfCache[entries[i].TermID]
	}
	r.dfMu.RUnlock()

	// Sort
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// ListTermsByFuzzy returns terms within Levenshtein distance of the query.
func (r *FSTTermRegistry) ListTermsByFuzzy(ctx context.Context, field, query string, maxDistance int, limit int) ([]TermEntry, error) {
	if maxDistance < 1 {
		maxDistance = 1
	}
	if maxDistance > 3 {
		maxDistance = 3 // Cap for performance
	}

	r.mu.RLock()

	var entries []TermEntry

	// Search FST
	if r.fst != nil {
		prefix := fstMakeKey(field, "")
		iter, err := r.fst.Iterator([]byte(prefix), nil)
		if err == nil {
			for err == nil {
				key, counter := iter.Current()
				f, t := fstParseKey(string(key))

				// Stop if we've moved past this field
				if f != field {
					break
				}

				// Check if term matches fuzzy using Levenshtein distance
				if levenshteinDistance(query, t) <= maxDistance {
					termID := fmt.Sprintf("t%d", counter)
					entries = append(entries, TermEntry{
						Field:  f,
						Term:   t,
						TermID: termID,
					})

					if limit > 0 && len(entries) >= limit {
						break
					}
				}

				err = iter.Next()
			}
		}
	}

	// Search pending
	for key, counter := range r.pending {
		f, t := fstParseKey(key)
		if f == field && levenshteinDistance(query, t) <= maxDistance {
			termID := fmt.Sprintf("t%d", counter)

			found := false
			for _, e := range entries {
				if e.Field == f && e.Term == t {
					found = true
					break
				}
			}
			if !found {
				entries = append(entries, TermEntry{
					Field:  f,
					Term:   t,
					TermID: termID,
				})
			}
		}
	}
	r.mu.RUnlock()

	// Populate DF values with separate lock
	r.dfMu.RLock()
	for i := range entries {
		entries[i].DF = r.dfCache[entries[i].TermID]
	}
	r.dfMu.RUnlock()

	// Sort by term
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// levenshteinDistance calculates the Levenshtein distance between two strings.
func levenshteinDistance(a, b string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	// Create two rows for the DP table
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)

	// Initialize first row
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}

	// Fill the table
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			curr[j] = minInt(
				prev[j]+1,          // deletion
				curr[j-1]+1,        // insertion
				prev[j-1]+cost,     // substitution
			)
		}
		prev, curr = curr, prev
	}

	return prev[len(b)]
}

// minInt returns the minimum of three integers.
func minInt(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// ListTermsByRegex returns terms matching a regex pattern.
func (r *FSTTermRegistry) ListTermsByRegex(ctx context.Context, field, pattern string, limit int) ([]TermEntry, error) {
	// Compile regex before locking
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}

	r.mu.RLock()

	var entries []TermEntry

	// Search FST
	if r.fst != nil {
		prefix := fstMakeKey(field, "")
		iter, err := r.fst.Iterator([]byte(prefix), nil)
		if err == nil {
			for err == nil {
				key, counter := iter.Current()
				f, t := fstParseKey(string(key))

				if f != field {
					break
				}

				if re.MatchString(t) {
					termID := fmt.Sprintf("t%d", counter)
					entries = append(entries, TermEntry{
						Field:  f,
						Term:   t,
						TermID: termID,
					})

					if limit > 0 && len(entries) >= limit {
						break
					}
				}

				err = iter.Next()
			}
		}
	}

	// Search pending
	for key, counter := range r.pending {
		f, t := fstParseKey(key)
		if f == field && re.MatchString(t) {
			termID := fmt.Sprintf("t%d", counter)

			found := false
			for _, e := range entries {
				if e.Field == f && e.Term == t {
					found = true
					break
				}
			}
			if !found {
				entries = append(entries, TermEntry{
					Field:  f,
					Term:   t,
					TermID: termID,
				})
			}
		}
	}
	r.mu.RUnlock()

	// Populate DF values with separate lock
	r.dfMu.RLock()
	for i := range entries {
		entries[i].DF = r.dfCache[entries[i].TermID]
	}
	r.dfMu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// GetTermsWithPrefix returns term IDs matching a prefix (for backward compatibility).
func (r *FSTTermRegistry) GetTermsWithPrefix(_ context.Context, field, prefix string) []string {
	entries, _ := r.ListTermsByPrefix(context.Background(), field, prefix, 0)
	termIDs := make([]string, len(entries))
	for i, e := range entries {
		termIDs[i] = e.TermID
	}
	return termIDs
}

// Stats returns registry statistics.
func (r *FSTTermRegistry) Stats() FSTRegistryStats {
	r.mu.RLock()
	var fstTerms int64
	if r.fst != nil {
		fstTerms = int64(r.fst.Len())
	}
	pendingCount := int64(len(r.pending))
	uniqueTermIDs := int64(len(r.idToInfo))
	r.mu.RUnlock()

	return FSTRegistryStats{
		FSTTermCount:     fstTerms,
		PendingTermCount: pendingCount,
		TotalTermCount:   fstTerms + pendingCount,
		UniqueTermIDs:    uniqueTermIDs,
		TotalDocs:        r.totalDocs.Load(),
		IsDirty:          r.dirty.Load(),
	}
}

// GetTermCount returns the total number of terms in the registry.
func (r *FSTTermRegistry) GetTermCount() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var fstTerms int64
	if r.fst != nil {
		fstTerms = int64(r.fst.Len())
	}
	return fstTerms + int64(len(r.pending))
}

// FSTRegistryStats contains statistics.
type FSTRegistryStats struct {
	FSTTermCount     int64 `json:"fst_term_count"`
	PendingTermCount int64 `json:"pending_term_count"`
	TotalTermCount   int64 `json:"total_term_count"`
	UniqueTermIDs    int64 `json:"unique_term_ids"`
	TotalDocs        int64 `json:"total_docs"`
	IsDirty          bool  `json:"is_dirty"`
}

// Rebuild reconstructs the FST from current data.
// This function uses a snapshot-build-swap pattern to minimize lock contention.
func (r *FSTTermRegistry) Rebuild() error {
	// Prevent concurrent rebuilds
	if !r.rebuilding.CompareAndSwap(false, true) {
		return nil // Already rebuilding
	}
	defer r.rebuilding.Store(false)

	// Record rebuild time at start
	r.lastRebuildTime.Store(time.Now().Unix())

	// Phase 1: Quick snapshot while holding lock
	r.mu.Lock()
	if !r.dirty.Load() && len(r.pending) == 0 {
		r.mu.Unlock()
		return nil
	}

	// Snapshot pending entries
	pendingSnapshot := make(map[string]uint64, len(r.pending))
	for k, v := range r.pending {
		pendingSnapshot[k] = v
	}

	// Get reference to current FST (safe to read without lock after snapshot)
	currentFST := r.fst
	r.mu.Unlock()

	// Phase 2: Build new FST WITHOUT holding lock
	// Collect all keys from current FST and pending
	keySet := make(map[string]uint64)

	if currentFST != nil {
		iter, err := currentFST.Iterator(nil, nil)
		if err == nil {
			for err == nil {
				key, val := iter.Current()
				keySet[string(key)] = val
				err = iter.Next()
			}
		}
	}

	// Merge pending (pending overrides FST)
	for k, v := range pendingSnapshot {
		keySet[k] = v
	}

	// Sort keys (required by vellum)
	sortedKeys := make([]string, 0, len(keySet))
	for k := range keySet {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Build new FST to temp file
	tempPath := r.fstPath + ".tmp"
	f, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("create temp fst: %w", err)
	}

	builder, err := vellum.New(f, nil)
	if err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("create fst builder: %w", err)
	}

	for _, key := range sortedKeys {
		if err := builder.Insert([]byte(key), keySet[key]); err != nil {
			builder.Close()
			f.Close()
			os.Remove(tempPath)
			return fmt.Errorf("insert key %s: %w", key, err)
		}
	}

	if err := builder.Close(); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("close builder: %w", err)
	}
	f.Close()

	// Open new FST
	newFST, err := vellum.Open(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("open new fst: %w", err)
	}

	// Phase 3: Swap with lock (quick operation)
	r.mu.Lock()

	// Remove only the keys we snapshotted from pending
	// (new keys added during rebuild stay in pending)
	for k := range pendingSnapshot {
		delete(r.pending, k)
	}

	oldFST := r.fst
	r.fst = newFST

	// Only mark clean if no new pending entries were added
	if len(r.pending) == 0 {
		r.dirty.Store(false)
	}

	r.mu.Unlock()

	// Phase 4: Finalize without lock
	// Rename temp to final
	if err := os.Rename(tempPath, r.fstPath); err != nil {
		// Rollback - need lock
		r.mu.Lock()
		r.fst = oldFST
		// Restore pending
		for k, v := range pendingSnapshot {
			r.pending[k] = v
		}
		r.mu.Unlock()
		newFST.Close()
		return fmt.Errorf("rename fst: %w", err)
	}

	// Close old FST
	if oldFST != nil {
		oldFST.Close()
	}

	// Save metadata (with appropriate locking)
	if err := r.saveMetaLocked(); err != nil {
		return fmt.Errorf("save meta: %w", err)
	}

	// Truncate WAL
	if r.wal != nil {
		r.wal.Truncate()
	}

	return nil
}

// loadFST loads the FST from disk.
func (r *FSTTermRegistry) loadFST() error {
	fst, err := vellum.Open(r.fstPath)
	if err != nil {
		return err
	}
	r.fst = fst
	return nil
}

// loadMeta loads metadata from disk.
func (r *FSTTermRegistry) loadMeta() error {
	data, err := os.ReadFile(r.metaPath)
	if err != nil {
		return err
	}

	// Simple binary format: counter(8) + totalDocs(8) + numEntries(4) + entries...
	if len(data) < 20 {
		return fmt.Errorf("meta too short")
	}

	r.termIDCounter = binary.BigEndian.Uint64(data[0:8])
	r.totalDocs.Store(int64(binary.BigEndian.Uint64(data[8:16])))
	numEntries := binary.BigEndian.Uint32(data[16:20])

	offset := 20
	for i := uint32(0); i < numEntries && offset < len(data); i++ {
		// termID length + termID + df
		if offset+4 > len(data) {
			break
		}
		idLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(idLen)+8 > len(data) {
			break
		}
		termID := string(data[offset : offset+int(idLen)])
		offset += int(idLen)

		df := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8

		r.dfCache[termID] = df
	}

	return nil
}

// saveMeta saves metadata to disk (requires main lock to be held).
func (r *FSTTermRegistry) saveMeta() error {
	var buf bytes.Buffer

	// counter + totalDocs + numEntries
	binary.Write(&buf, binary.BigEndian, r.termIDCounter)
	binary.Write(&buf, binary.BigEndian, uint64(r.totalDocs.Load()))

	r.dfMu.RLock()
	binary.Write(&buf, binary.BigEndian, uint32(len(r.dfCache)))
	for termID, df := range r.dfCache {
		binary.Write(&buf, binary.BigEndian, uint32(len(termID)))
		buf.WriteString(termID)
		binary.Write(&buf, binary.BigEndian, uint64(df))
	}
	r.dfMu.RUnlock()

	tempPath := r.metaPath + ".tmp"
	if err := os.WriteFile(tempPath, buf.Bytes(), 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, r.metaPath)
}

// saveMetaLocked saves metadata with proper locking.
func (r *FSTTermRegistry) saveMetaLocked() error {
	var buf bytes.Buffer

	// Snapshot counter atomically
	r.mu.RLock()
	counter := r.termIDCounter
	r.mu.RUnlock()

	// counter + totalDocs + numEntries
	binary.Write(&buf, binary.BigEndian, counter)
	binary.Write(&buf, binary.BigEndian, uint64(r.totalDocs.Load()))

	r.dfMu.RLock()
	binary.Write(&buf, binary.BigEndian, uint32(len(r.dfCache)))
	for termID, df := range r.dfCache {
		binary.Write(&buf, binary.BigEndian, uint32(len(termID)))
		buf.WriteString(termID)
		binary.Write(&buf, binary.BigEndian, uint64(df))
	}
	r.dfMu.RUnlock()

	tempPath := r.metaPath + ".tmp"
	if err := os.WriteFile(tempPath, buf.Bytes(), 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, r.metaPath)
}

// replayWAL replays the WAL to recover state.
func (r *FSTTermRegistry) replayWAL() error {
	walPath := filepath.Join(r.dataDir, "fst_registry.wal")

	reader, err := wal.NewReader(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer reader.Close()

	entries, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.Type == wal.OpAlias {
			var op wal.AliasOp
			if err := decodeAliasOp(entry.Data, &op); err != nil {
				continue
			}

			key := fstMakeKey(op.Field, op.Term)
			var counter uint64
			fmt.Sscanf(op.TermID, "t%d", &counter)

			r.pending[key] = counter
			if counter > r.termIDCounter {
				r.termIDCounter = counter
			}

			r.idToInfo[op.TermID] = FSTTermInfo{
				TermID:    op.TermID,
				Field:     op.Field,
				Term:      op.Term,
				CreatedAt: entry.Timestamp,
			}

			if _, ok := r.dfCache[op.TermID]; !ok {
				r.dfCache[op.TermID] = 0
			}
		}
	}

	r.dirty.Store(len(r.pending) > 0)
	return nil
}

// decodeAliasOp decodes a WAL alias operation.
func decodeAliasOp(data []byte, op *wal.AliasOp) error {
	return json.Unmarshal(data, op)
}

// Sync forces a sync.
func (r *FSTTermRegistry) Sync() error {
	if r.wal != nil {
		return r.wal.Sync()
	}
	return nil
}

// Close closes the registry.
func (r *FSTTermRegistry) Close() error {
	// Save if dirty (Rebuild handles its own locking)
	r.mu.RLock()
	hasPending := len(r.pending) > 0
	isDirty := r.dirty.Load()
	r.mu.RUnlock()

	if isDirty || hasPending {
		r.Rebuild()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fst != nil {
		r.fst.Close()
	}

	if r.wal != nil {
		r.wal.Close()
	}

	return nil
}
