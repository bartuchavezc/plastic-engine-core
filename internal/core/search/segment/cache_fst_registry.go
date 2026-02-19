package segment

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
)

// CacheFSTRegistry is a term registry using an in-memory cache for fast writes,
// a term WAL for durability, and FST for advanced lookups (prefix, fuzzy, regex).
//
// Architecture:
//   - Cache (map[string]uint64): All known terms in memory. GetOrCreate is a map lookup.
//   - Term WAL (append-only file): Durability for new terms between FST rebuilds.
//   - FST: Immutable read structure for prefix/fuzzy/regex searches.
//
// Hot path (GetOrCreate): pure in-memory map lookup, zero disk I/O.
// Background: periodically rebuild FST from cache, truncate WAL.
type CacheFSTRegistry struct {
	// In-memory cache: "field\x00term" -> termID counter (uint64).
	// Bounded by vocabulary size (~10-30MB for 100K-300K terms).
	cache   map[string]uint64
	cacheMu sync.RWMutex

	// Read layer for advanced lookups (prefix, fuzzy, regex)
	fst   *vellum.FST
	fstMu sync.RWMutex // Only for atomic FST swap

	// Term WAL for durability of new terms between FST rebuilds
	walFile   *os.File
	walWriter *bufio.Writer
	walMu     sync.Mutex
	walPath   string

	// Document frequency: fully in-memory
	dfCache   map[string]int64
	dfCacheMu sync.RWMutex

	// Global stats (atomic for lock-free access)
	totalDocs atomic.Int64

	// Term ID counter
	termIDCounter atomic.Uint64

	// Merge control (cache -> FST rebuild)
	merging       atomic.Bool
	lastMergeTime atomic.Int64 // Unix timestamp
	mergeConfig   MergeConfig

	// Paths
	dataDir  string
	fstPath  string
	metaPath string

	// Background worker
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// MergeConfig configures when the cache is consolidated into FST.
type MergeConfig struct {
	// MaxCacheSizeMB triggers FST rebuild when estimated cache size exceeds this.
	// Default: 64MB
	MaxCacheSizeMB int

	// MaxNewTermCount triggers FST rebuild when new terms (not in FST) exceed this.
	// Default: 100,000
	MaxNewTermCount int

	// MaxTimeSinceLastMerge triggers FST rebuild after this duration.
	// Default: 10 minutes
	MaxTimeSinceLastMerge time.Duration

	// MinTimeBetweenMerges prevents rebuilds too close together.
	// Default: 30 seconds
	MinTimeBetweenMerges time.Duration

	// MergeCheckInterval is how often to check rebuild triggers.
	// Default: 5 seconds
	MergeCheckInterval time.Duration
}

// DefaultMergeConfig returns sensible defaults for production use.
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		MaxCacheSizeMB:        64,
		MaxNewTermCount:       100_000,
		MaxTimeSinceLastMerge: 10 * time.Minute,
		MinTimeBetweenMerges:  30 * time.Second,
		MergeCheckInterval:    5 * time.Second,
	}
}

// CacheFSTConfig configures the registry.
type CacheFSTConfig struct {
	DataDir     string
	MergeConfig MergeConfig
}

// formatTermID converts a uint64 counter to a term ID string.
func formatTermID(counter uint64) string {
	return "t" + strconv.FormatUint(counter, 10)
}

// parseTermID extracts the counter from a term ID string.
func parseTermID(termID string) uint64 {
	if len(termID) < 2 || termID[0] != 't' {
		return 0
	}
	counter, _ := strconv.ParseUint(termID[1:], 10, 64)
	return counter
}

// cacheKey builds the map key for field+term.
func cacheKey(field, term string) string {
	return field + "\x00" + term
}

// parseCacheKey extracts field and term from a cache key.
func parseCacheKey(key string) (field, term string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

// Term WAL binary format per entry:
// entryLen(4) + crc32(4) + fieldLen(2) + field + termLen(2) + term + counter(8)
const termWALHeaderSize = 4 + 4

// NewCacheFSTRegistry creates a new term registry.
func NewCacheFSTRegistry(config CacheFSTConfig) (*CacheFSTRegistry, error) {
	if config.MergeConfig.MaxCacheSizeMB == 0 {
		config.MergeConfig = DefaultMergeConfig()
	}

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	fstPath := filepath.Join(config.DataDir, "terms.fst")
	walPath := filepath.Join(config.DataDir, "terms.wal")
	metaPath := filepath.Join(config.DataDir, "meta.bin")

	r := &CacheFSTRegistry{
		cache:       make(map[string]uint64, 100_000),
		dfCache:     make(map[string]int64, 100_000),
		dataDir:     config.DataDir,
		fstPath:     fstPath,
		walPath:     walPath,
		metaPath:    metaPath,
		mergeConfig: config.MergeConfig,
		closeCh:     make(chan struct{}),
	}

	// Load metadata (counter, totalDocs)
	r.loadMeta()

	// Load existing FST and populate cache
	if err := r.loadFST(); err != nil {
		// FST doesn't exist yet, that's OK for fresh start
	}
	r.warmCacheFromFST()

	// Replay term WAL to recover terms added since last FST build
	if err := r.replayTermWAL(); err != nil {
		// WAL doesn't exist yet or is empty, OK for fresh start
	}

	// Open term WAL for append
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open term wal: %w", err)
	}
	r.walFile = walFile
	r.walWriter = bufio.NewWriterSize(walFile, 64*1024)

	// Start background worker
	r.wg.Add(1)
	go r.mergeWorker()

	return r, nil
}

// warmCacheFromFST loads all terms from FST into the in-memory cache.
func (r *CacheFSTRegistry) warmCacheFromFST() {
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()

	if fst == nil {
		return
	}

	iter, err := fst.Iterator(nil, nil)
	if err != nil {
		return
	}

	r.cacheMu.Lock()
	for err == nil {
		key, val := iter.Current()
		r.cache[string(key)] = val
		err = iter.Next()
	}
	r.cacheMu.Unlock()
}

// replayTermWAL reads the term WAL and adds entries to the cache.
func (r *CacheFSTRegistry) replayTermWAL() error {
	data, err := os.ReadFile(r.walPath)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}

	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	pos := 0
	var maxCounter uint64

	for pos+termWALHeaderSize <= len(data) {
		entryLen := binary.BigEndian.Uint32(data[pos:])
		storedCRC := binary.BigEndian.Uint32(data[pos+4:])
		pos += termWALHeaderSize

		if pos+int(entryLen) > len(data) {
			break // truncated entry
		}

		entryData := data[pos : pos+int(entryLen)]
		if crc32.ChecksumIEEE(entryData) != storedCRC {
			break // corrupted entry
		}

		// Parse: fieldLen(2) + field + termLen(2) + term + counter(8)
		ep := 0
		if ep+2 > len(entryData) {
			break
		}
		fieldLen := int(binary.BigEndian.Uint16(entryData[ep:]))
		ep += 2
		if ep+fieldLen > len(entryData) {
			break
		}
		field := string(entryData[ep : ep+fieldLen])
		ep += fieldLen

		if ep+2 > len(entryData) {
			break
		}
		termLen := int(binary.BigEndian.Uint16(entryData[ep:]))
		ep += 2
		if ep+termLen > len(entryData) {
			break
		}
		term := string(entryData[ep : ep+termLen])
		ep += termLen

		if ep+8 > len(entryData) {
			break
		}
		counter := binary.BigEndian.Uint64(entryData[ep:])

		r.cache[cacheKey(field, term)] = counter

		if counter > maxCounter {
			maxCounter = counter
		}

		pos += int(entryLen)
	}

	// Ensure counter is at least as high as any replayed term
	if current := r.termIDCounter.Load(); maxCounter > current {
		r.termIDCounter.Store(maxCounter)
	}

	return nil
}

// appendTermWAL writes a new term to the WAL for durability.
func (r *CacheFSTRegistry) appendTermWAL(field, term string, counter uint64) {
	entryLen := 2 + len(field) + 2 + len(term) + 8
	entry := make([]byte, entryLen)
	ep := 0

	binary.BigEndian.PutUint16(entry[ep:], uint16(len(field)))
	ep += 2
	copy(entry[ep:], field)
	ep += len(field)
	binary.BigEndian.PutUint16(entry[ep:], uint16(len(term)))
	ep += 2
	copy(entry[ep:], term)
	ep += len(term)
	binary.BigEndian.PutUint64(entry[ep:], counter)

	checksum := crc32.ChecksumIEEE(entry)

	r.walMu.Lock()
	var header [termWALHeaderSize]byte
	binary.BigEndian.PutUint32(header[:4], uint32(entryLen))
	binary.BigEndian.PutUint32(header[4:], checksum)
	r.walWriter.Write(header[:])
	r.walWriter.Write(entry)
	r.walMu.Unlock()
}

// flushTermWAL flushes the WAL buffer to disk.
func (r *CacheFSTRegistry) flushTermWAL() {
	r.walMu.Lock()
	r.walWriter.Flush()
	r.walMu.Unlock()
}

// Get retrieves a term ID if it exists.
func (r *CacheFSTRegistry) Get(ctx context.Context, field, term string) (string, bool) {
	key := cacheKey(field, term)

	r.cacheMu.RLock()
	counter, ok := r.cache[key]
	r.cacheMu.RUnlock()

	if ok {
		return formatTermID(counter), true
	}
	return "", false
}

// GetOrCreate retrieves an existing term or creates a new one.
// Hot path: pure in-memory map lookup, zero disk I/O for existing terms.
func (r *CacheFSTRegistry) GetOrCreate(ctx context.Context, field, term string) (string, error) {
	key := cacheKey(field, term)

	// Fast path: RLock read from cache
	r.cacheMu.RLock()
	counter, ok := r.cache[key]
	r.cacheMu.RUnlock()

	if ok {
		return formatTermID(counter), nil
	}

	// Slow path: create new term
	r.cacheMu.Lock()
	// Double-check (another goroutine might have created it)
	if counter, ok := r.cache[key]; ok {
		r.cacheMu.Unlock()
		return formatTermID(counter), nil
	}

	counter = r.termIDCounter.Add(1)
	r.cache[key] = counter
	r.cacheMu.Unlock()

	// Append to WAL for durability (buffered, not sync)
	r.appendTermWAL(field, term, counter)

	return formatTermID(counter), nil
}

// CreateAlias creates a term alias pointing to an existing term ID.
func (r *CacheFSTRegistry) CreateAlias(field, newTerm, existingTermID string) error {
	key := cacheKey(field, newTerm)
	counter := parseTermID(existingTermID)

	r.cacheMu.Lock()
	if _, ok := r.cache[key]; ok {
		r.cacheMu.Unlock()
		return nil
	}
	r.cache[key] = counter
	r.cacheMu.Unlock()

	r.appendTermWAL(field, newTerm, counter)
	return nil
}

// GetDF returns the document frequency for a term.
func (r *CacheFSTRegistry) GetDF(termID string) int64 {
	r.dfCacheMu.RLock()
	df := r.dfCache[termID]
	r.dfCacheMu.RUnlock()
	return df
}

// IncrementDF atomically increments the document frequency.
func (r *CacheFSTRegistry) IncrementDF(termID string, delta int64) {
	r.dfCacheMu.Lock()
	r.dfCache[termID] += delta
	r.dfCacheMu.Unlock()
}

// GetTotalDocs returns the total indexed documents.
func (r *CacheFSTRegistry) GetTotalDocs() int64 {
	return r.totalDocs.Load()
}

// IncrementTotalDocs increments the total document count.
func (r *CacheFSTRegistry) IncrementTotalDocs(delta int64) {
	r.totalDocs.Add(delta)
}

// GetTermsWithPrefix returns term IDs matching a prefix.
func (r *CacheFSTRegistry) GetTermsWithPrefix(ctx context.Context, field, prefix string) []string {
	entries, _ := r.ListTermsByPrefix(ctx, field, prefix, 0)
	termIDs := make([]string, len(entries))
	for i, e := range entries {
		termIDs[i] = e.TermID
	}
	return termIDs
}

// GetTermCount returns the total number of terms.
func (r *CacheFSTRegistry) GetTermCount() int64 {
	r.cacheMu.RLock()
	count := int64(len(r.cache))
	r.cacheMu.RUnlock()
	return count
}

// Sync flushes the term WAL to disk.
func (r *CacheFSTRegistry) Sync() error {
	r.flushTermWAL()
	return nil
}

// Close closes the registry.
func (r *CacheFSTRegistry) Close() error {
	close(r.closeCh)
	r.wg.Wait()

	// Final FST rebuild
	r.doMerge()

	// Save metadata
	r.saveMeta()

	// Flush and close WAL
	r.walMu.Lock()
	r.walWriter.Flush()
	r.walFile.Close()
	r.walMu.Unlock()

	// Close FST
	r.fstMu.Lock()
	if r.fst != nil {
		r.fst.Close()
	}
	r.fstMu.Unlock()

	return nil
}

// ListTerms returns all terms, optionally filtered by field.
func (r *CacheFSTRegistry) ListTerms(ctx context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	r.cacheMu.RLock()
	entries := make([]TermEntry, 0, len(r.cache))
	for key, counter := range r.cache {
		f, t := parseCacheKey(key)
		if field == "" || f == field {
			termID := formatTermID(counter)
			entries = append(entries, TermEntry{
				Field:  f,
				Term:   t,
				TermID: termID,
				DF:     r.GetDF(termID),
			})
		}
	}
	r.cacheMu.RUnlock()

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

// ListTermsByPrefix returns terms matching a prefix.
// Uses FST for efficient prefix iteration when available, falls back to cache scan.
func (r *CacheFSTRegistry) ListTermsByPrefix(ctx context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	var entries []TermEntry
	searchKey := field + "\x00" + prefix

	// Try FST first (efficient ordered prefix iteration)
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()

	seen := make(map[string]bool)

	if fst != nil {
		iter, err := fst.Iterator([]byte(searchKey), nil)
		if err == nil {
			for err == nil {
				key, val := iter.Current()
				if !bytes.HasPrefix(key, []byte(searchKey)) {
					break
				}
				f, t := parseCacheKey(string(key))
				termID := formatTermID(val)
				entryKey := string(key)
				seen[entryKey] = true
				entries = append(entries, TermEntry{
					Field:  f,
					Term:   t,
					TermID: termID,
					DF:     r.GetDF(termID),
				})
				if limit > 0 && len(entries) >= limit {
					break
				}
				err = iter.Next()
			}
		}
	}

	// Also scan cache for terms not yet in FST
	r.cacheMu.RLock()
	for key, counter := range r.cache {
		if seen[key] {
			continue
		}
		if !bytes.HasPrefix([]byte(key), []byte(searchKey)) {
			continue
		}
		f, t := parseCacheKey(key)
		termID := formatTermID(counter)
		entries = append(entries, TermEntry{
			Field:  f,
			Term:   t,
			TermID: termID,
			DF:     r.GetDF(termID),
		})
		if limit > 0 && len(entries) >= limit {
			break
		}
	}
	r.cacheMu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// ListTermsByFuzzy returns terms within Levenshtein distance.
func (r *CacheFSTRegistry) ListTermsByFuzzy(ctx context.Context, field, query string, maxDistance int, limit int) ([]TermEntry, error) {
	if maxDistance < 1 {
		maxDistance = 1
	}
	if maxDistance > 3 {
		maxDistance = 3
	}

	var entries []TermEntry

	r.cacheMu.RLock()
	for key, counter := range r.cache {
		f, t := parseCacheKey(key)
		if f != field {
			continue
		}
		if levenshteinDistance(query, t) <= maxDistance {
			termID := formatTermID(counter)
			entries = append(entries, TermEntry{
				Field:  f,
				Term:   t,
				TermID: termID,
				DF:     r.GetDF(termID),
			})
			if limit > 0 && len(entries) >= limit {
				break
			}
		}
	}
	r.cacheMu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// ListTermsByRegex returns terms matching a regex pattern.
func (r *CacheFSTRegistry) ListTermsByRegex(ctx context.Context, field, pattern string, limit int) ([]TermEntry, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}

	var entries []TermEntry

	r.cacheMu.RLock()
	for key, counter := range r.cache {
		f, t := parseCacheKey(key)
		if f != field {
			continue
		}
		if re.MatchString(t) {
			termID := formatTermID(counter)
			entries = append(entries, TermEntry{
				Field:  f,
				Term:   t,
				TermID: termID,
				DF:     r.GetDF(termID),
			})
			if limit > 0 && len(entries) >= limit {
				break
			}
		}
	}
	r.cacheMu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// loadFST loads the FST from disk.
func (r *CacheFSTRegistry) loadFST() error {
	fst, err := vellum.Open(r.fstPath)
	if err != nil {
		return err
	}
	r.fstMu.Lock()
	r.fst = fst
	r.fstMu.Unlock()
	return nil
}

// loadMeta loads metadata from the meta file.
func (r *CacheFSTRegistry) loadMeta() {
	data, err := os.ReadFile(r.metaPath)
	if err != nil || len(data) < 16 {
		return
	}
	r.termIDCounter.Store(binary.BigEndian.Uint64(data[0:8]))
	r.totalDocs.Store(int64(binary.BigEndian.Uint64(data[8:16])))
}

// saveMeta saves metadata to a file.
func (r *CacheFSTRegistry) saveMeta() {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], r.termIDCounter.Load())
	binary.BigEndian.PutUint64(buf[8:16], uint64(r.totalDocs.Load()))
	os.WriteFile(r.metaPath, buf, 0644)
}

// mergeWorker runs the background FST rebuild job.
func (r *CacheFSTRegistry) mergeWorker() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.mergeConfig.MergeCheckInterval)
	defer ticker.Stop()

	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-r.closeCh:
			return
		case <-ticker.C:
			if r.shouldMerge() {
				r.doMerge()
			}
		case <-flushTicker.C:
			r.flushTermWAL()
			r.saveMeta()
		}
	}
}

// shouldMerge checks if FST rebuild triggers are met.
func (r *CacheFSTRegistry) shouldMerge() bool {
	if r.merging.Load() {
		return false
	}

	lastMerge := r.lastMergeTime.Load()
	now := time.Now().Unix()

	if now-lastMerge < int64(r.mergeConfig.MinTimeBetweenMerges.Seconds()) {
		return false
	}

	if now-lastMerge >= int64(r.mergeConfig.MaxTimeSinceLastMerge.Seconds()) {
		return true
	}

	// Check if cache has significantly more terms than FST
	r.fstMu.RLock()
	var fstCount int
	if r.fst != nil {
		fstCount = int(r.fst.Len())
	}
	r.fstMu.RUnlock()

	r.cacheMu.RLock()
	cacheCount := len(r.cache)
	r.cacheMu.RUnlock()

	newTerms := cacheCount - fstCount
	if newTerms >= r.mergeConfig.MaxNewTermCount {
		return true
	}

	return false
}

// doMerge rebuilds the FST from the in-memory cache.
func (r *CacheFSTRegistry) doMerge() {
	if !r.merging.CompareAndSwap(false, true) {
		return
	}
	defer r.merging.Store(false)
	r.lastMergeTime.Store(time.Now().Unix())

	// Snapshot the cache under read lock
	r.cacheMu.RLock()
	keyVals := make(map[string]uint64, len(r.cache))
	for k, v := range r.cache {
		keyVals[k] = v
	}
	r.cacheMu.RUnlock()

	if len(keyVals) == 0 {
		return
	}

	// Sort keys for FST builder
	sortedKeys := make([]string, 0, len(keyVals))
	for k := range keyVals {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Build new FST
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

	newFST, err := vellum.Open(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return
	}

	// Atomic swap
	r.fstMu.Lock()
	oldFST := r.fst
	r.fst = newFST
	r.fstMu.Unlock()

	os.Rename(tempPath, r.fstPath)

	if oldFST != nil {
		oldFST.Close()
	}

	// Truncate WAL (all terms are now in FST)
	r.walMu.Lock()
	r.walWriter.Flush()
	r.walFile.Truncate(0)
	r.walFile.Seek(0, io.SeekStart)
	r.walWriter.Reset(r.walFile)
	r.walMu.Unlock()

	r.saveMeta()
}

// Stats returns registry statistics.
func (r *CacheFSTRegistry) Stats() RegistryStats {
	r.fstMu.RLock()
	var fstCount int64
	if r.fst != nil {
		fstCount = int64(r.fst.Len())
	}
	r.fstMu.RUnlock()

	r.cacheMu.RLock()
	cacheCount := int64(len(r.cache))
	r.cacheMu.RUnlock()

	return RegistryStats{
		FSTTermCount:   fstCount,
		NewTermCount:   cacheCount - fstCount,
		TotalTermCount: cacheCount,
		TotalDocs:      r.totalDocs.Load(),
		IsMerging:      r.merging.Load(),
	}
}

// RegistryStats contains registry statistics.
type RegistryStats struct {
	FSTTermCount   int64 `json:"fst_term_count"`
	NewTermCount   int64 `json:"new_term_count"`
	TotalTermCount int64 `json:"total_term_count"`
	TotalDocs      int64 `json:"total_docs"`
	IsMerging      bool  `json:"is_merging"`
}

// parseFSTKey extracts field and term from FST key format (same as cache key).
func parseFSTKey(key string) (field, term string) {
	return parseCacheKey(key)
}

// Compile-time check
var _ TermRegistry = (*CacheFSTRegistry)(nil)
