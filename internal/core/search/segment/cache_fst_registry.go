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

// CacheFSTRegistry manages the global term dictionary.
//
// Architecture:
//   - Cache map: ALL terms, always. O(1) lookup for the hot path (GetOrCreate/Get).
//     ~50 bytes per term × 1M terms = ~50MB. Loaded from FST at startup.
//   - FST (on disk): compact snapshot of all terms as of the last background merge.
//     NOT on the GetOrCreate/Get hot path. Used for: (a) startup warm-up, (b) advanced
//     queries (fuzzy, regex, prefix range scans), (c) durable persistence across restarts.
//   - Term WAL: durability for terms added after the last FST rebuild.
//
// Hot path (GetOrCreate / Get):
//   O(1) map lookup only. No FST involved. Critical at millions of calls/second.
//
// Background (doMerge):
//   Rebuilds FST from the cache map when enough new terms accumulate or time passes.
//   O(N log N) sort + O(N) FST build — runs at most every MinTimeBetweenMerges.
//   Truncates WAL after FST swap (all terms now durable in FST).
type CacheFSTRegistry struct {
	// cache holds ALL terms: both those in the FST and those added since the last merge.
	// Always the source of truth for GetOrCreate and Get.
	cache   map[string]uint64
	cacheMu sync.RWMutex

	// fstTermCount is the len(cache) at the last successful doMerge().
	// Used by shouldMerge() to detect new terms without iterating the cache.
	fstTermCount atomic.Int64

	// FST: compact term dictionary for persistence and advanced queries (fuzzy/regex/prefix).
	// NOT used on the GetOrCreate/Get hot path.
	fst   *vellum.FST
	fstMu sync.RWMutex

	// Term WAL for durability of delta terms between FST rebuilds.
	walFile   *os.File
	walWriter *bufio.Writer
	walMu     sync.Mutex
	walPath   string

	// Document frequency: global DF needed for BM25 scoring and document updates.
	dfCache   map[string]int64
	dfCacheMu sync.RWMutex

	totalDocs atomic.Int64

	termIDCounter atomic.Uint64

	merging       atomic.Bool
	lastMergeTime atomic.Int64
	mergeConfig   MergeConfig

	dataDir  string
	fstPath  string
	metaPath string

	closeCh chan struct{}
	wg      sync.WaitGroup
}

type MergeConfig struct {
	MaxCacheSizeMB        int
	MaxNewTermCount       int
	MaxTimeSinceLastMerge time.Duration
	MinTimeBetweenMerges  time.Duration
	MergeCheckInterval    time.Duration
}

func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		MaxCacheSizeMB:        64,
		MaxNewTermCount:       100_000,
		MaxTimeSinceLastMerge: 10 * time.Minute,
		MinTimeBetweenMerges:  30 * time.Second,
		MergeCheckInterval:    5 * time.Second,
	}
}

type CacheFSTConfig struct {
	DataDir     string
	MergeConfig MergeConfig
}

func formatTermID(counter uint64) string {
	return "t" + strconv.FormatUint(counter, 10)
}

func parseTermID(termID string) uint64 {
	if len(termID) < 2 || termID[0] != 't' {
		return 0
	}
	counter, _ := strconv.ParseUint(termID[1:], 10, 64)
	return counter
}

func cacheKey(field, term string) string {
	return field + "\x00" + term
}

func parseCacheKey(key string) (field, term string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

const termWALHeaderSize = 4 + 4

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
		cache:       make(map[string]uint64, 1024),
		dfCache:     make(map[string]int64, 100_000),
		dataDir:     config.DataDir,
		fstPath:     fstPath,
		walPath:     walPath,
		metaPath:    metaPath,
		mergeConfig: config.MergeConfig,
		closeCh:     make(chan struct{}),
	}

	r.loadMeta()

	// Load FST and warm the cache map from it — O(1) lookup for all historical terms.
	if err := r.loadFST(); err == nil {
		r.warmCacheFromFST()
	}

	// Replay WAL to restore terms added after the last FST build.
	if err := r.replayTermWAL(); err != nil {
		// WAL missing or empty, fine for fresh start.
	}

	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open term wal: %w", err)
	}
	r.walFile = walFile
	r.walWriter = bufio.NewWriterSize(walFile, 64*1024)

	r.wg.Add(1)
	go r.mergeWorker()

	return r, nil
}

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
			break
		}

		entryData := data[pos : pos+int(entryLen)]
		if crc32.ChecksumIEEE(entryData) != storedCRC {
			break
		}

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

	if current := r.termIDCounter.Load(); maxCounter > current {
		r.termIDCounter.Store(maxCounter)
	}

	return nil
}

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

func (r *CacheFSTRegistry) flushTermWAL() {
	r.walMu.Lock()
	r.walWriter.Flush()
	r.walMu.Unlock()
}

// warmCacheFromFST populates the cache map from the FST at startup.
// After this call, all historical terms are O(1) accessible via the cache.
func (r *CacheFSTRegistry) warmCacheFromFST() {
	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()
	if fst == nil {
		return
	}

	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	iter, err := fst.Iterator(nil, nil)
	count := int64(0)
	for err == nil {
		key, val := iter.Current()
		r.cache[string(key)] = val
		if val > r.termIDCounter.Load() {
			r.termIDCounter.Store(val)
		}
		count++
		err = iter.Next()
	}
	r.fstTermCount.Store(count)
}

// Get retrieves a term ID if it exists. O(1) map lookup.
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
//
// Fast path (existing terms): O(1) map lookup.
// Slow path (new terms): write lock, WAL append.
func (r *CacheFSTRegistry) GetOrCreate(ctx context.Context, field, term string) (string, error) {
	key := cacheKey(field, term)

	// Fast path: O(1) map lookup — covers all terms (historical + recent).
	r.cacheMu.RLock()
	counter, ok := r.cache[key]
	r.cacheMu.RUnlock()
	if ok {
		return formatTermID(counter), nil
	}

	// Slow path: new term — acquire write lock and re-check.
	r.cacheMu.Lock()
	if counter, ok := r.cache[key]; ok {
		r.cacheMu.Unlock()
		return formatTermID(counter), nil
	}
	counter = r.termIDCounter.Add(1)
	r.cache[key] = counter
	r.cacheMu.Unlock()

	r.appendTermWAL(field, term, counter)

	return formatTermID(counter), nil
}

// CreateAlias creates a term alias pointing to an existing term ID.
func (r *CacheFSTRegistry) CreateAlias(field, newTerm, existingTermID string) error {
	key := cacheKey(field, newTerm)
	counter := parseTermID(existingTermID)

	r.cacheMu.RLock()
	_, ok := r.cache[key]
	r.cacheMu.RUnlock()
	if ok {
		return nil
	}

	r.fstMu.RLock()
	fst := r.fst
	r.fstMu.RUnlock()
	if fst != nil {
		if _, exists, err := fst.Get([]byte(key)); err == nil && exists {
			return nil
		}
	}

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

func (r *CacheFSTRegistry) GetDF(termID string) int64 {
	r.dfCacheMu.RLock()
	df := r.dfCache[termID]
	r.dfCacheMu.RUnlock()
	return df
}

func (r *CacheFSTRegistry) IncrementDF(termID string, delta int64) {
	r.dfCacheMu.Lock()
	r.dfCache[termID] += delta
	r.dfCacheMu.Unlock()
}

func (r *CacheFSTRegistry) GetTotalDocs() int64 {
	return r.totalDocs.Load()
}

func (r *CacheFSTRegistry) IncrementTotalDocs(delta int64) {
	r.totalDocs.Add(delta)
}

func (r *CacheFSTRegistry) GetTermsWithPrefix(ctx context.Context, field, prefix string) []string {
	entries, _ := r.ListTermsByPrefix(ctx, field, prefix, 0)
	termIDs := make([]string, len(entries))
	for i, e := range entries {
		termIDs[i] = e.TermID
	}
	return termIDs
}

func (r *CacheFSTRegistry) GetTermCount() int64 {
	r.cacheMu.RLock()
	n := int64(len(r.cache))
	r.cacheMu.RUnlock()
	return n
}

func (r *CacheFSTRegistry) Sync() error {
	r.flushTermWAL()
	return nil
}

func (r *CacheFSTRegistry) Close() error {
	close(r.closeCh)
	r.wg.Wait()

	r.doMerge()
	r.saveMeta()

	r.walMu.Lock()
	r.walWriter.Flush()
	r.walFile.Close()
	r.walMu.Unlock()

	r.fstMu.Lock()
	if r.fst != nil {
		r.fst.Close()
	}
	r.fstMu.Unlock()

	return nil
}

// ListTerms returns all terms from the cache (always complete).
func (r *CacheFSTRegistry) ListTerms(ctx context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	var entries []TermEntry

	r.cacheMu.RLock()
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

// ListTermsByPrefix returns terms matching a prefix from the cache.
func (r *CacheFSTRegistry) ListTermsByPrefix(ctx context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	var entries []TermEntry
	searchKey := field + "\x00" + prefix

	r.cacheMu.RLock()
	for key, counter := range r.cache {
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

// ListTermsByFuzzy returns terms within Levenshtein distance, scanning the cache.
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
		if DamerauLevenshteinDistance(query, t) <= maxDistance {
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

// ListTermsByRegex returns terms matching a regex pattern, scanning the cache.
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

func (r *CacheFSTRegistry) loadMeta() {
	data, err := os.ReadFile(r.metaPath)
	if err != nil || len(data) < 16 {
		return
	}
	r.termIDCounter.Store(binary.BigEndian.Uint64(data[0:8]))
	r.totalDocs.Store(int64(binary.BigEndian.Uint64(data[8:16])))
}

func (r *CacheFSTRegistry) saveMeta() {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], r.termIDCounter.Load())
	binary.BigEndian.PutUint64(buf[8:16], uint64(r.totalDocs.Load()))
	os.WriteFile(r.metaPath, buf, 0644)
}

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

	// Trigger if enough new terms have been added since the last FST rebuild.
	r.cacheMu.RLock()
	currentTerms := int64(len(r.cache))
	r.cacheMu.RUnlock()

	return currentTerms-r.fstTermCount.Load() >= int64(r.mergeConfig.MaxNewTermCount)
}

// doMerge rebuilds the FST from the full cache map (all terms), then truncates the WAL.
// The cache map is never modified — it remains the O(1) hot path.
// After the FST swap, fstTermCount is updated for shouldMerge() tracking.
func (r *CacheFSTRegistry) doMerge() {
	if !r.merging.CompareAndSwap(false, true) {
		return
	}
	defer r.merging.Store(false)
	r.lastMergeTime.Store(time.Now().Unix())

	// Snapshot the full cache under a read lock.
	r.cacheMu.RLock()
	snapshot := make(map[string]uint64, len(r.cache))
	for k, v := range r.cache {
		snapshot[k] = v
	}
	r.cacheMu.RUnlock()

	if len(snapshot) == 0 {
		return
	}

	sortedKeys := make([]string, 0, len(snapshot))
	for k := range snapshot {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

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
		if err := builder.Insert([]byte(key), snapshot[key]); err != nil {
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

	// Atomic FST swap.
	r.fstMu.Lock()
	oldFSTToClose := r.fst
	r.fst = newFST
	r.fstMu.Unlock()

	os.Rename(tempPath, r.fstPath)

	if oldFSTToClose != nil {
		oldFSTToClose.Close()
	}

	// Update term count so shouldMerge() knows how many terms are in the FST.
	r.fstTermCount.Store(int64(len(snapshot)))

	// Truncate WAL — all terms in the snapshot are now durably in the FST file.
	// Terms added to the cache after the snapshot was taken will re-appear in the WAL.
	r.walMu.Lock()
	r.walWriter.Flush()
	r.walFile.Truncate(0)
	r.walFile.Seek(0, io.SeekStart)
	r.walWriter.Reset(r.walFile)
	r.walMu.Unlock()

	r.saveMeta()
}

func (r *CacheFSTRegistry) Stats() RegistryStats {
	r.cacheMu.RLock()
	total := int64(len(r.cache))
	r.cacheMu.RUnlock()

	fstCount := r.fstTermCount.Load()

	return RegistryStats{
		FSTTermCount:   fstCount,
		NewTermCount:   total - fstCount,
		TotalTermCount: total,
		TotalDocs:      r.totalDocs.Load(),
		IsMerging:      r.merging.Load(),
	}
}

type RegistryStats struct {
	FSTTermCount   int64 `json:"fst_term_count"`
	NewTermCount   int64 `json:"new_term_count"`
	TotalTermCount int64 `json:"total_term_count"`
	TotalDocs      int64 `json:"total_docs"`
	IsMerging      bool  `json:"is_merging"`
}

func parseFSTKey(key string) (field, term string) {
	return parseCacheKey(key)
}

var _ TermRegistry = (*CacheFSTRegistry)(nil)
