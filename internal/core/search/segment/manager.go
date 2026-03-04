package segment

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	rmetrics "runtime/metrics"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/core/search/knowledge"
	"plastic-engine-core/internal/core/search/wal"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// mergeHeapItem represents a term from one segment in the merge heap.
type mergeHeapItem struct {
	termID   string
	postings []Posting
	df       int64
	iterIdx  int // which iterator this came from
}

// mergeHeap implements heap.Interface for k-way merge.
type mergeHeap []*mergeHeapItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].termID < h[j].termID }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*mergeHeapItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return item
}

// mergeJob represents a merge task for the merge pool.
type mergeJob struct {
	segments []*DiskSegment
	level    int
}

// termAccumulator buffers new term deltas for background drain to the adjacency matrix.
type termAccumulator struct {
	mu    sync.Mutex
	terms map[string]int64 // field\x00term → df delta
}

// Add adds a DF delta for a term key.
func (a *termAccumulator) Add(termKey string, delta int64) {
	a.mu.Lock()
	a.terms[termKey] += delta
	a.mu.Unlock()
}

// AddBatch merges a map of DF deltas under a single lock acquisition.
func (a *termAccumulator) AddBatch(deltas map[string]int64) {
	a.mu.Lock()
	for k, v := range deltas {
		a.terms[k] += v
	}
	a.mu.Unlock()
}

// Drain returns and resets the accumulated deltas.
func (a *termAccumulator) Drain() map[string]int64 {
	a.mu.Lock()
	if len(a.terms) == 0 {
		a.mu.Unlock()
		return nil
	}
	drained := a.terms
	a.terms = make(map[string]int64, len(drained))
	a.mu.Unlock()
	return drained
}

// Manager orchestrates segments, handling indexing, searching, and merging.
type Manager struct {
	activeMu sync.Mutex   // protects: m.active (ingestion hot path)
	segMu    sync.RWMutex // protects: m.segments (read by search, written by flush/merge)

	// Active segment (receives writes)
	active *MemSegment

	// Disk segments (immutable, for reads)
	segments []*DiskSegment

	// Term registry (optional, kept for backward compatibility)
	registry TermRegistry
	// ownsRegistry is true if this Manager created the registry and should close it
	ownsRegistry bool

	// WAL for the active segment
	wal *wal.WAL

	// Configuration
	config Config

	// Manifest tracks segments
	manifest *Manifest

	// Term accumulator: buffers new terms + DF deltas for background drain.
	accumulator *termAccumulator

	// Adjacency matrix for term relationships (optional, set via SetMatrix).
	matrix *knowledge.AdjacencyMatrix

	// Total indexed documents (local counter, no registry dependency).
	totalDocs atomic.Int64

	// Background workers
	flushCh    chan struct{}
	mergeCh    chan struct{}   // signal to check for merges
	mergeJobCh chan mergeJob   // merge jobs for pool workers
	closeCh    chan struct{}
	wg         sync.WaitGroup
	closed     atomic.Bool

	// I/O semaphore: bounds concurrent flush + merge disk I/O to prevent
	// throughput collapse from competing writes.
	ioSema chan struct{}

	// Track segments being merged to avoid double-selection
	mergingMu sync.Mutex
	merging   map[string]bool

	// Heap pressure monitor: 0=none, 1=medium (reduce thresholds), 2=high (emergency flush)
	pressureLevel atomic.Int32
	memLimitBytes int64 // GOMEMLIMIT in bytes; 0 = monitor disabled

	// Stats
	indexedDocs atomic.Int64
	searches    atomic.Int64
}

// Manifest tracks which segments exist.
type Manifest struct {
	mu sync.RWMutex

	Segments  []ManifestEntry `json:"segments"`
	ActiveID  string          `json:"active_id"`
	Version   int64           `json:"version"`
	UpdatedAt time.Time       `json:"updated_at"`

	path string
}

// ManifestEntry describes a segment in the manifest.
type ManifestEntry struct {
	ID        string    `json:"id"`
	Path      string    `json:"path"`
	DocCount  int       `json:"doc_count"`
	TermCount int       `json:"term_count"`
	Level     int       `json:"level"`
	CreatedAt time.Time `json:"created_at"`
	SizeBytes int64     `json:"size_bytes"`
}

// NewManager creates a new segment manager.
func NewManager(config Config) (*Manager, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// Use external registry if provided, otherwise create our own
	var registry TermRegistry
	var ownsRegistry bool

	if config.TermRegistry != nil {
		// Use the provided external registry (shared across shards)
		registry = config.TermRegistry
		ownsRegistry = false
	} else {
		// Create a Pebble-backed term registry.
		// Data lives in <DataDir>/registry/pebble/ — separate from old FST files.
		registryDir := filepath.Join(config.DataDir, "registry")

		newRegistry, err := NewPebbleTermRegistry(PebbleRegistryConfig{
			DataDir: registryDir,
		})
		if err != nil {
			return nil, fmt.Errorf("create term registry: %w", err)
		}
		registry = newRegistry
		ownsRegistry = true
	}

	// Initialize WAL
	// SyncNone: WAL buffers writes without fsync. Durability comes from segment flushes
	// (WAL is truncated after each flush). SyncBatch was doing fsync every 1s via syncWorker,
	// which could stall indexing for 5-50ms when it coincided with AppendDocuments.
	// 256KB buffer absorbs larger batches without flushing to the OS page cache mid-write.
	walPath := filepath.Join(config.DataDir, "active.wal")
	activeWAL, err := wal.Open(walPath, wal.Options{
		SyncMode:   wal.SyncNone,
		BufferSize: 256 * 1024,
	})
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	// Apply defaults for zero-value config fields
	if config.FlushInterval <= 0 {
		config.FlushInterval = 10 * time.Second
	}
	if config.MergeInterval <= 0 {
		config.MergeInterval = 30 * time.Second
	}
	if config.MergeWorkers <= 0 {
		config.MergeWorkers = 2
	}
	ioConcurrency := config.IOConcurrency
	if ioConcurrency < 2 {
		ioConcurrency = 2
	}

	m := &Manager{
		registry:     registry,
		ownsRegistry: ownsRegistry,
		wal:          activeWAL,
		config:       config,
		accumulator:  &termAccumulator{terms: make(map[string]int64, 1024)},
		flushCh:      make(chan struct{}, 1),
		mergeCh:      make(chan struct{}, 1),
		mergeJobCh:   make(chan mergeJob, config.MergeWorkers*2),
		closeCh:      make(chan struct{}),
		ioSema:       make(chan struct{}, ioConcurrency),
		merging:      make(map[string]bool),
	}

	// Read GOMEMLIMIT for the heap monitor (0 if not configured = disabled).
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < int64(^uint64(0)>>1) {
		m.memLimitBytes = limit
	}

	// Load or create manifest
	m.manifest = &Manifest{
		path: filepath.Join(config.DataDir, "manifest.json"),
	}
	if err := m.manifest.Load(); err != nil {
		// Create new manifest
		m.manifest.Version = 1
		m.manifest.UpdatedAt = time.Now().UTC()
	}

	// Load existing segments
	if err := m.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	// Create active segment
	activeID := fmt.Sprintf("mem_%d", time.Now().UnixNano())
	m.active = NewMemSegment(activeID)
	m.manifest.ActiveID = activeID

	// Replay WAL to recover active segment
	if err := m.replayWAL(); err != nil {
		return nil, fmt.Errorf("replay wal: %w", err)
	}

	// Recover totalDocs from disk segments + active segment.
	var totalDocs int64
	for _, seg := range m.segments {
		totalDocs += int64(seg.DocCount())
	}
	totalDocs += int64(m.active.DocCount())
	m.totalDocs.Store(totalDocs)

	// Start background workers
	m.startBackgroundWorkers()

	return m, nil
}

// loadSegments loads all disk segments from the manifest.
func (m *Manager) loadSegments() error {
	m.manifest.mu.RLock()
	entries := make([]ManifestEntry, len(m.manifest.Segments))
	copy(entries, m.manifest.Segments)
	m.manifest.mu.RUnlock()

	for _, entry := range entries {
		seg, err := OpenDiskSegment(entry.Path)
		if err != nil {
			// Log and skip corrupt segments
			continue
		}
		m.segments = append(m.segments, seg)
	}

	// Sort by level then creation time
	sort.Slice(m.segments, func(i, j int) bool {
		if m.segments[i].meta.Level != m.segments[j].meta.Level {
			return m.segments[i].meta.Level < m.segments[j].meta.Level
		}
		return m.segments[i].meta.CreatedAt.Before(m.segments[j].meta.CreatedAt)
	})

	return nil
}

// replayWAL replays the WAL to recover the active segment.
func (m *Manager) replayWAL() error {
	walPath := filepath.Join(m.config.DataDir, "active.wal")

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
		switch entry.Type {
		case wal.OpIndex:
			// Legacy per-term format
			var op wal.IndexOp
			if err := json.Unmarshal(entry.Data, &op); err != nil {
				continue
			}
			m.active.Add(op.TermID, op.DocID, op.TF, op.Positions)

		case wal.OpIndexDoc:
			// New batch format: entire document in one entry
			op, err := wal.DecodeDocIndexOp(entry.Data)
			if err != nil {
				continue
			}
			for _, field := range op.Fields {
				for _, p := range field.Postings {
					m.active.Add(p.TermID, op.DocID, p.TF, p.Positions)
				}
			}
		}
	}

	return nil
}

// startBackgroundWorkers starts flush, merge, and sync workers.
func (m *Manager) startBackgroundWorkers() {
	// Periodic flush worker
	m.wg.Add(1)
	go m.flushWorker()

	// Merge scheduler (checks for merge candidates)
	m.wg.Add(1)
	go m.mergeScheduler()

	// Dedicated merge pool workers
	for i := 0; i < m.config.MergeWorkers; i++ {
		m.wg.Add(1)
		go m.mergePoolWorker()
	}

	// Periodic sync worker
	m.wg.Add(1)
	go m.syncWorker()

	// Real-time heap pressure monitor (safety net for unaccounted memory)
	m.wg.Add(1)
	go m.heapMonitor()

	// Term accumulator drain worker (pushes to adjacency matrix)
	m.wg.Add(1)
	go m.drainWorker()
}

// flushWorker handles flushing the active segment.
func (m *Manager) flushWorker() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			m.maybeFlush()
		case <-m.flushCh:
			m.doFlush()
		}
	}
}

// mergeScheduler checks for merge candidates and submits jobs to the pool.
func (m *Manager) mergeScheduler() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.MergeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			m.scheduleMerge()
		case <-m.mergeCh:
			m.scheduleMerge()
		}
	}
}

// mergePoolWorker processes merge jobs from the pool.
func (m *Manager) mergePoolWorker() {
	defer m.wg.Done()

	for {
		select {
		case <-m.closeCh:
			return
		case job := <-m.mergeJobCh:
			m.executeMerge(job)
		}
	}
}

// syncWorker periodically syncs WAL and registry.
func (m *Manager) syncWorker() {
	defer m.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			m.wal.Sync()
			if m.registry != nil {
				m.registry.Sync()
			}
		}
	}
}

// heapMonitor watches runtime heap usage and sets pressureLevel.
// Two levels with hysteresis:
//   - medium (1): heap > 70%, exit below 60% — halves flush thresholds proactively
//   - high   (2): heap > 85%, exit below 75% — emergency flush + backpressure
//
// Uses runtime/metrics (non-STW) instead of runtime.ReadMemStats.
func (m *Manager) heapMonitor() {
	defer m.wg.Done()
	if m.memLimitBytes <= 0 {
		return
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	sample := []rmetrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			rmetrics.Read(sample)
			heapInuse := int64(sample[0].Value.Uint64())
			ratio := float64(heapInuse) / float64(m.memLimitBytes)

			prev := m.pressureLevel.Load()
			var level int32
			switch {
			case ratio > 0.85 || (prev >= 2 && ratio > 0.75):
				level = 2 // high: emergency flush + backpressure
			case ratio > 0.70 || (prev >= 1 && ratio > 0.60):
				level = 1 // medium: reduce flush thresholds
			default:
				level = 0
			}
			m.pressureLevel.Store(level)

			if level >= 2 {
				select {
				case m.flushCh <- struct{}{}:
				default:
				}
			}
		}
	}
}

// drainWorker periodically drains the term accumulator to the adjacency matrix.
func (m *Manager) drainWorker() {
	defer m.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			// Final drain on shutdown
			m.drainTermsToMatrix()
			return
		case <-ticker.C:
			m.drainTermsToMatrix()
		}
	}
}

// Index adds a document posting to the index.
func (m *Manager) Index(ctx context.Context, field, term, docID string, tf int, positions []int) error {
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}

	// Deterministic term key — no registry lookup, zero coordination.
	termKey := field + "\x00" + term

	// Log to WAL
	if _, err := m.wal.AppendIndex(wal.IndexOp{
		TermID:    termKey,
		DocID:     docID,
		TF:        tf,
		Positions: positions,
		Field:     field,
		Term:      term,
	}); err != nil {
		return fmt.Errorf("append to wal: %w", err)
	}

	// Add to active segment
	m.activeMu.Lock()
	m.active.Add(termKey, docID, tf, positions)
	docCount := m.active.DocCount()
	estimatedBytes := m.active.EstimatedBytes()
	m.activeMu.Unlock()

	// Buffer term delta for background drain to adjacency matrix.
	m.accumulator.Add(termKey, 1)
	m.totalDocs.Add(1)

	// Check if we need to flush (by docs or by bytes)
	if m.needsFlush(docCount, estimatedBytes) {
		select {
		case m.flushCh <- struct{}{}:
		default:
		}
	}

	m.indexedDocs.Add(1)
	return nil
}

// DocumentBatch represents a document with its field terms for batch indexing.
type DocumentBatch struct {
	DocID      string
	FieldTerms map[string][]TermPosting
}

// IndexDocumentBatch indexes multiple documents in a single optimized pass.
// This is much faster than calling IndexDocument N times because:
//   - Term lookups are deduplicated across all documents (each unique term resolved once)
//   - WAL writes use a single lock acquisition for the entire batch
//   - Segment adds use a single lock acquisition via AddBatch
//   - DF increments are aggregated per unique term
var mgrTracer = otel.Tracer("search/segment-manager")

func (m *Manager) IndexDocumentBatch(ctx context.Context, docs []DocumentBatch) error {
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}
	if len(docs) == 0 {
		return nil
	}

	// Proportional backpressure from two independent signals:
	// A) Heap pressure (heapMonitor): global memory nearing GOMEMLIMIT
	// B) MemSegment fill ratio: local segment approaching flush threshold
	//
	// Signal B catches the case where heap is fine globally (e.g. 30% used)
	// but a single MemSegment is growing toward 285MB and about to trigger
	// a massive flush + GC storm.
	m.activeMu.Lock()
	estBytes := m.active.EstimatedBytes()
	m.activeMu.Unlock()

	flushBytes := m.config.FlushThresholdBytes
	if flushBytes <= 0 {
		flushBytes = 16 * 1024 * 1024
	}
	fillRatio := float64(estBytes*2) / float64(flushBytes) // 2x correction matches needsFlush

	pressure := m.pressureLevel.Load()

	if pressure >= 2 || fillRatio >= 0.9 {
		// High pressure: force flush + proportional sleep 5-55ms.
		select {
		case m.flushCh <- struct{}{}:
		default:
		}
		r := fillRatio
		if r > 1.0 {
			r = 1.0
		}
		sleepMs := 5 + int(r*50)
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	} else if pressure >= 1 || fillRatio >= 0.7 {
		// Medium pressure: light yield to let flush catch up.
		time.Sleep(1 * time.Millisecond)
	}

	// Pre-calculate total postings to pre-allocate allEntries in one shot.
	totalPostings := 0
	for _, doc := range docs {
		for _, terms := range doc.FieldTerms {
			totalPostings += len(terms)
		}
	}

	// Phase 1: Build segment entries + WAL data using deterministic term keys.
	// No registry lookup needed — termKey = field + "\x00" + term (zero coordination).
	_, buildSpan := mgrTracer.Start(ctx, "index.build_entries")
	buildSpan.SetAttributes(
		attribute.Int("docs", len(docs)),
		attribute.Int("postings", totalPostings),
	)

	allEntries := make([]SegmentEntry, 0, totalPostings)
	walEncoded := make([][]byte, 0, len(docs))
	dfDeltas := make(map[string]int64, totalPostings/2)

	// Intern table: field → term → interned "field\x00term" key.
	// For 1k docs with ~850 unique (field,term) pairs, this reduces
	// string concatenations from ~50k to ~850.
	internedKeys := make(map[string]map[string]string)
	getTermKey := func(field, term string) string {
		byTerm, ok := internedKeys[field]
		if !ok {
			byTerm = make(map[string]string)
			internedKeys[field] = byTerm
		}
		if key, ok := byTerm[term]; ok {
			return key
		}
		key := field + "\x00" + term
		byTerm[term] = key
		return key
	}

	for _, doc := range docs {
		walFields := make([]wal.DocFieldTerms, 0, len(doc.FieldTerms))

		for field, terms := range doc.FieldTerms {
			walPostings := make([]wal.DocTermPosting, 0, len(terms))

			for _, tp := range terms {
				termKey := getTermKey(field, tp.Term)

				allEntries = append(allEntries, SegmentEntry{
					TermID:    termKey,
					DocID:     doc.DocID,
					TF:        tp.TF,
					Positions: tp.Positions,
				})

				walPostings = append(walPostings, wal.DocTermPosting{
					TermID:    termKey,
					Term:      tp.Term,
					TF:        tp.TF,
					Positions: tp.Positions,
				})

				dfDeltas[termKey]++
			}

			walFields = append(walFields, wal.DocFieldTerms{
				Field:    field,
				Postings: walPostings,
			})
		}

		walEncoded = append(walEncoded, wal.EncodeDocIndexOp(wal.DocIndexOp{
			DocID:  doc.DocID,
			Fields: walFields,
		}))
	}
	buildSpan.End()

	// Phase 2: WAL write
	_, walSpan := mgrTracer.Start(ctx, "index.wal_write")
	walSpan.SetAttributes(attribute.Int("docs", len(walEncoded)))
	if _, err := m.wal.AppendPreEncodedDocuments(walEncoded); err != nil {
		walSpan.End()
		return fmt.Errorf("append docs to wal: %w", err)
	}
	walEncoded = nil
	walSpan.End()

	// Phase 3: AddBatch to MemSegment
	_, addSpan := mgrTracer.Start(ctx, "index.add_batch")
	addSpan.SetAttributes(attribute.Int("entries", len(allEntries)))
	m.activeMu.Lock()
	m.active.AddBatch(allEntries)
	docCount := m.active.DocCount()
	estimatedBytes := m.active.EstimatedBytes()
	m.activeMu.Unlock()
	addSpan.End()

	// Phase 4: Buffer term deltas for background drain to adjacency matrix.
	m.accumulator.AddBatch(dfDeltas)
	m.totalDocs.Add(int64(len(docs)))
	m.indexedDocs.Add(int64(len(docs)))

	if m.needsFlush(docCount, estimatedBytes) {
		select {
		case m.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// IndexDocument indexes all terms for a single document.
// For batches, prefer IndexDocumentBatch which is significantly faster.
func (m *Manager) IndexDocument(ctx context.Context, docID string, fieldTerms map[string][]TermPosting) error {
	return m.IndexDocumentBatch(ctx, []DocumentBatch{{DocID: docID, FieldTerms: fieldTerms}})
}

// TermPosting is a term with its posting data for indexing.
type TermPosting struct {
	Term      string
	TF        int
	Positions []int
}

// Search finds documents matching a term.
func (m *Manager) Search(_ context.Context, field, term string) ([]Hit, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("manager is closed")
	}

	m.searches.Add(1)

	// Deterministic key — no registry lookup.
	termKey := field + "\x00" + term
	return m.SearchByTermID(termKey)
}

// searchSema limits concurrent disk-segment search goroutines to prevent goroutine explosion
// under concurrent queries with many segments. Scales with GOMAXPROCS (min 8).
var searchSema = func() chan struct{} {
	n := runtime.GOMAXPROCS(0)
	if n < 8 {
		n = 8
	}
	return make(chan struct{}, n)
}()

// SearchByTermID searches using a term ID directly.
func (m *Manager) SearchByTermID(termID string) ([]Hit, error) {
	m.activeMu.Lock()
	active := m.active
	m.activeMu.Unlock()

	m.segMu.RLock()
	segments := make([]*DiskSegment, len(m.segments))
	copy(segments, m.segments)
	m.segMu.RUnlock()

	// Pre-allocate results per segment (index-based, no mutex needed).
	segResults := make([][]Hit, len(segments)+1) // +1 for active segment
	segResults[0] = active.Search(termID)

	// Search disk segments in parallel (bounded concurrency)
	var wg sync.WaitGroup
	for i, seg := range segments {
		wg.Add(1)
		searchSema <- struct{}{} // acquire slot
		go func(idx int, s *DiskSegment) {
			defer wg.Done()
			defer func() { <-searchSema }() // release slot
			segResults[idx+1] = s.Search(termID)
		}(i, seg)
	}
	wg.Wait()

	// Merge without mutex — count total first, then single allocation.
	total := 0
	for _, r := range segResults {
		total += len(r)
	}
	if total == 0 {
		return nil, nil
	}
	allHits := make([]Hit, 0, total)
	for _, r := range segResults {
		allHits = append(allHits, r...)
	}
	return allHits, nil
}

// GetDF returns the global document frequency for a term, computed from all segments.
func (m *Manager) GetDF(termID string) int64 {
	m.activeMu.Lock()
	total := m.active.GetLocalDF(termID)
	m.activeMu.Unlock()

	m.segMu.RLock()
	segments := make([]*DiskSegment, len(m.segments))
	copy(segments, m.segments)
	m.segMu.RUnlock()

	if len(segments) <= 4 {
		// Sequential for small segment counts (goroutine overhead > savings).
		for _, seg := range segments {
			total += seg.GetLocalDF(termID)
		}
		return total
	}

	// Parallel for many segments.
	results := make([]int64, len(segments))
	var wg sync.WaitGroup
	for i, seg := range segments {
		wg.Add(1)
		go func(idx int, s *DiskSegment) {
			defer wg.Done()
			results[idx] = s.GetLocalDF(termID)
		}(i, seg)
	}
	wg.Wait()
	for _, r := range results {
		total += r
	}
	return total
}

// GetTotalDocs returns the total number of indexed documents.
func (m *Manager) GetTotalDocs() int64 {
	return m.totalDocs.Load()
}

// CreateAlias creates a term alias.
func (m *Manager) CreateAlias(field, newTerm, existingTermID string) error {
	if m.registry == nil {
		return fmt.Errorf("no registry configured for alias support")
	}
	return m.registry.CreateAlias(field, newTerm, existingTermID)
}

// SetMatrix attaches an adjacency matrix to this manager.
// The background drain goroutine will push accumulated term deltas to it.
func (m *Manager) SetMatrix(matrix *knowledge.AdjacencyMatrix) {
	m.matrix = matrix
}

// Matrix returns the adjacency matrix (may be nil).
func (m *Manager) Matrix() *knowledge.AdjacencyMatrix {
	return m.matrix
}

// drainTermsToMatrix drains accumulated term deltas to the adjacency matrix.
func (m *Manager) drainTermsToMatrix() {
	if m.matrix == nil {
		return
	}
	deltas := m.accumulator.Drain()
	if len(deltas) == 0 {
		return
	}
	// Best-effort: log errors but don't block indexing.
	_ = m.matrix.IngestTermBatch(deltas)
}

// UpdateFlushThresholds allows the shard manager to adjust flush thresholds
// after re-tuning with the real shard count (known after first Sync).
func (m *Manager) UpdateFlushThresholds(flushBytes int64, flushDocs int) {
	m.config.FlushThresholdBytes = flushBytes
	m.config.FlushThreshold = flushDocs
}

// needsFlush returns true if the active segment should be flushed.
func (m *Manager) needsFlush(docCount int, estimatedBytes int64) bool {
	pressure := m.pressureLevel.Load()
	if pressure >= 2 {
		return true // emergency
	}

	// estimatedBytes subestima heap real de Go por map/string/GC overhead.
	// 2x correction: AddBatch is now append-only (no dedup maps), reducing alloc overhead.
	correctedBytes := estimatedBytes * 2

	flushDocs := m.config.FlushThreshold
	flushBytes := m.config.FlushThresholdBytes
	if pressure >= 1 {
		flushDocs /= 2   // flush at half thresholds under medium pressure
		flushBytes /= 2
	}

	if docCount >= flushDocs {
		return true
	}
	if flushBytes > 0 && correctedBytes >= flushBytes {
		return true
	}
	return false
}

// maybeFlush checks if flush is needed.
func (m *Manager) maybeFlush() {
	m.activeMu.Lock()
	docCount := m.active.DocCount()
	estimatedBytes := m.active.EstimatedBytes()
	m.activeMu.Unlock()

	if m.needsFlush(docCount, estimatedBytes) {
		m.doFlush()
	}
}

// doFlush flushes the active segment to disk.
func (m *Manager) doFlush() {
	m.activeMu.Lock()
	if m.active.DocCount() == 0 {
		m.activeMu.Unlock()
		return
	}

	// Swap active segment — capture old sizes for capacity hints.
	oldActive := m.active
	docCount := oldActive.DocCount()
	termCount := oldActive.TermCount()
	newActiveID := fmt.Sprintf("mem_%d", time.Now().UnixNano())
	// Pre-allocate at 75% of previous generation to avoid map growth churn.
	m.active = NewMemSegmentWithHints(newActiveID, termCount*3/4, docCount*3/4)
	m.manifest.ActiveID = newActiveID
	m.activeMu.Unlock()

	flushStart := time.Now()

	// Flush old active to disk (bounded by I/O semaphore).
	m.ioSema <- struct{}{} // acquire I/O slot
	segmentDir := filepath.Join(m.config.DataDir, "segments")
	diskSeg, err := FlushMemSegment(oldActive, segmentDir)
	<-m.ioSema // release I/O slot
	if err != nil {
		// Put it back if flush failed
		m.activeMu.Lock()
		m.active = oldActive
		m.activeMu.Unlock()
		return
	}

	// Release old MemSegment maps immediately so GC can collect them
	// before the next allocation spike.
	oldActive.Clear()

	// Record flush metrics
	if mp := metrics.Global(); mp != nil {
		mp.RecordSegmentFlush(context.Background(), docCount, time.Since(flushStart))
	}

	// Add to segments list
	m.segMu.Lock()
	m.segments = append(m.segments, diskSeg)
	m.segMu.Unlock()

	// Update manifest
	m.manifest.mu.Lock()
	m.manifest.Segments = append(m.manifest.Segments, ManifestEntry{
		ID:        diskSeg.ID(),
		Path:      diskSeg.Path(),
		DocCount:  diskSeg.DocCount(),
		TermCount: diskSeg.TermCount(),
		Level:     0,
		CreatedAt: diskSeg.Meta().CreatedAt,
		SizeBytes: diskSeg.Meta().SizeBytes,
	})
	m.manifest.Version++
	m.manifest.UpdatedAt = time.Now().UTC()
	m.manifest.mu.Unlock()

	// Save manifest
	m.manifest.Save()

	// Rotate WAL in background — the segment is already persisted and manifest saved,
	// so new writes to WAL will just be replayed harmlessly if we crash before rotation.
	// RotateTruncate holds the lock for ~1μs (vs 1-10ms for Truncate).
	go m.wal.RotateTruncate()

	// Trigger merge check
	select {
	case m.mergeCh <- struct{}{}:
	default:
	}
}

// scheduleMerge finds merge candidates and submits jobs to the merge pool.
// Runs in the merge scheduler goroutine — no heavy I/O here.
func (m *Manager) scheduleMerge() {
	m.segMu.RLock()
	segments := make([]*DiskSegment, len(m.segments))
	copy(segments, m.segments)
	m.segMu.RUnlock()

	m.mergingMu.Lock()
	defer m.mergingMu.Unlock()

	// Group by level, excluding segments already being merged
	levels := make(map[int][]*DiskSegment)
	for _, seg := range segments {
		if m.merging[seg.ID()] {
			continue
		}
		level := seg.Meta().Level
		levels[level] = append(levels[level], seg)
	}

	// Submit merge jobs for levels that exceed MaxSegmentsPerLevel.
	// Merge all segments at the level (up to MaxSegmentsPerLevel) to clear the
	// backlog completely. With maxMergeBatch=3 (previous), L0 never fully cleared:
	// 5 segs → merge 3 → 2+1=3 left < threshold → stall until 5 again.
	// Now: 5 segs → merge 5 → 1 at L1, L0 is clean.
	for level, segs := range levels {
		if len(segs) >= m.config.MaxSegmentsPerLevel {
			maxBatch := m.config.MaxSegmentsPerLevel
			if level > 0 {
				maxBatch = 3 // larger segments → smaller batches to bound merge time
			}
			batch := segs
			if len(batch) > maxBatch {
				batch = batch[:maxBatch]
			}

			// Mark segments as merging
			for _, seg := range batch {
				m.merging[seg.ID()] = true
			}

			select {
			case m.mergeJobCh <- mergeJob{segments: batch, level: level + 1}:
			default:
				// Pool busy, unmark and try later
				for _, seg := range batch {
					delete(m.merging, seg.ID())
				}
			}
		}
	}
}

// executeMerge performs a merge job. Runs in a merge pool worker goroutine.
// The heavy I/O (mergeSegments) runs without holding m.mu.
func (m *Manager) executeMerge(job mergeJob) {
	mergeStart := time.Now()

	// Phase 1: Merge segments (I/O heavy, bounded by sema)
	m.ioSema <- struct{}{} // acquire I/O slot
	merged, err := m.mergeSegments(job.segments, job.level)
	<-m.ioSema // release I/O slot

	// Unmark merging segments regardless of success
	m.mergingMu.Lock()
	for _, seg := range job.segments {
		delete(m.merging, seg.ID())
	}
	m.mergingMu.Unlock()

	if err != nil {
		return
	}

	// Phase 2: Swap segments under write lock (fast, no I/O)
	oldIDs := make(map[string]bool, len(job.segments))
	for _, seg := range job.segments {
		oldIDs[seg.ID()] = true
	}

	m.segMu.Lock()
	capHint := len(m.segments) - len(job.segments) + 1
	if capHint < 1 {
		capHint = 1
	}
	newSegments := make([]*DiskSegment, 0, capHint)
	oldPaths := make([]string, 0, len(job.segments))

	for _, s := range m.segments {
		if oldIDs[s.ID()] {
			oldPaths = append(oldPaths, s.Path())
		} else {
			newSegments = append(newSegments, s)
		}
	}
	newSegments = append(newSegments, merged)
	m.segments = newSegments
	m.segMu.Unlock()

	// Phase 3: Update manifest
	m.updateManifestAfterMerge(job.segments, merged)

	// Phase 4: Close old segments immediately to free termDict + file handles,
	// but delay file deletion for in-flight readers.
	for _, seg := range job.segments {
		seg.Close()
	}
	go func(paths []string) {
		time.Sleep(5 * time.Second)
		for _, path := range paths {
			os.Remove(path)
		}
	}(oldPaths)

	// Record merge metrics
	if mp := metrics.Global(); mp != nil {
		mp.RecordSegmentMerge(context.Background(), len(job.segments), time.Since(mergeStart))
	}

	// Trigger another merge check (merged segment may enable cascading merges)
	select {
	case m.mergeCh <- struct{}{}:
	default:
	}
}

// mergeSegments merges multiple segments into one using streaming k-way merge.
// This uses O(K) memory where K is the number of segments, instead of O(N) where N is total postings.
func (m *Manager) mergeSegments(segments []*DiskSegment, newLevel int) (*DiskSegment, error) {
	newID := fmt.Sprintf("merged_%d", time.Now().UnixNano())
	segmentDir := filepath.Join(m.config.DataDir, "segments")

	writer, err := NewDiskSegmentWriter(segmentDir, newID)
	if err != nil {
		return nil, err
	}

	// Collect all iterators
	iters := make([]SegmentIterator, len(segments))
	for i, seg := range segments {
		iters[i] = seg.Iterator()
	}
	defer func() {
		for _, iter := range iters {
			if iter != nil {
				iter.Close()
			}
		}
	}()

	// Count total docs
	totalDocs := 0
	for _, seg := range segments {
		totalDocs += seg.DocCount()
	}

	// Initialize min-heap with first term from each iterator
	h := &mergeHeap{}
	heap.Init(h)

	for i, iter := range iters {
		if iter.Next() {
			heap.Push(h, &mergeHeapItem{
				termID:   iter.Term(),
				postings: iter.Postings(),
				df:       iter.DF(),
				iterIdx:  i,
			})
		}
	}

	// Streaming k-way merge
	termCount := 0
	var currentTermID string
	var currentPostings []Posting
	var currentDF int64

	flushCurrentTerm := func() error {
		if currentTermID == "" || len(currentPostings) == 0 {
			return nil
		}

		// Dedup in-place: postings from each segment are already sorted by DocID.
		// Keep last occurrence of each DocID (latest segment wins).
		if len(currentPostings) > 1 {
			j := 0
			for i := 1; i < len(currentPostings); i++ {
				if currentPostings[i].DocID == currentPostings[j].DocID {
					currentPostings[j] = currentPostings[i] // overwrite with latest
				} else {
					j++
					currentPostings[j] = currentPostings[i]
				}
			}
			currentPostings = currentPostings[:j+1]
		}

		if err := writer.AddTermSorted(currentTermID, currentPostings, currentDF); err != nil {
			return err
		}
		termCount++

		// Reset for next term
		currentPostings = currentPostings[:0]
		currentDF = 0
		return nil
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(*mergeHeapItem)

		// If new term, flush previous and start new
		if item.termID != currentTermID {
			if err := flushCurrentTerm(); err != nil {
				writer.Abort()
				return nil, err
			}
			currentTermID = item.termID
		}

		// Accumulate postings for current term
		currentPostings = append(currentPostings, item.postings...)
		currentDF += item.df

		// Advance the iterator that gave us this item
		iter := iters[item.iterIdx]
		if iter.Next() {
			heap.Push(h, &mergeHeapItem{
				termID:   iter.Term(),
				postings: iter.Postings(),
				df:       iter.DF(),
				iterIdx:  item.iterIdx,
			})
		}
	}

	// Flush last term
	if err := flushCurrentTerm(); err != nil {
		writer.Abort()
		return nil, err
	}

	// Set metadata
	writer.SetMeta(SegmentMeta{
		ID:        newID,
		DocCount:  totalDocs,
		TermCount: termCount,
		CreatedAt: time.Now().UTC(),
		Level:     newLevel,
	})

	// Finalize
	path, err := writer.Finalize()
	if err != nil {
		return nil, err
	}

	return OpenDiskSegmentLazy(path)
}

// updateManifestAfterMerge updates the manifest after a merge.
func (m *Manager) updateManifestAfterMerge(old []*DiskSegment, merged *DiskSegment) {
	m.manifest.mu.Lock()

	oldIDs := make(map[string]bool)
	for _, seg := range old {
		oldIDs[seg.ID()] = true
	}

	// Remove old entries
	newEntries := make([]ManifestEntry, 0, len(m.manifest.Segments)-len(old)+1)
	for _, entry := range m.manifest.Segments {
		if !oldIDs[entry.ID] {
			newEntries = append(newEntries, entry)
		}
	}

	// Add merged entry
	newEntries = append(newEntries, ManifestEntry{
		ID:        merged.ID(),
		Path:      merged.Path(),
		DocCount:  merged.DocCount(),
		TermCount: merged.TermCount(),
		Level:     merged.Meta().Level,
		CreatedAt: merged.Meta().CreatedAt,
		SizeBytes: merged.Meta().SizeBytes,
	})

	m.manifest.Segments = newEntries
	m.manifest.Version++
	m.manifest.UpdatedAt = time.Now().UTC()
	m.manifest.mu.Unlock()

	// Save manifest outside the write lock — Save() acquires its own RLock
	// for serialization. This matches the pattern used by doFlush (lines 742-757).
	m.manifest.Save()
}

// Flush forces a flush of the active segment.
func (m *Manager) Flush() error {
	m.doFlush()
	return nil
}

// Close closes the manager.
func (m *Manager) Close() error {
	if m.closed.Swap(true) {
		return nil
	}

	close(m.closeCh)
	m.wg.Wait()

	// Final flush
	m.doFlush()

	// Close WAL
	if m.wal != nil {
		m.wal.Close()
	}

	// Close registry only if we own it (not shared) and it exists
	if m.registry != nil && m.ownsRegistry {
		m.registry.Close()
	}

	// Drain any remaining accumulated terms (best-effort)
	_ = m.accumulator.Drain()

	// Close all segments
	m.segMu.Lock()
	for _, seg := range m.segments {
		seg.Close()
	}
	m.segMu.Unlock()

	// Save manifest
	m.manifest.Save()

	return nil
}

// Stats returns manager statistics.
func (m *Manager) Stats() ManagerStats {
	m.activeMu.Lock()
	activeDocCount := m.active.DocCount()
	activeTermCount := m.active.TermCount()
	m.activeMu.Unlock()

	m.segMu.RLock()
	diskSegments := len(m.segments)
	totalDiskDocs := 0
	totalTerms := 0
	for _, seg := range m.segments {
		totalDiskDocs += seg.DocCount()
		totalTerms += seg.TermCount()
	}
	m.segMu.RUnlock()

	return ManagerStats{
		ActiveDocCount:  activeDocCount,
		ActiveTermCount: activeTermCount,
		DiskSegments:    diskSegments,
		TotalDiskDocs:   totalDiskDocs,
		IndexedDocs:     m.indexedDocs.Load(),
		Searches:        m.searches.Load(),
		TotalTerms:      activeTermCount + totalTerms,
		TotalDocs:       m.totalDocs.Load(),
	}
}

// ManagerStats contains manager statistics.
type ManagerStats struct {
	ActiveDocCount  int
	ActiveTermCount int
	DiskSegments    int
	TotalDiskDocs   int
	TotalTerms      int
	TotalDocs       int64
	IndexedDocs     int64
	Searches        int64
}

// Registry returns the term registry (may be nil if no registry is configured).
func (m *Manager) Registry() TermRegistry {
	return m.registry
}

// PebbleRegistry returns the Pebble-backed term registry for advanced term operations.
// Returns nil if no registry is configured.
func (m *Manager) PebbleRegistry() *PebbleTermRegistry {
	if m.registry == nil {
		return nil
	}
	reg, _ := m.registry.(*PebbleTermRegistry)
	return reg
}

// FSTRegistry is an alias for PebbleRegistry kept for backward compatibility.
func (m *Manager) FSTRegistry() *PebbleTermRegistry {
	return m.PebbleRegistry()
}

// GetTermsWithPrefix returns all termIDs (field\x00term keys) matching a prefix.
// Used by the query executor for prefix search without registry.
func (m *Manager) GetTermsWithPrefix(_ context.Context, field, prefix string) []string {
	searchPrefix := field + "\x00" + prefix

	// Collect from active segment
	m.activeMu.Lock()
	activeTerms := m.active.GetTermsWithPrefix(searchPrefix)
	m.activeMu.Unlock()

	// Collect from disk segments
	m.segMu.RLock()
	segments := make([]*DiskSegment, len(m.segments))
	copy(segments, m.segments)
	m.segMu.RUnlock()

	seen := make(map[string]struct{}, len(activeTerms))
	for _, t := range activeTerms {
		seen[t] = struct{}{}
	}

	for _, seg := range segments {
		for _, t := range seg.GetTermsWithPrefix(searchPrefix) {
			if _, exists := seen[t]; !exists {
				seen[t] = struct{}{}
				activeTerms = append(activeTerms, t)
			}
		}
	}

	return activeTerms
}

// ListTerms returns all terms, optionally filtered by field, by scanning segments.
func (m *Manager) ListTerms(_ context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	searchPrefix := ""
	if field != "" {
		searchPrefix = field + "\x00"
	}

	entries := m.collectTermEntries(searchPrefix, field)

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

// ListTermsByPrefix returns terms matching a prefix by scanning segments.
func (m *Manager) ListTermsByPrefix(_ context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	searchPrefix := field + "\x00" + prefix
	entries := m.collectTermEntries(searchPrefix, field)

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

// ListTermsByFuzzy returns terms within Levenshtein distance of the query by scanning segments.
func (m *Manager) ListTermsByFuzzy(_ context.Context, field, query string, maxDistance, limit int) ([]TermEntry, error) {
	if maxDistance < 1 {
		maxDistance = 1
	}
	if maxDistance > 3 {
		maxDistance = 3
	}

	// Scan all terms for the field
	searchPrefix := field + "\x00"
	allTermIDs := m.collectAllTermIDs(searchPrefix)

	var entries []TermEntry
	for _, termID := range allTermIDs {
		// Extract term from termID (field\x00term)
		term := termID[len(searchPrefix):]
		if levenshteinDistance(query, term) <= maxDistance {
			entries = append(entries, TermEntry{
				Field:  field,
				Term:   term,
				TermID: termID,
				DF:     m.GetDF(termID),
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

// ListTermsByRegex returns terms matching a regex pattern by scanning segments.
func (m *Manager) ListTermsByRegex(_ context.Context, field, pattern string, limit int) ([]TermEntry, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}

	searchPrefix := field + "\x00"
	allTermIDs := m.collectAllTermIDs(searchPrefix)

	var entries []TermEntry
	for _, termID := range allTermIDs {
		term := termID[len(searchPrefix):]
		if re.MatchString(term) {
			entries = append(entries, TermEntry{
				Field:  field,
				Term:   term,
				TermID: termID,
				DF:     m.GetDF(termID),
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

// collectTermEntries gathers unique TermEntry from all segments matching the given prefix.
func (m *Manager) collectTermEntries(prefix, field string) []TermEntry {
	allTermIDs := m.collectAllTermIDs(prefix)

	entries := make([]TermEntry, 0, len(allTermIDs))
	for _, termID := range allTermIDs {
		// Parse field and term from termID: field\x00term
		sep := -1
		for i := 0; i < len(termID); i++ {
			if termID[i] == 0 {
				sep = i
				break
			}
		}
		if sep < 0 {
			continue
		}
		entryField := termID[:sep]
		entryTerm := termID[sep+1:]
		if field != "" && entryField != field {
			continue
		}
		entries = append(entries, TermEntry{
			Field:  entryField,
			Term:   entryTerm,
			TermID: termID,
			DF:     m.GetDF(termID),
		})
	}
	return entries
}

// collectAllTermIDs returns unique termIDs matching the prefix across all segments.
func (m *Manager) collectAllTermIDs(prefix string) []string {
	m.activeMu.Lock()
	activeTerms := m.active.GetTermsWithPrefix(prefix)
	m.activeMu.Unlock()

	m.segMu.RLock()
	segments := make([]*DiskSegment, len(m.segments))
	copy(segments, m.segments)
	m.segMu.RUnlock()

	seen := make(map[string]struct{}, len(activeTerms))
	result := make([]string, 0, len(activeTerms))
	for _, t := range activeTerms {
		if _, exists := seen[t]; !exists {
			seen[t] = struct{}{}
			result = append(result, t)
		}
	}

	for _, seg := range segments {
		for _, t := range seg.GetTermsWithPrefix(prefix) {
			if _, exists := seen[t]; !exists {
				seen[t] = struct{}{}
				result = append(result, t)
			}
		}
	}

	return result
}

// Load loads the manifest from disk.
func (mf *Manifest) Load() error {
	data, err := os.ReadFile(mf.path)
	if err != nil {
		return err
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()

	return json.Unmarshal(data, mf)
}

// Save saves the manifest to disk.
func (mf *Manifest) Save() error {
	mf.mu.RLock()
	data, err := json.MarshalIndent(mf, "", "  ")
	mf.mu.RUnlock()

	if err != nil {
		return err
	}

	tempPath := mf.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, mf.path)
}
