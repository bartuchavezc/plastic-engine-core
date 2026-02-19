package segment

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/core/search/wal"
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

// Manager orchestrates segments, handling indexing, searching, and merging.
type Manager struct {
	mu sync.RWMutex

	// Active segment (receives writes)
	active *MemSegment

	// Disk segments (immutable, for reads)
	segments []*DiskSegment

	// Term registry (FST-backed)
	registry TermRegistry
	// ownsRegistry is true if this Manager created the registry and should close it
	ownsRegistry bool

	// WAL for the active segment
	wal *wal.WAL

	// Configuration
	config Config

	// Manifest tracks segments
	manifest *Manifest

	// Background workers
	flushCh    chan struct{}
	mergeCh    chan struct{}   // signal to check for merges
	mergeJobCh chan mergeJob   // merge jobs for pool workers
	closeCh    chan struct{}
	wg         sync.WaitGroup
	closed     atomic.Bool

	// Track segments being merged to avoid double-selection
	mergingMu sync.Mutex
	merging   map[string]bool

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
		// Create our own registry (legacy behavior)
		registryDir := filepath.Join(config.DataDir, "registry")

		mergeConfig := DefaultMergeConfig()
		if config.TermRegistryMergeConfig != nil {
			mergeConfig = *config.TermRegistryMergeConfig
		}

		newRegistry, err := NewCacheFSTRegistry(CacheFSTConfig{
			DataDir:     registryDir,
			MergeConfig: mergeConfig,
		})
		if err != nil {
			return nil, fmt.Errorf("create term registry: %w", err)
		}
		registry = newRegistry
		ownsRegistry = true
	}

	// Initialize WAL
	walPath := filepath.Join(config.DataDir, "active.wal")
	activeWAL, err := wal.Open(walPath, wal.Options{
		SyncMode:   wal.SyncBatch,
		BufferSize: 64 * 1024,
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

	m := &Manager{
		registry:     registry,
		ownsRegistry: ownsRegistry,
		wal:          activeWAL,
		config:       config,
		flushCh:      make(chan struct{}, 1),
		mergeCh:      make(chan struct{}, 1),
		mergeJobCh:   make(chan mergeJob, config.MergeWorkers*2),
		closeCh:      make(chan struct{}),
		merging:      make(map[string]bool),
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
			m.registry.Sync()
		}
	}
}

// Index adds a document posting to the index.
func (m *Manager) Index(ctx context.Context, field, term, docID string, tf int, positions []int) error {
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}

	// Get or create term ID
	termID, err := m.registry.GetOrCreate(ctx, field, term)
	if err != nil {
		return fmt.Errorf("get term id: %w", err)
	}

	// Log to WAL
	if _, err := m.wal.AppendIndex(wal.IndexOp{
		TermID:    termID,
		DocID:     docID,
		TF:        tf,
		Positions: positions,
		Field:     field,
		Term:      term,
	}); err != nil {
		return fmt.Errorf("append to wal: %w", err)
	}

	// Add to active segment
	m.mu.Lock()
	m.active.Add(termID, docID, tf, positions)
	docCount := m.active.DocCount()
	estimatedBytes := m.active.EstimatedBytes()
	m.mu.Unlock()

	// Update registry stats
	m.registry.IncrementDF(termID, 1)

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
func (m *Manager) IndexDocumentBatch(ctx context.Context, docs []DocumentBatch) error {
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}
	if len(docs) == 0 {
		return nil
	}

	// Pre-calculate total postings to pre-allocate allEntries in one shot,
	// avoiding repeated reallocs and over-allocation.
	totalPostings := 0
	for _, doc := range docs {
		for _, terms := range doc.FieldTerms {
			totalPostings += len(terms)
		}
	}

	termCache := make(map[string]string, 64) // "field\x00term" -> termID (dedup across batch)
	allEntries := make([]SegmentEntry, 0, totalPostings)
	walOps := make([]wal.DocIndexOp, 0, len(docs))
	dfDeltas := make(map[string]int64, 64) // termID -> delta (aggregated across all docs)

	// Single pass: resolve terms, build segment entries and WAL ops simultaneously.
	// Previously we built a per-doc intermediate slice (allDocs[i].entries) and then
	// copied everything into allEntries — doubling peak memory for the entries data.
	// Now allEntries is built directly: one copy, one allocation.
	for _, doc := range docs {
		walFields := make([]wal.DocFieldTerms, 0, len(doc.FieldTerms))

		for field, terms := range doc.FieldTerms {
			walPostings := make([]wal.DocTermPosting, 0, len(terms))

			for _, tp := range terms {
				// Deduplicated term resolution: each unique field+term resolved once.
				key := field + "\x00" + tp.Term
				termID, ok := termCache[key]
				if !ok {
					var err error
					termID, err = m.registry.GetOrCreate(ctx, field, tp.Term)
					if err != nil {
						return fmt.Errorf("get or create term: %w", err)
					}
					termCache[key] = termID
				}

				allEntries = append(allEntries, SegmentEntry{
					TermID:    termID,
					DocID:     doc.DocID,
					TF:        tp.TF,
					Positions: tp.Positions,
				})

				walPostings = append(walPostings, wal.DocTermPosting{
					TermID:    termID,
					Term:      tp.Term,
					TF:        tp.TF,
					Positions: tp.Positions,
				})

				dfDeltas[termID]++
			}

			walFields = append(walFields, wal.DocFieldTerms{
				Field:    field,
				Postings: walPostings,
			})
		}

		walOps = append(walOps, wal.DocIndexOp{
			DocID:  doc.DocID,
			Fields: walFields,
		})
	}

	// Phase 2: Batch WAL write (single lock acquisition for all documents)
	if _, err := m.wal.AppendDocuments(walOps); err != nil {
		return fmt.Errorf("append docs to wal: %w", err)
	}
	walOps = nil // release WAL data before AddBatch to reduce peak memory

	// Phase 3: Add to active segment under single lock (AddBatch = 1 lock acquisition)
	m.mu.Lock()
	m.active.AddBatch(allEntries)
	docCount := m.active.DocCount()
	estimatedBytes := m.active.EstimatedBytes()
	m.mu.Unlock()

	// Phase 4: Aggregated DF increments (one call per unique term, not per posting)
	for termID, delta := range dfDeltas {
		m.registry.IncrementDF(termID, delta)
	}

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
func (m *Manager) Search(ctx context.Context, field, term string) ([]Hit, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("manager is closed")
	}

	m.searches.Add(1)

	// Get term ID from registry
	termID, found := m.registry.Get(ctx, field, term)
	if !found {
		return nil, nil
	}

	return m.SearchByTermID(termID)
}

// searchSema limits concurrent disk-segment search goroutines to prevent goroutine explosion
// under concurrent queries with many segments. 8 allows good parallelism within 2-CPU containers.
var searchSema = make(chan struct{}, 8)

// SearchByTermID searches using a term ID directly.
func (m *Manager) SearchByTermID(termID string) ([]Hit, error) {
	m.mu.RLock()
	active := m.active
	segments := make([]*DiskSegment, len(m.segments))
	copy(segments, m.segments)
	m.mu.RUnlock()

	var allHits []Hit

	// Search active segment
	hits := active.Search(termID)
	allHits = append(allHits, hits...)

	// Search disk segments in parallel (bounded concurrency)
	var wg sync.WaitGroup
	var hitsMu sync.Mutex

	for _, seg := range segments {
		wg.Add(1)
		searchSema <- struct{}{} // acquire slot
		go func(s *DiskSegment) {
			defer wg.Done()
			defer func() { <-searchSema }() // release slot
			hits := s.Search(termID)
			if len(hits) > 0 {
				hitsMu.Lock()
				allHits = append(allHits, hits...)
				hitsMu.Unlock()
			}
		}(seg)
	}

	wg.Wait()
	return allHits, nil
}

// GetDF returns the global document frequency for a term.
func (m *Manager) GetDF(termID string) int64 {
	return m.registry.GetDF(termID)
}

// GetTotalDocs returns the total number of indexed documents.
func (m *Manager) GetTotalDocs() int64 {
	return m.registry.GetTotalDocs()
}

// CreateAlias creates a term alias.
func (m *Manager) CreateAlias(field, newTerm, existingTermID string) error {
	return m.registry.CreateAlias(field, newTerm, existingTermID)
}

// needsFlush returns true if the active segment should be flushed.
func (m *Manager) needsFlush(docCount int, estimatedBytes int64) bool {
	if docCount >= m.config.FlushThreshold {
		return true
	}
	if m.config.FlushThresholdBytes > 0 && estimatedBytes >= m.config.FlushThresholdBytes {
		return true
	}
	return false
}

// maybeFlush checks if flush is needed.
func (m *Manager) maybeFlush() {
	m.mu.RLock()
	docCount := m.active.DocCount()
	estimatedBytes := m.active.EstimatedBytes()
	m.mu.RUnlock()

	if m.needsFlush(docCount, estimatedBytes) {
		m.doFlush()
	}
}

// doFlush flushes the active segment to disk.
func (m *Manager) doFlush() {
	m.mu.Lock()
	if m.active.DocCount() == 0 {
		m.mu.Unlock()
		return
	}

	// Swap active segment
	oldActive := m.active
	docCount := oldActive.DocCount()
	newActiveID := fmt.Sprintf("mem_%d", time.Now().UnixNano())
	m.active = NewMemSegment(newActiveID)
	m.manifest.ActiveID = newActiveID
	m.mu.Unlock()

	flushStart := time.Now()

	// Flush old active to disk
	segmentDir := filepath.Join(m.config.DataDir, "segments")
	diskSeg, err := FlushMemSegment(oldActive, segmentDir)
	if err != nil {
		// Put it back if flush failed
		m.mu.Lock()
		m.active = oldActive
		m.mu.Unlock()
		return
	}

	// Record flush metrics
	if mp := metrics.Global(); mp != nil {
		mp.RecordSegmentFlush(context.Background(), docCount, time.Since(flushStart))
	}

	// Add to segments list
	m.mu.Lock()
	m.segments = append(m.segments, diskSeg)
	m.mu.Unlock()

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

	// Truncate WAL
	m.wal.Truncate()

	// Trigger merge check
	select {
	case m.mergeCh <- struct{}{}:
	default:
	}
}

// scheduleMerge finds merge candidates and submits jobs to the merge pool.
// Runs in the merge scheduler goroutine — no heavy I/O here.
func (m *Manager) scheduleMerge() {
	m.mu.RLock()
	segments := make([]*DiskSegment, len(m.segments))
	copy(segments, m.segments)
	m.mu.RUnlock()

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

	// Submit merge jobs for all levels that need it
	for level, segs := range levels {
		if len(segs) >= m.config.MaxSegmentsPerLevel {
			// Mark segments as merging
			for _, seg := range segs {
				m.merging[seg.ID()] = true
			}

			select {
			case m.mergeJobCh <- mergeJob{segments: segs, level: level + 1}:
			default:
				// Pool busy, unmark and try later
				for _, seg := range segs {
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

	// Phase 1: Merge segments (I/O heavy, no lock needed)
	merged, err := m.mergeSegments(job.segments, job.level)

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

	m.mu.Lock()
	newSegments := make([]*DiskSegment, 0, len(m.segments)-len(job.segments)+1)
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
	m.mu.Unlock()

	// Phase 3: Update manifest
	m.updateManifestAfterMerge(job.segments, merged)

	// Phase 4: Cleanup old segments after delay
	go func(paths []string, oldSegs []*DiskSegment) {
		time.Sleep(5 * time.Second)
		for _, seg := range oldSegs {
			seg.Close()
		}
		for _, path := range paths {
			os.Remove(path)
		}
	}(oldPaths, job.segments)

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

		// Deduplicate postings by doc ID (keep latest)
		postingMap := make(map[string]Posting, len(currentPostings))
		for _, p := range currentPostings {
			postingMap[p.DocID] = p
		}

		dedupedPostings := make([]Posting, 0, len(postingMap))
		for _, p := range postingMap {
			dedupedPostings = append(dedupedPostings, p)
		}

		if err := writer.AddTerm(currentTermID, dedupedPostings, currentDF); err != nil {
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

	return OpenDiskSegment(path)
}

// updateManifestAfterMerge updates the manifest after a merge.
func (m *Manager) updateManifestAfterMerge(old []*DiskSegment, merged *DiskSegment) {
	m.manifest.mu.Lock()
	defer m.manifest.mu.Unlock()

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

	// Close registry only if we own it (not shared)
	if m.registry != nil && m.ownsRegistry {
		m.registry.Close()
	}

	// Close all segments
	m.mu.Lock()
	for _, seg := range m.segments {
		seg.Close()
	}
	m.mu.Unlock()

	// Save manifest
	m.manifest.Save()

	return nil
}

// Stats returns manager statistics.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := ManagerStats{
		ActiveDocCount:  m.active.DocCount(),
		ActiveTermCount: m.active.TermCount(),
		DiskSegments:    len(m.segments),
		IndexedDocs:     m.indexedDocs.Load(),
		Searches:        m.searches.Load(),
	}

	for _, seg := range m.segments {
		stats.TotalDiskDocs += seg.DocCount()
	}

	stats.TotalTerms = int(m.registry.GetTermCount())
	stats.TotalDocs = m.registry.GetTotalDocs()

	return stats
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

// Registry returns the term registry.
func (m *Manager) Registry() TermRegistry {
	return m.registry
}

// FSTRegistry returns the FST registry for advanced term operations.
func (m *Manager) FSTRegistry() *CacheFSTRegistry {
	return m.registry.(*CacheFSTRegistry)
}

// ListTerms returns all terms, optionally filtered by field.
func (m *Manager) ListTerms(ctx context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	return m.FSTRegistry().ListTerms(ctx, field, limit, offset)
}

// ListTermsByPrefix returns terms matching a prefix.
func (m *Manager) ListTermsByPrefix(ctx context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	return m.FSTRegistry().ListTermsByPrefix(ctx, field, prefix, limit)
}

// ListTermsByFuzzy returns terms within Levenshtein distance of the query.
func (m *Manager) ListTermsByFuzzy(ctx context.Context, field, query string, maxDistance, limit int) ([]TermEntry, error) {
	return m.FSTRegistry().ListTermsByFuzzy(ctx, field, query, maxDistance, limit)
}

// ListTermsByRegex returns terms matching a regex pattern.
func (m *Manager) ListTermsByRegex(ctx context.Context, field, pattern string, limit int) ([]TermEntry, error) {
	return m.FSTRegistry().ListTermsByRegex(ctx, field, pattern, limit)
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
