package segment

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"plastic-engine-core/internal/core/search/wal"
)

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
	flushCh chan struct{}
	mergeCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool

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

		newRegistry, err := NewPebbleFSTRegistry(PebbleFSTConfig{
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

	m := &Manager{
		registry:     registry,
		ownsRegistry: ownsRegistry,
		wal:          activeWAL,
		config:       config,
		flushCh:      make(chan struct{}, 1),
		mergeCh:      make(chan struct{}, 1),
		closeCh:      make(chan struct{}),
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
		if entry.Type == wal.OpIndex {
			var op wal.IndexOp
			if err := json.Unmarshal(entry.Data, &op); err != nil {
				continue
			}
			m.active.Add(op.TermID, op.DocID, op.TF, op.Positions)
		}
	}

	return nil
}

// startBackgroundWorkers starts flush and merge workers.
func (m *Manager) startBackgroundWorkers() {
	// Periodic flush worker
	m.wg.Add(1)
	go m.flushWorker()

	// Periodic merge worker
	m.wg.Add(1)
	go m.mergeWorker()

	// Periodic sync worker
	m.wg.Add(1)
	go m.syncWorker()
}

// flushWorker handles flushing the active segment.
func (m *Manager) flushWorker() {
	defer m.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
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

// mergeWorker handles merging segments.
func (m *Manager) mergeWorker() {
	defer m.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			m.maybeMerge()
		case <-m.mergeCh:
			m.doMerge()
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
	m.mu.Unlock()

	// Update registry stats
	m.registry.IncrementDF(termID, 1)

	// Check if we need to flush
	if docCount >= m.config.FlushThreshold {
		select {
		case m.flushCh <- struct{}{}:
		default:
		}
	}

	m.indexedDocs.Add(1)
	return nil
}

// IndexDocument indexes all terms for a document.
// Assumes fieldTerms contains unique terms per field (pre-aggregated by tokenizer).
func (m *Manager) IndexDocument(ctx context.Context, docID string, fieldTerms map[string][]TermPosting) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for field, terms := range fieldTerms {
		for _, tp := range terms {
			termID, err := m.registry.GetOrCreate(ctx, field, tp.Term)
			if err != nil {
				return fmt.Errorf("get or create term: %w", err)
			}

			if _, err := m.wal.AppendIndex(wal.IndexOp{
				TermID:    termID,
				DocID:     docID,
				TF:        tp.TF,
				Positions: tp.Positions,
				Field:     field,
				Term:      tp.Term,
			}); err != nil {
				return fmt.Errorf("append to wal: %w", err)
			}

			m.registry.IncrementDF(termID, 1)
			m.active.Add(termID, docID, tp.TF, tp.Positions)
		}
	}

	m.indexedDocs.Add(1)

	if m.active.DocCount() >= m.config.FlushThreshold {
		select {
		case m.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
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

	// Search disk segments in parallel
	var wg sync.WaitGroup
	var hitsMu sync.Mutex

	for _, seg := range segments {
		wg.Add(1)
		go func(s *DiskSegment) {
			defer wg.Done()
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

// maybeFlush checks if flush is needed.
func (m *Manager) maybeFlush() {
	m.mu.RLock()
	docCount := m.active.DocCount()
	m.mu.RUnlock()

	if docCount >= m.config.FlushThreshold {
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
	newActiveID := fmt.Sprintf("mem_%d", time.Now().UnixNano())
	m.active = NewMemSegment(newActiveID)
	m.manifest.ActiveID = newActiveID
	m.mu.Unlock()

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

// maybeMerge checks if merge is needed.
func (m *Manager) maybeMerge() {
	m.mu.RLock()
	segmentCount := len(m.segments)
	m.mu.RUnlock()

	if segmentCount >= m.config.MaxSegmentsPerLevel {
		m.doMerge()
	}
}

// doMerge merges segments based on tiered policy.
func (m *Manager) doMerge() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Group segments by level
	levels := make(map[int][]*DiskSegment)
	for _, seg := range m.segments {
		level := seg.Meta().Level
		levels[level] = append(levels[level], seg)
	}

	// Find a level that needs merging
	for level, segs := range levels {
		if len(segs) >= m.config.MaxSegmentsPerLevel {
			// Merge these segments
			merged, err := m.mergeSegments(segs, level+1)
			if err != nil {
				continue
			}

			// Remove old segments, add merged
			newSegments := make([]*DiskSegment, 0, len(m.segments)-len(segs)+1)
			oldPaths := make([]string, 0, len(segs))

			for _, s := range m.segments {
				isOld := false
				for _, old := range segs {
					if s.ID() == old.ID() {
						isOld = true
						oldPaths = append(oldPaths, s.Path())
						break
					}
				}
				if !isOld {
					newSegments = append(newSegments, s)
				}
			}

			newSegments = append(newSegments, merged)
			m.segments = newSegments

			// Update manifest
			m.updateManifestAfterMerge(segs, merged)

			// Close and delete old segments (after a delay to ensure no readers)
			go func(paths []string, oldSegs []*DiskSegment) {
				time.Sleep(5 * time.Second)
				for _, seg := range oldSegs {
					seg.Close()
				}
				for _, path := range paths {
					os.Remove(path)
				}
			}(oldPaths, segs)

			break // One merge per call
		}
	}
}

// mergeSegments merges multiple segments into one.
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

	// K-way merge
	totalDocs := 0
	for _, seg := range segments {
		totalDocs += seg.DocCount()
	}

	// Simple approach: collect all terms, sort, merge
	allTerms := make(map[string][]Posting)
	allDF := make(map[string]int64)

	for i, iter := range iters {
		for iter.Next() {
			termID := iter.Term()
			postings := iter.Postings()
			df := iter.DF()

			existing := allTerms[termID]
			existing = append(existing, postings...)
			allTerms[termID] = existing
			allDF[termID] += df
		}
		iters[i] = nil // Allow GC
	}

	// Sort term IDs
	termIDs := make([]string, 0, len(allTerms))
	for termID := range allTerms {
		termIDs = append(termIDs, termID)
	}
	sort.Strings(termIDs)

	// Write merged terms
	for _, termID := range termIDs {
		postings := allTerms[termID]
		df := allDF[termID]

		// Deduplicate postings by doc ID (keep latest)
		postingMap := make(map[string]Posting)
		for _, p := range postings {
			postingMap[p.DocID] = p
		}

		dedupedPostings := make([]Posting, 0, len(postingMap))
		for _, p := range postingMap {
			dedupedPostings = append(dedupedPostings, p)
		}

		if err := writer.AddTerm(termID, dedupedPostings, df); err != nil {
			writer.Abort()
			return nil, err
		}
	}

	// Set metadata
	writer.SetMeta(SegmentMeta{
		ID:        newID,
		DocCount:  totalDocs,
		TermCount: len(termIDs),
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
func (m *Manager) FSTRegistry() *PebbleFSTRegistry {
	return m.registry.(*PebbleFSTRegistry)
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
