package segment

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	rmetrics "runtime/metrics"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"plastic-engine-core/internal/core/search/knowledge"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

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

// Manager orchestrates indexing, searching, and knowledge graph integration.
// Storage is delegated entirely to PebblePostingStore (Pebble handles memtable/flush/compaction).
type Manager struct {
	store        *PebblePostingStore
	registry     TermRegistry
	ownsRegistry bool

	accumulator  *termAccumulator
	cooccurrence *cooccurrenceAccumulator

	// termMatrix is an owned adjacency matrix for term co-occurrence (PMI edges).
	// Created automatically alongside the posting store. Separate from the knowledge graph.
	termMatrix *knowledge.AdjacencyMatrix

	// matrix is the external knowledge graph (optional, set via SetMatrix).
	// Used for concept-level relationships (agents, user feedback, etc.).
	matrix *knowledge.AdjacencyMatrix

	totalDocs atomic.Int64
	config    Config

	closeCh chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool

	// Heap pressure monitor: 0=none, 1=medium, 2=high
	pressureLevel atomic.Int32
	memLimitBytes int64

	// Stats
	indexedDocs atomic.Int64
	searches    atomic.Int64
}

var mgrTracer = otel.Tracer("search/segment-manager")

// NewManager creates a new segment manager backed by PebblePostingStore.
func NewManager(config Config) (*Manager, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// Use external registry if provided, otherwise create our own
	var registry TermRegistry
	var ownsRegistry bool

	if config.TermRegistry != nil {
		registry = config.TermRegistry
		ownsRegistry = false
	} else {
		registryDir := config.DataDir + "/registry"
		newRegistry, err := NewPebbleTermRegistry(PebbleRegistryConfig{
			DataDir: registryDir,
		})
		if err != nil {
			return nil, fmt.Errorf("create term registry: %w", err)
		}
		registry = newRegistry
		ownsRegistry = true
	}

	// Open PebblePostingStore
	storeCfg := config.PostingStoreConfig
	if storeCfg.DataDir == "" {
		storeCfg.DataDir = config.DataDir
	}
	storeCfg.Cache = config.Cache
	store, err := NewPebblePostingStore(storeCfg)
	if err != nil {
		return nil, fmt.Errorf("open posting store: %w", err)
	}

	// Co-occurrence accumulator + term matrix (skip when disabled for isolated benchmarks)
	cooccCfg := config.CooccurrenceConfig
	if cooccCfg.WindowSize == 0 && !cooccCfg.Disabled {
		cooccCfg = DefaultCooccurrenceConfig()
	}

	var cooc *cooccurrenceAccumulator
	var termMatrix *knowledge.AdjacencyMatrix

	if !cooccCfg.Disabled {
		cooc = newCooccurrenceAccumulator(cooccCfg)

		tm, err := knowledge.NewAdjacencyMatrix(knowledge.AdjacencyMatrixConfig{
			DataDir:       config.DataDir + "/term_cooccurrence",
			Cache:         config.Cache,
			EdgeCacheSize: 10000,
		})
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("open term co-occurrence matrix: %w", err)
		}
		termMatrix = tm
	}

	m := &Manager{
		store:        store,
		registry:     registry,
		ownsRegistry: ownsRegistry,
		accumulator:  &termAccumulator{terms: make(map[string]int64, 1024)},
		cooccurrence: cooc,
		termMatrix:   termMatrix,
		config:       config,
		closeCh:      make(chan struct{}),
	}

	// Recover totalDocs from the posting store
	m.totalDocs.Store(store.GetTotalDocs())

	// Read GOMEMLIMIT for the heap monitor
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < int64(^uint64(0)>>1) {
		m.memLimitBytes = limit
	}

	// Start background workers
	m.startBackgroundWorkers()

	return m, nil
}

// startBackgroundWorkers starts drain and heap monitor workers.
func (m *Manager) startBackgroundWorkers() {
	m.wg.Add(1)
	go m.heapMonitor()

	m.wg.Add(1)
	go m.drainWorker()

	if m.cooccurrence != nil {
		m.wg.Add(1)
		go m.cooccurrenceDrainWorker()
	}
}

// heapMonitor watches runtime heap usage and sets pressureLevel.
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
				level = 2
			case ratio > 0.70 || (prev >= 1 && ratio > 0.60):
				level = 1
			default:
				level = 0
			}
			m.pressureLevel.Store(level)
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
			m.drainTermsToMatrix()
			return
		case <-ticker.C:
			m.drainTermsToMatrix()
		}
	}
}

// cooccurrenceDrainWorker runs two tickers:
//   - Hot drain (DrainInterval, default 10s): merges raw pairs into epoch accumulator.
//     Integer-only operations, no Pebble I/O.
//   - Cold epoch (EpochInterval, default 90s): takes a DF snapshot via single prefix scan,
//     computes NPMI for all accumulated pairs, writes edges, resets epoch.
func (m *Manager) cooccurrenceDrainWorker() {
	defer m.wg.Done()

	hotInterval := m.cooccurrence.cfg.DrainInterval
	if hotInterval <= 0 {
		hotInterval = 10 * time.Second
	}
	coldInterval := m.cooccurrence.cfg.EpochInterval
	if coldInterval <= 0 {
		coldInterval = 90 * time.Second
	}

	hotTicker := time.NewTicker(hotInterval)
	coldTicker := time.NewTicker(coldInterval)
	defer hotTicker.Stop()
	defer coldTicker.Stop()

	for {
		select {
		case <-m.closeCh:
			// Final hot drain + cold epoch on shutdown
			m.hotDrainCooccurrence()
			m.coldEpochCooccurrence()
			return
		case <-hotTicker.C:
			m.hotDrainCooccurrence()
		case <-coldTicker.C:
			m.coldEpochCooccurrence()
		}
	}
}

// DocumentBatch represents a document with its field terms for batch indexing.
type DocumentBatch struct {
	DocID      string
	FieldTerms map[string][]TermPosting
}

// TermPosting is a term with its posting data for indexing.
type TermPosting struct {
	Term        string
	TF          int
	Positions   []int
	SentenceIDs []int // parallel to Positions: sentence index per position (transient, for co-occurrence)
}

// IndexDocumentBatch indexes multiple documents via PebblePostingStore.
func (m *Manager) IndexDocumentBatch(ctx context.Context, docs []DocumentBatch) error {
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}
	if len(docs) == 0 {
		return nil
	}

	// Backpressure from heap pressure only
	pressure := m.pressureLevel.Load()
	if pressure >= 2 {
		time.Sleep(50 * time.Millisecond)
	} else if pressure >= 1 {
		time.Sleep(1 * time.Millisecond)
	}

	_, span := mgrTracer.Start(ctx, "index.batch")
	span.SetAttributes(attribute.Int("docs", len(docs)))

	// Phase 1: Build dfDeltas (co-occurrence is async via EnqueueBatch)
	dfDeltas := make(map[string]int64, len(docs)*4)
	for _, doc := range docs {
		for field, terms := range doc.FieldTerms {
			for _, tp := range terms {
				termKey := field + "\x00" + tp.Term
				dfDeltas[termKey]++
			}
		}
	}
	if m.cooccurrence != nil {
		m.cooccurrence.EnqueueBatch(docs)
	}

	// Phase 2: Single Pebble batch commit (with pre-aggregated DF deltas)
	if err := m.store.IndexBatch(docs, dfDeltas); err != nil {
		span.End()
		return fmt.Errorf("index batch: %w", err)
	}

	// Phase 3: Buffer term deltas for background drain
	m.accumulator.AddBatch(dfDeltas)
	m.totalDocs.Store(m.store.GetTotalDocs())
	m.indexedDocs.Add(int64(len(docs)))

	span.End()
	return nil
}

// IndexDocument indexes all terms for a single document.
func (m *Manager) IndexDocument(ctx context.Context, docID string, fieldTerms map[string][]TermPosting) error {
	return m.IndexDocumentBatch(ctx, []DocumentBatch{{DocID: docID, FieldTerms: fieldTerms}})
}

// Index adds a document posting to the index (legacy single-posting API).
func (m *Manager) Index(ctx context.Context, field, term, docID string, tf int, positions []int) error {
	return m.IndexDocument(ctx, docID, map[string][]TermPosting{
		field: {{Term: term, TF: tf, Positions: positions}},
	})
}

// Search finds documents matching a term.
func (m *Manager) Search(_ context.Context, field, term string) ([]Hit, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("manager is closed")
	}
	m.searches.Add(1)
	return m.store.Search(field, term)
}

// SearchWithPositions is like Search but also decodes position data for each hit.
// Use this for phrase/proximity queries that need position information.
func (m *Manager) SearchWithPositions(_ context.Context, field, term string) ([]Hit, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("manager is closed")
	}
	m.searches.Add(1)
	return m.store.SearchWithPositions(field, term)
}

// SearchByTermID searches using a term ID (field\x00term format).
func (m *Manager) SearchByTermID(termID string) ([]Hit, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("manager is closed")
	}
	m.searches.Add(1)
	return m.store.SearchByTermID(termID)
}

// GetDF returns the document frequency for a term.
func (m *Manager) GetDF(termID string) int64 {
	return m.store.GetDFByTermID(termID)
}

// GetTotalDocs returns the total number of indexed documents.
func (m *Manager) GetTotalDocs() int64 {
	return m.totalDocs.Load()
}

// GetTermsWithPrefix returns all termIDs (field\x00term keys) matching a prefix.
func (m *Manager) GetTermsWithPrefix(_ context.Context, field, prefix string) []string {
	return m.store.GetTermsWithPrefix(field, prefix)
}

// CreateAlias creates a term alias.
func (m *Manager) CreateAlias(field, newTerm, existingTermID string) error {
	if m.registry == nil {
		return fmt.Errorf("no registry configured for alias support")
	}
	return m.registry.CreateAlias(field, newTerm, existingTermID)
}

// SetMatrix attaches an adjacency matrix to this manager.
func (m *Manager) SetMatrix(matrix *knowledge.AdjacencyMatrix) {
	m.matrix = matrix
}

// Matrix returns the knowledge graph adjacency matrix (may be nil).
func (m *Manager) Matrix() *knowledge.AdjacencyMatrix {
	return m.matrix
}

// TermMatrix returns the term co-occurrence adjacency matrix.
// This is a separate graph from the knowledge graph, containing PMI-based
// edges between terms that frequently co-occur in documents.
func (m *Manager) TermMatrix() *knowledge.AdjacencyMatrix {
	return m.termMatrix
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
	_ = m.matrix.IngestTermBatch(deltas)
}

// hotDrainCooccurrence drains raw extraction buffer and merges into epoch accumulator.
// Integer-only operations — no Pebble I/O, no NPMI scoring.
func (m *Manager) hotDrainCooccurrence() {
	if m.cooccurrence == nil {
		return
	}
	drained := m.cooccurrence.Drain()
	if len(drained) == 0 {
		return
	}
	m.cooccurrence.MergeHotDrain(drained)
}

// coldEpochCooccurrence scores accumulated pairs with NPMI using a single DF snapshot.
// Runs infrequently (every ~90s) to amortize the prefix scan cost.
func (m *Manager) coldEpochCooccurrence() {
	if m.termMatrix == nil || m.cooccurrence == nil {
		return
	}

	// Flush any pending hot drain first
	m.hotDrainCooccurrence()

	pairs := m.cooccurrence.DrainEpoch()
	if len(pairs) == 0 {
		return
	}

	totalDocs := m.totalDocs.Load()
	if totalDocs <= 0 {
		return
	}

	// Single prefix scan for all DF values — replaces N point lookups
	dfSnapshot := m.store.SnapshotDF()
	if dfSnapshot == nil {
		return
	}

	maxDF := m.cooccurrence.cfg.MaxDFThreshold
	minCount := m.cooccurrence.cfg.MinPairCount

	for i := range pairs {
		p := &pairs[i]
		if p.Count < minCount {
			continue
		}

		dfA := dfSnapshot[p.TermA]
		dfB := dfSnapshot[p.TermB]

		if dfA > maxDF || dfB > maxDF {
			continue
		}
		if dfA <= 0 || dfB <= 0 {
			continue
		}

		avgWeight := p.WeightedSum / float64(p.Count)
		weight := computeNPMIWeight(p.Count, dfA, dfB, totalDocs, avgWeight)
		if weight <= 0 {
			continue
		}

		edgeData := knowledge.EdgeData{
			Weight:   weight,
			EdgeType: "cooccurrence",
			Source:   "pmi",
		}
		_ = m.termMatrix.AddEdge(p.TermA, p.TermB, edgeData)
		_ = m.termMatrix.AddEdge(p.TermB, p.TermA, edgeData)
	}
}

// EffectiveBatchSize returns the batch size adjusted for current heap pressure.
// Under high pressure the batch shrinks to reduce peak memory per chunk.
func (m *Manager) EffectiveBatchSize(baseBatchSize int) int {
	switch m.pressureLevel.Load() {
	case 2:
		return max(baseBatchSize/4, 64)
	case 1:
		return max(baseBatchSize/2, 128)
	default:
		return baseBatchSize
	}
}

// UpdateFlushThresholds is a no-op now (Pebble manages its own flush).
// Kept for API compatibility with shards.Manager.
func (m *Manager) UpdateFlushThresholds(flushBytes int64, flushDocs int) {
	// No-op: Pebble handles memtable flush internally
}

// Flush is a no-op now (Pebble manages its own flush).
func (m *Manager) Flush() error {
	return nil
}

// Close closes the manager and all resources.
func (m *Manager) Close() error {
	if m.closed.Swap(true) {
		return nil
	}

	close(m.closeCh)
	m.wg.Wait()

	// Close co-occurrence worker before closing the store
	if m.cooccurrence != nil {
		m.cooccurrence.Close()
	}

	// Close posting store
	if m.store != nil {
		m.store.Close()
	}

	// Close term co-occurrence matrix
	if m.termMatrix != nil {
		m.termMatrix.Close()
	}

	// Close registry only if we own it
	if m.registry != nil && m.ownsRegistry {
		m.registry.Close()
	}

	// Drain remaining terms
	_ = m.accumulator.Drain()

	return nil
}

// Stats returns manager statistics.
func (m *Manager) Stats() ManagerStats {
	return ManagerStats{
		TotalDocs:   m.totalDocs.Load(),
		IndexedDocs: m.indexedDocs.Load(),
		Searches:    m.searches.Load(),
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

// Registry returns the term registry (may be nil).
func (m *Manager) Registry() TermRegistry {
	return m.registry
}

// PebbleRegistry returns the Pebble-backed term registry.
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

// ListTerms returns all terms, optionally filtered by field.
func (m *Manager) ListTerms(_ context.Context, field string, limit, offset int) ([]TermEntry, int64, error) {
	searchPrefix := ""
	if field != "" {
		searchPrefix = field + "\x00"
	}

	allTermIDs := m.store.GetTermsWithPrefix(field, searchPrefix)

	entries := make([]TermEntry, 0, len(allTermIDs))
	for _, termID := range allTermIDs {
		sep := strings.IndexByte(termID, 0)
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
func (m *Manager) ListTermsByPrefix(_ context.Context, field, prefix string, limit int) ([]TermEntry, error) {
	termIDs := m.store.GetTermsWithPrefix(field, prefix)

	entries := make([]TermEntry, 0, len(termIDs))
	fieldPrefix := field + "\x00"
	for _, termID := range termIDs {
		if !strings.HasPrefix(termID, fieldPrefix) {
			continue
		}
		term := termID[len(fieldPrefix):]
		entries = append(entries, TermEntry{
			Field:  field,
			Term:   term,
			TermID: termID,
			DF:     m.GetDF(termID),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

// ListTermsByFuzzy returns terms within Damerau-Levenshtein distance.
// Uses prefix-based candidate generation to avoid full term scans.
func (m *Manager) ListTermsByFuzzy(_ context.Context, field, query string, maxDistance, limit int) ([]TermEntry, error) {
	if maxDistance < 1 {
		maxDistance = 1
	}
	if maxDistance > 3 {
		maxDistance = 3
	}
	if len(query) < 2 {
		return nil, nil
	}

	// Generate targeted prefixes instead of scanning all terms
	prefixes := FuzzyPrefixes(query, maxDistance)
	fieldPrefix := field + "\x00"

	seen := make(map[string]struct{})
	var entries []TermEntry

	for _, pfx := range prefixes {
		termIDs := m.store.GetTermsWithPrefix(field, pfx)
		for _, termID := range termIDs {
			if _, ok := seen[termID]; ok {
				continue
			}
			seen[termID] = struct{}{}

			term := termID[len(fieldPrefix):]
			// Quick length filter: DL distance ≥ abs(len difference)
			if diff := len(query) - len(term); diff > maxDistance || -diff > maxDistance {
				continue
			}
			if DamerauLevenshteinDistance(query, term) <= maxDistance {
				entries = append(entries, TermEntry{
					Field:  field,
					Term:   term,
					TermID: termID,
					DF:     m.GetDF(termID),
				})
			}
		}
	}

	// Sort by DF descending — best candidates first
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].DF > entries[j].DF
	})
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

// ListTermsByRegex returns terms matching a regex pattern.
func (m *Manager) ListTermsByRegex(_ context.Context, field, pattern string, limit int) ([]TermEntry, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}

	allTermIDs := m.store.GetTermsWithPrefix(field, "")
	fieldPrefix := field + "\x00"

	var entries []TermEntry
	for _, termID := range allTermIDs {
		if !strings.HasPrefix(termID, fieldPrefix) {
			continue
		}
		term := termID[len(fieldPrefix):]
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
