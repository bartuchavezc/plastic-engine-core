package segment

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"plastic-engine-core/internal/core/search/wal"
)

// TermInfo stores metadata about a term.
type TermInfo struct {
	TermID    string    `json:"term_id"`
	CreatedAt time.Time `json:"created_at"`
	IsAlias   bool      `json:"is_alias,omitempty"`
	AliasOf   string    `json:"alias_of,omitempty"` // Original term if this is an alias
}

// MemTermRegistry is an in-memory term registry with WAL for durability.
// It provides O(1) lookups and supports alias creation for the "plastic" renaming feature.
type MemTermRegistry struct {
	mu sync.RWMutex

	// termToID maps "field:term" to term_id
	termToID map[string]string

	// idInfo maps term_id to metadata
	idInfo map[string]TermInfo

	// dfCache maps term_id to global document frequency
	dfCache map[string]int64

	// totalDocs is the total number of documents indexed
	totalDocs int64

	// avgDocLen maps field to average document length
	avgDocLen map[string]float64

	// wal for durability
	wal *wal.WAL

	// dataDir for snapshots
	dataDir string

	// dirty tracks if there are unsaved changes
	dirty atomic.Bool

	// termIDCounter for generating unique IDs
	termIDCounter uint64
}

// TermRegistrySnapshot is the serializable state of the registry.
type TermRegistrySnapshot struct {
	TermToID  map[string]string    `json:"term_to_id"`
	IDInfo    map[string]TermInfo  `json:"id_info"`
	DFCache   map[string]int64     `json:"df_cache"`
	TotalDocs int64                `json:"total_docs"`
	AvgDocLen map[string]float64   `json:"avg_doc_len"`
	Counter   uint64               `json:"counter"`
	Timestamp time.Time            `json:"timestamp"`
}

// NewMemTermRegistry creates a new in-memory term registry.
func NewMemTermRegistry(dataDir string) (*MemTermRegistry, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	walPath := filepath.Join(dataDir, "term_registry.wal")
	w, err := wal.Open(walPath, wal.Options{
		SyncMode:   wal.SyncBatch,
		BufferSize: 32 * 1024,
	})
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	r := &MemTermRegistry{
		termToID:  make(map[string]string),
		idInfo:    make(map[string]TermInfo),
		dfCache:   make(map[string]int64),
		avgDocLen: make(map[string]float64),
		wal:       w,
		dataDir:   dataDir,
	}

	// Load from snapshot if exists
	if err := r.loadSnapshot(); err != nil {
		// Snapshot doesn't exist or is corrupt, start fresh
	}

	// Replay WAL
	if err := r.replayWAL(); err != nil {
		return nil, fmt.Errorf("replay wal: %w", err)
	}

	return r, nil
}

// makeKey creates the lookup key from field and term.
func makeKey(field, term string) string {
	return field + ":" + term
}

// generateTermID creates a unique term ID.
func (r *MemTermRegistry) generateTermID(field, term string) string {
	counter := atomic.AddUint64(&r.termIDCounter, 1)
	// Use a combination of timestamp and counter for uniqueness
	return fmt.Sprintf("t%d_%x", counter, time.Now().UnixNano()&0xFFFFFF)
}

// GetOrCreate retrieves an existing term or creates a new one.
// This is O(1) - no disk I/O.
func (r *MemTermRegistry) GetOrCreate(_ context.Context, field, term string) (string, error) {
	key := makeKey(field, term)

	// Fast path: read lock
	r.mu.RLock()
	if termID, ok := r.termToID[key]; ok {
		r.mu.RUnlock()
		return termID, nil
	}
	r.mu.RUnlock()

	// Slow path: write lock
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if termID, ok := r.termToID[key]; ok {
		return termID, nil
	}

	// Create new term
	termID := r.generateTermID(field, term)
	r.termToID[key] = termID
	r.idInfo[termID] = TermInfo{
		TermID:    termID,
		CreatedAt: time.Now().UTC(),
	}
	r.dfCache[termID] = 0
	r.dirty.Store(true)

	// Log to WAL (async-safe, we hold the lock)
	if r.wal != nil {
		_, _ = r.wal.AppendAlias(wal.AliasOp{
			Field:  field,
			Term:   term,
			TermID: termID,
		})
	}

	return termID, nil
}

// Get retrieves a term ID if it exists.
func (r *MemTermRegistry) Get(_ context.Context, field, term string) (string, bool) {
	key := makeKey(field, term)

	r.mu.RLock()
	defer r.mu.RUnlock()

	termID, ok := r.termToID[key]
	return termID, ok
}

// CreateAlias creates a new term that points to an existing term_id.
// This is the core "plastic" operation - no documents are modified.
func (r *MemTermRegistry) CreateAlias(field, newTerm, existingTermID string) error {
	key := makeKey(field, newTerm)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Verify the existing term_id is valid
	if _, ok := r.idInfo[existingTermID]; !ok {
		return fmt.Errorf("term_id %s does not exist", existingTermID)
	}

	// Check if alias already exists
	if existing, ok := r.termToID[key]; ok {
		if existing == existingTermID {
			return nil // Already aliased to same term
		}
		return fmt.Errorf("term %s:%s already exists with different term_id", field, newTerm)
	}

	// Create alias
	r.termToID[key] = existingTermID
	r.dirty.Store(true)

	// Log to WAL
	if r.wal != nil {
		_, _ = r.wal.AppendAlias(wal.AliasOp{
			Field:  field,
			Term:   newTerm,
			TermID: existingTermID,
		})
	}

	return nil
}

// GetDF returns the global document frequency for a term.
func (r *MemTermRegistry) GetDF(termID string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.dfCache[termID]
}

// IncrementDF atomically increments the document frequency.
func (r *MemTermRegistry) IncrementDF(termID string, delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dfCache[termID] += delta
	r.dirty.Store(true)
}

// SetDF sets the document frequency for a term.
func (r *MemTermRegistry) SetDF(termID string, df int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dfCache[termID] = df
	r.dirty.Store(true)
}

// GetTotalDocs returns the total number of indexed documents.
func (r *MemTermRegistry) GetTotalDocs() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.totalDocs
}

// IncrementTotalDocs increments the total document count.
func (r *MemTermRegistry) IncrementTotalDocs(delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.totalDocs += delta
	r.dirty.Store(true)
}

// GetAvgDocLen returns the average document length for a field.
func (r *MemTermRegistry) GetAvgDocLen(field string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if avg, ok := r.avgDocLen[field]; ok {
		return avg
	}
	return 100 // Default
}

// SetAvgDocLen sets the average document length for a field.
func (r *MemTermRegistry) SetAvgDocLen(field string, avg float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.avgDocLen[field] = avg
	r.dirty.Store(true)
}

// Snapshot saves the current state to disk.
func (r *MemTermRegistry) Snapshot() error {
	r.mu.RLock()
	snapshot := TermRegistrySnapshot{
		TermToID:  make(map[string]string, len(r.termToID)),
		IDInfo:    make(map[string]TermInfo, len(r.idInfo)),
		DFCache:   make(map[string]int64, len(r.dfCache)),
		TotalDocs: r.totalDocs,
		AvgDocLen: make(map[string]float64, len(r.avgDocLen)),
		Counter:   r.termIDCounter,
		Timestamp: time.Now().UTC(),
	}

	for k, v := range r.termToID {
		snapshot.TermToID[k] = v
	}
	for k, v := range r.idInfo {
		snapshot.IDInfo[k] = v
	}
	for k, v := range r.dfCache {
		snapshot.DFCache[k] = v
	}
	for k, v := range r.avgDocLen {
		snapshot.AvgDocLen[k] = v
	}
	r.mu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	snapshotPath := filepath.Join(r.dataDir, "term_registry.snapshot")
	tempPath := snapshotPath + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}

	if err := os.Rename(tempPath, snapshotPath); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}

	// Truncate WAL after successful snapshot
	if r.wal != nil {
		if err := r.wal.Truncate(); err != nil {
			return fmt.Errorf("truncate wal: %w", err)
		}
	}

	r.dirty.Store(false)
	return nil
}

// loadSnapshot loads state from a snapshot file.
func (r *MemTermRegistry) loadSnapshot() error {
	snapshotPath := filepath.Join(r.dataDir, "term_registry.snapshot")

	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}

	var snapshot TermRegistrySnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	r.termToID = snapshot.TermToID
	r.idInfo = snapshot.IDInfo
	r.dfCache = snapshot.DFCache
	r.totalDocs = snapshot.TotalDocs
	r.avgDocLen = snapshot.AvgDocLen
	r.termIDCounter = snapshot.Counter

	if r.termToID == nil {
		r.termToID = make(map[string]string)
	}
	if r.idInfo == nil {
		r.idInfo = make(map[string]TermInfo)
	}
	if r.dfCache == nil {
		r.dfCache = make(map[string]int64)
	}
	if r.avgDocLen == nil {
		r.avgDocLen = make(map[string]float64)
	}

	return nil
}

// replayWAL replays operations from the WAL.
func (r *MemTermRegistry) replayWAL() error {
	walPath := filepath.Join(r.dataDir, "term_registry.wal")

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
		case wal.OpAlias:
			var op wal.AliasOp
			if err := json.Unmarshal(entry.Data, &op); err != nil {
				continue
			}
			key := makeKey(op.Field, op.Term)
			r.termToID[key] = op.TermID
			if _, ok := r.idInfo[op.TermID]; !ok {
				r.idInfo[op.TermID] = TermInfo{
					TermID:    op.TermID,
					CreatedAt: entry.Timestamp,
				}
			}
			if _, ok := r.dfCache[op.TermID]; !ok {
				r.dfCache[op.TermID] = 0
			}
		}
	}

	return nil
}

// Sync forces a sync of the WAL to disk.
func (r *MemTermRegistry) Sync() error {
	if r.wal != nil {
		return r.wal.Sync()
	}
	return nil
}

// Close closes the registry and saves state.
func (r *MemTermRegistry) Close() error {
	if r.dirty.Load() {
		if err := r.Snapshot(); err != nil {
			// Log but don't fail
		}
	}
	if r.wal != nil {
		return r.wal.Close()
	}
	return nil
}

// Stats returns statistics about the registry.
func (r *MemTermRegistry) Stats() TermRegistryStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return TermRegistryStats{
		TermCount:    len(r.termToID),
		UniqueTerms:  len(r.idInfo),
		TotalDocs:    r.totalDocs,
		FieldCount:   len(r.avgDocLen),
	}
}

// TermRegistryStats contains registry statistics.
type TermRegistryStats struct {
	TermCount   int   // Total mappings (including aliases)
	UniqueTerms int   // Unique term IDs
	TotalDocs   int64 // Total indexed documents
	FieldCount  int   // Number of fields with avgDocLen
}

// AllTermIDs returns all term IDs in the registry.
func (r *MemTermRegistry) AllTermIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.idInfo))
	for id := range r.idInfo {
		ids = append(ids, id)
	}
	return ids
}

// GetTermInfo returns metadata for a term ID.
func (r *MemTermRegistry) GetTermInfo(termID string) (TermInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, ok := r.idInfo[termID]
	return info, ok
}

// GetTermsWithPrefix returns all term IDs for terms that match a prefix in a field.
// This is used for prefix queries.
func (r *MemTermRegistry) GetTermsWithPrefix(_ context.Context, field, prefix string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keyPrefix := field + ":" + prefix
	termIDs := make([]string, 0)
	seen := make(map[string]struct{})

	for key, termID := range r.termToID {
		if len(key) >= len(keyPrefix) && key[:len(keyPrefix)] == keyPrefix {
			if _, ok := seen[termID]; !ok {
				termIDs = append(termIDs, termID)
				seen[termID] = struct{}{}
			}
		}
	}

	return termIDs
}
