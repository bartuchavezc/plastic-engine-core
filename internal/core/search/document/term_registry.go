package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"plastic-engine-core/internal/adapters/storage/pebble"
)

// TermEntry holds metadata for a registered term.
type TermEntry struct {
	TermID    string    `json:"term_id"`
	DF        int64     `json:"df"`         // Document frequency
	CreatedAt time.Time `json:"created_at"`
}

// TermRegistryStore defines the storage operations required by TermRegistry.
type TermRegistryStore interface {
	Get(key string) (string, error)
	Set(key string, value string) error
}

// TermRegistry manages the mapping from (field, term) to term_id.
// This enables O(1) term renames and efficient updates.
type TermRegistry struct {
	store TermRegistryStore
}

// NewTermRegistry creates a new TermRegistry backed by the given store.
func NewTermRegistry(store TermRegistryStore) *TermRegistry {
	return &TermRegistry{store: store}
}

// GetOrCreate retrieves an existing term entry or creates a new one.
// If the term doesn't exist, a new entry with DF=0 is created.
func (r *TermRegistry) GetOrCreate(_ context.Context, field, term string) (TermEntry, error) {
	key := pebble.TermRegistryKey(field, term)

	raw, err := r.store.Get(key)
	if err == nil {
		var entry TermEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			return TermEntry{}, fmt.Errorf("decode term entry: %w", err)
		}
		return entry, nil
	}

	if !pebble.IsNotFound(err) {
		return TermEntry{}, fmt.Errorf("get term entry: %w", err)
	}

	// Create new entry
	entry := TermEntry{
		TermID:    pebble.GenerateTermID(field, term),
		DF:        0,
		CreatedAt: time.Now().UTC(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return TermEntry{}, fmt.Errorf("encode term entry: %w", err)
	}

	if err := r.store.Set(key, string(data)); err != nil {
		return TermEntry{}, fmt.Errorf("save term entry: %w", err)
	}

	return entry, nil
}

// Get retrieves a term entry if it exists.
// Returns (entry, true, nil) if found, (empty, false, nil) if not found.
func (r *TermRegistry) Get(_ context.Context, field, term string) (TermEntry, bool, error) {
	key := pebble.TermRegistryKey(field, term)

	raw, err := r.store.Get(key)
	if err != nil {
		if pebble.IsNotFound(err) {
			return TermEntry{}, false, nil
		}
		return TermEntry{}, false, fmt.Errorf("get term entry: %w", err)
	}

	var entry TermEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return TermEntry{}, false, fmt.Errorf("decode term entry: %w", err)
	}

	return entry, true, nil
}

// IncrementDF increases the document frequency for a term by 1.
// The term must exist or an error is returned.
func (r *TermRegistry) IncrementDF(_ context.Context, field, term string) error {
	return r.updateDF(field, term, 1)
}

// DecrementDF decreases the document frequency for a term by 1.
// The term must exist or an error is returned.
// DF will not go below 0.
func (r *TermRegistry) DecrementDF(_ context.Context, field, term string) error {
	return r.updateDF(field, term, -1)
}

func (r *TermRegistry) updateDF(field, term string, delta int64) error {
	key := pebble.TermRegistryKey(field, term)

	raw, err := r.store.Get(key)
	if err != nil {
		if pebble.IsNotFound(err) {
			return errors.New("term not found in registry")
		}
		return fmt.Errorf("get term entry: %w", err)
	}

	var entry TermEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return fmt.Errorf("decode term entry: %w", err)
	}

	entry.DF += delta
	if entry.DF < 0 {
		entry.DF = 0
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode term entry: %w", err)
	}

	if err := r.store.Set(key, string(data)); err != nil {
		return fmt.Errorf("save term entry: %w", err)
	}

	return nil
}

// BatchTermRegistry provides batch operations for term registration.
type BatchTermRegistry struct {
	store   TermRegistryStore
	entries map[string]TermEntry // key -> entry
}

// NewBatchTermRegistry creates a batch registry for bulk operations.
func NewBatchTermRegistry(store TermRegistryStore) *BatchTermRegistry {
	return &BatchTermRegistry{
		store:   store,
		entries: make(map[string]TermEntry),
	}
}

// GetOrCreate retrieves or creates a term, caching in the batch.
func (b *BatchTermRegistry) GetOrCreate(ctx context.Context, field, term string) (TermEntry, error) {
	key := pebble.TermRegistryKey(field, term)

	// Check batch cache first
	if entry, ok := b.entries[key]; ok {
		return entry, nil
	}

	// Try to load from store
	raw, err := b.store.Get(key)
	if err == nil {
		var entry TermEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			return TermEntry{}, fmt.Errorf("decode term entry: %w", err)
		}
		b.entries[key] = entry
		return entry, nil
	}

	if !pebble.IsNotFound(err) {
		return TermEntry{}, fmt.Errorf("get term entry: %w", err)
	}

	// Create new entry
	entry := TermEntry{
		TermID:    pebble.GenerateTermID(field, term),
		DF:        0,
		CreatedAt: time.Now().UTC(),
	}
	b.entries[key] = entry

	return entry, nil
}

// IncrementDF increases DF for a cached term.
func (b *BatchTermRegistry) IncrementDF(field, term string) error {
	key := pebble.TermRegistryKey(field, term)
	entry, ok := b.entries[key]
	if !ok {
		return errors.New("term not in batch cache")
	}
	entry.DF++
	b.entries[key] = entry
	return nil
}

// Commit persists all cached entries to the store.
func (b *BatchTermRegistry) Commit() error {
	for key, entry := range b.entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode term entry: %w", err)
		}
		if err := b.store.Set(key, string(data)); err != nil {
			return fmt.Errorf("save term entry %s: %w", key, err)
		}
	}
	return nil
}

