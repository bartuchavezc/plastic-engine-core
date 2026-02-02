package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"plastic-engine-core/internal/core/stream_proxy/hydration"
)

const (
	// InternalConnectorName is the identifier for the internal connector.
	InternalConnectorName = "internal"

	// docPrefix is the key prefix for stored documents.
	docPrefix = "doc:"
)

// InternalStore defines the storage interface for the internal connector.
type InternalStore interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
}

// InternalConnector stores documents locally using the shard's Pebble store.
// This is useful for testing and single-node deployments.
type InternalConnector struct {
	store InternalStore
	mu    sync.RWMutex
}

// NewInternalConnector creates an internal connector backed by the given store.
func NewInternalConnector(store InternalStore) *InternalConnector {
	return &InternalConnector{
		store: store,
	}
}

// Name returns the connector type.
func (c *InternalConnector) Name() string {
	return InternalConnectorName
}

// Fetch retrieves a document by key from local storage.
func (c *InternalConnector) Fetch(ctx context.Context, key string) (hydration.Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	storeKey := docPrefix + key

	raw, err := c.store.Get(storeKey)
	if err != nil {
		// Check if it's a not-found error
		if isNotFound(err) {
			return hydration.Document{
				ID:    key,
				Found: false,
			}, nil
		}
		return hydration.Document{}, fmt.Errorf("fetch document %s: %w", key, err)
	}

	var stored storedDocument
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return hydration.Document{}, fmt.Errorf("decode document %s: %w", key, err)
	}

	return hydration.Document{
		ID:     key,
		Source: stored.Source,
		Metadata: hydration.DocumentMeta{
			StoredAt: stored.StoredAt,
			Size:     int64(len(raw)),
		},
		Found: true,
	}, nil
}

// FetchBatch retrieves multiple documents by key.
func (c *InternalConnector) FetchBatch(ctx context.Context, keys []string) (map[string]hydration.Document, error) {
	results := make(map[string]hydration.Document, len(keys))

	for _, key := range keys {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		doc, err := c.Fetch(ctx, key)
		if err != nil {
			// Log error but continue with other docs
			results[key] = hydration.Document{ID: key, Found: false}
			continue
		}
		results[key] = doc
	}

	return results, nil
}

// Store saves a document to local storage.
func (c *InternalConnector) Store(ctx context.Context, key string, source map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	stored := storedDocument{
		Source:   source,
		StoredAt: time.Now().UTC(),
	}

	data, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("encode document %s: %w", key, err)
	}

	storeKey := docPrefix + key
	if err := c.store.Set(storeKey, string(data)); err != nil {
		return fmt.Errorf("store document %s: %w", key, err)
	}

	return nil
}

// Delete removes a document from local storage.
func (c *InternalConnector) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	storeKey := docPrefix + key
	if err := c.store.Delete(storeKey); err != nil && !isNotFound(err) {
		return fmt.Errorf("delete document %s: %w", key, err)
	}

	return nil
}

// Close releases resources (no-op for internal connector).
func (c *InternalConnector) Close() error {
	return nil
}

// storedDocument is the internal storage format.
type storedDocument struct {
	Source   map[string]any `json:"source"`
	StoredAt time.Time      `json:"stored_at"`
}

// isNotFound checks if an error indicates a missing key.
// This is a simple heuristic that works with pebble.ErrNotFound.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "pebble: not found"
}

// Ensure InternalConnector implements Connector.
var _ hydration.Connector = (*InternalConnector)(nil)

