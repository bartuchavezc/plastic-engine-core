package hydration

import (
	"context"
	"fmt"
	"sync"

	"plastic-engine-core/internal/pkg/logger"
)

// ConnectorFactory creates connectors from config.
type ConnectorFactory func(connectorType string, settings map[string]string) (Connector, error)

// ForwardIndexReader reads the key field from the forward index.
type ForwardIndexReader interface {
	// GetFieldValue retrieves a field value from a document's forward index.
	GetFieldValue(ctx context.Context, docID, fieldName string) (string, error)
}

// Hydrator enriches search results with full document content.
type Hydrator struct {
	connectorFactory ConnectorFactory
	forwardReader    ForwardIndexReader
	log              logger.Logger

	mu         sync.RWMutex
	connectors map[string]Connector // indexID -> connector (cached)
}

// NewHydrator creates a hydrator with the given dependencies.
func NewHydrator(factory ConnectorFactory, forwardReader ForwardIndexReader, log logger.Logger) *Hydrator {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Hydrator{
		connectorFactory: factory,
		forwardReader:    forwardReader,
		log:              log,
		connectors:       make(map[string]Connector),
	}
}

// HydratedHit is a search hit enriched with document source.
type HydratedHit struct {
	DocID   string         `json:"doc_id"`
	ShardID string         `json:"shard_id"`
	Score   float64        `json:"score"`
	Source  map[string]any `json:"_source,omitempty"`
	Found   bool           `json:"_found"`
}

// HydrateRequest contains the parameters for hydration.
type HydrateRequest struct {
	IndexID string
	Config  Config
	Hits    []Hit
}

// Hit represents a search result to hydrate.
type Hit struct {
	DocID   string
	ShardID string
	Score   float64
}

// Hydrate enriches hits with document source from the configured connector.
func (h *Hydrator) Hydrate(ctx context.Context, req HydrateRequest) ([]HydratedHit, error) {
	if !req.Config.Enabled {
		// Hydration disabled, return hits without source
		return h.convertWithoutSource(req.Hits), nil
	}

	// Get or create connector for this index
	conn, err := h.getConnector(req.IndexID, req.Config)
	if err != nil {
		return nil, fmt.Errorf("get connector: %w", err)
	}

	// Extract keys from forward index using KeyField
	keys, keyToDoc := h.extractKeys(ctx, req.Hits, req.Config.KeyField)
	if len(keys) == 0 {
		return h.convertWithoutSource(req.Hits), nil
	}

	// Fetch documents in batch
	docs, err := conn.FetchBatch(ctx, keys)
	if err != nil {
		h.log.Error("batch fetch failed",
			logger.Field{Key: "index_id", Value: req.IndexID},
			logger.Field{Key: "error", Value: err})
		return h.convertWithoutSource(req.Hits), nil
	}

	// Build hydrated hits
	results := make([]HydratedHit, len(req.Hits))
	for i, hit := range req.Hits {
		results[i] = HydratedHit{
			DocID:   hit.DocID,
			ShardID: hit.ShardID,
			Score:   hit.Score,
			Found:   false,
		}

		if key, ok := keyToDoc[hit.DocID]; ok {
			if doc, ok := docs[key]; ok && doc.Found {
				results[i].Source = doc.Source
				results[i].Found = true
			}
		}
	}

	return results, nil
}

// HydrateWithConnector hydrates using a pre-configured connector.
// Useful for internal connector when you already have the store.
func (h *Hydrator) HydrateWithConnector(ctx context.Context, conn Connector, keyField string, hits []Hit) ([]HydratedHit, error) {
	keys, keyToDoc := h.extractKeys(ctx, hits, keyField)
	if len(keys) == 0 {
		return h.convertWithoutSource(hits), nil
	}

	docs, err := conn.FetchBatch(ctx, keys)
	if err != nil {
		return h.convertWithoutSource(hits), nil
	}

	results := make([]HydratedHit, len(hits))
	for i, hit := range hits {
		results[i] = HydratedHit{
			DocID:   hit.DocID,
			ShardID: hit.ShardID,
			Score:   hit.Score,
			Found:   false,
		}

		if key, ok := keyToDoc[hit.DocID]; ok {
			if doc, ok := docs[key]; ok && doc.Found {
				results[i].Source = doc.Source
				results[i].Found = true
			}
		}
	}

	return results, nil
}

// extractKeys gets the key values from forward index for each hit.
func (h *Hydrator) extractKeys(ctx context.Context, hits []Hit, keyField string) ([]string, map[string]string) {
	keys := make([]string, 0, len(hits))
	keyToDoc := make(map[string]string, len(hits)) // docID -> key

	for _, hit := range hits {
		// If keyField is "_id", use docID directly
		if keyField == "_id" || keyField == "" {
			keys = append(keys, hit.DocID)
			keyToDoc[hit.DocID] = hit.DocID
			continue
		}

		// Otherwise, read from forward index
		if h.forwardReader != nil {
			key, err := h.forwardReader.GetFieldValue(ctx, hit.DocID, keyField)
			if err != nil {
				h.log.Debug("failed to read key field",
					logger.Field{Key: "doc_id", Value: hit.DocID},
					logger.Field{Key: "field", Value: keyField},
					logger.Field{Key: "error", Value: err})
				continue
			}
			keys = append(keys, key)
			keyToDoc[hit.DocID] = key
		}
	}

	return keys, keyToDoc
}

func (h *Hydrator) getConnector(indexID string, config Config) (Connector, error) {
	h.mu.RLock()
	if conn, ok := h.connectors[indexID]; ok {
		h.mu.RUnlock()
		return conn, nil
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := h.connectors[indexID]; ok {
		return conn, nil
	}

	// Create new connector
	conn, err := h.connectorFactory(config.Connector, config.Settings)
	if err != nil {
		return nil, err
	}

	h.connectors[indexID] = conn
	return conn, nil
}

func (h *Hydrator) convertWithoutSource(hits []Hit) []HydratedHit {
	results := make([]HydratedHit, len(hits))
	for i, hit := range hits {
		results[i] = HydratedHit{
			DocID:   hit.DocID,
			ShardID: hit.ShardID,
			Score:   hit.Score,
			Found:   false,
		}
	}
	return results
}

// Close releases all cached connectors.
func (h *Hydrator) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var firstErr error
	for _, conn := range h.connectors {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	h.connectors = make(map[string]Connector)
	return firstErr
}

