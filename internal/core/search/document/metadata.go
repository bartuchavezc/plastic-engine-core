package document

import (
	"context"
	"fmt"
	"sync"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	shards "plastic-engine-core/internal/core/search/shards"
)

// DefinitionFetcher fetches index definitions from an external source (e.g. coordinator).
type DefinitionFetcher interface {
	FetchIndexDefinition(ctx context.Context, indexID string) (indexes.IndexDefinition, error)
}

// MetadataResolver provides index definitions matching shard assignments.
type MetadataResolver struct {
	fetcher DefinitionFetcher

	mu    sync.RWMutex
	cache map[string]indexes.IndexDefinition
}

// NewMetadataResolver builds a resolver using the provided fetcher.
func NewMetadataResolver(fetcher DefinitionFetcher) *MetadataResolver {
	return &MetadataResolver{
		fetcher: fetcher,
		cache:   make(map[string]indexes.IndexDefinition),
	}
}

// Resolve returns an up-to-date index definition for the given assignment.
// If the cached version is outdated, it fetches the latest from the coordinator.
func (r *MetadataResolver) Resolve(ctx context.Context, assignment shards.Assignment) (indexes.IndexDefinition, error) {
	// Check cache first with exact version match
	if def, ok := r.loadFromCache(assignment.IndexID, assignment.MappingVersion); ok {
		return def, nil
	}

	// If assignment has embedded fields, use them directly
	if len(assignment.Fields) > 0 {
		def := indexes.IndexDefinition{
			ID:               assignment.IndexID,
			ShardStrategy:    assignment.ShardStrategy,
			DefaultAnalyzer:  assignment.Analyzer,
			DefaultTokenizer: assignment.Tokenizer,
			FieldMappings:    assignment.Fields,
			MappingVersion:   assignment.MappingVersion,
		}
		r.store(def)
		return def, nil
	}

	// Fetch from coordinator
	if r.fetcher == nil {
		return indexes.IndexDefinition{}, fmt.Errorf("no index definition for %s and fetcher unavailable", assignment.IndexID)
	}

	def, err := r.fetcher.FetchIndexDefinition(ctx, assignment.IndexID)
	if err != nil {
		return indexes.IndexDefinition{}, fmt.Errorf("fetch index definition %s: %w", assignment.IndexID, err)
	}

	r.store(def)

	// Return the fetched definition even if version differs
	// The caller should handle version mismatches appropriately
	return def, nil
}

// RefreshMapping fetches the latest mapping for an index from the coordinator.
// This implements part of the search.MappingRefresher interface.
func (r *MetadataResolver) RefreshMapping(ctx context.Context, indexID string) error {
	if r.fetcher == nil {
		return fmt.Errorf("fetcher unavailable")
	}

	def, err := r.fetcher.FetchIndexDefinition(ctx, indexID)
	if err != nil {
		return fmt.Errorf("fetch index definition %s: %w", indexID, err)
	}

	r.store(def)
	return nil
}

// GetLocalVersion returns the cached mapping version for an index.
// Returns 0 if the index is not in cache.
// This implements part of the search.MappingRefresher interface.
func (r *MetadataResolver) GetLocalVersion(indexID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if def, ok := r.cache[indexID]; ok {
		return def.MappingVersion
	}
	return 0
}

// Invalidate removes an index from the cache, forcing a refresh on next access.
func (r *MetadataResolver) Invalidate(indexID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cache, indexID)
}

func (r *MetadataResolver) loadFromCache(indexID string, version int) (indexes.IndexDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	def, ok := r.cache[indexID]
	if !ok {
		return indexes.IndexDefinition{}, false
	}

	if def.MappingVersion != version {
		return indexes.IndexDefinition{}, false
	}

	return def, true
}

func (r *MetadataResolver) store(def indexes.IndexDefinition) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cache[def.ID] = def
}
