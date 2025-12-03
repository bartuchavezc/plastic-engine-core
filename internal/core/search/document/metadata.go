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
func (r *MetadataResolver) Resolve(ctx context.Context, assignment shards.Assignment) (indexes.IndexDefinition, error) {
	if def, ok := r.loadFromCache(assignment.IndexID, assignment.MappingVersion); ok {
		return def, nil
	}

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

	if r.fetcher == nil {
		return indexes.IndexDefinition{}, fmt.Errorf("no index definition for %s and fetcher unavailable", assignment.IndexID)
	}

	def, err := r.fetcher.FetchIndexDefinition(ctx, assignment.IndexID)
	if err != nil {
		return indexes.IndexDefinition{}, fmt.Errorf("fetch index definition %s: %w", assignment.IndexID, err)
	}

	r.store(def)

	if def.MappingVersion != assignment.MappingVersion {
		return indexes.IndexDefinition{}, fmt.Errorf(
			"index %s mapping version mismatch. assignment=%d fetched=%d",
			assignment.IndexID, assignment.MappingVersion, def.MappingVersion,
		)
	}

	return def, nil
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
