package indexer

import (
	"context"
	"fmt"
	"sync"

	coreindex "plastic-engine-core/internal/core/index"
	searchshard "plastic-engine-core/internal/core/search/shard"
)

// DefinitionFetcher fetches index definitions from an external source (e.g. coordinator).
type DefinitionFetcher interface {
	FetchIndexDefinition(ctx context.Context, indexID string) (coreindex.IndexDefinition, error)
}

// MetadataResolver provides index definitions matching shard assignments.
type MetadataResolver struct {
	fetcher DefinitionFetcher

	mu    sync.RWMutex
	cache map[string]coreindex.IndexDefinition
}

// NewMetadataResolver builds a resolver using the provided fetcher.
func NewMetadataResolver(fetcher DefinitionFetcher) *MetadataResolver {
	return &MetadataResolver{
		fetcher: fetcher,
		cache:   make(map[string]coreindex.IndexDefinition),
	}
}

// Resolve returns an up-to-date index definition for the given assignment.
func (r *MetadataResolver) Resolve(ctx context.Context, assignment searchshard.Assignment) (coreindex.IndexDefinition, error) {
	if def, ok := r.loadFromCache(assignment.IndexID, assignment.MappingVersion); ok {
		return def, nil
	}

	if len(assignment.Fields) > 0 {
		def := coreindex.IndexDefinition{
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
		return coreindex.IndexDefinition{}, fmt.Errorf("no index definition for %s and fetcher unavailable", assignment.IndexID)
	}

	def, err := r.fetcher.FetchIndexDefinition(ctx, assignment.IndexID)
	if err != nil {
		return coreindex.IndexDefinition{}, fmt.Errorf("fetch index definition %s: %w", assignment.IndexID, err)
	}

	r.store(def)

	if def.MappingVersion != assignment.MappingVersion {
		return coreindex.IndexDefinition{}, fmt.Errorf(
			"index %s mapping version mismatch. assignment=%d fetched=%d",
			assignment.IndexID, assignment.MappingVersion, def.MappingVersion,
		)
	}

	return def, nil
}

func (r *MetadataResolver) loadFromCache(indexID string, version int) (coreindex.IndexDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	def, ok := r.cache[indexID]
	if !ok {
		return coreindex.IndexDefinition{}, false
	}

	if def.MappingVersion != version {
		return coreindex.IndexDefinition{}, false
	}

	return def, true
}

func (r *MetadataResolver) store(def coreindex.IndexDefinition) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cache[def.ID] = def
}
