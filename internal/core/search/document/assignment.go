package document

import (
	"context"
	"errors"
	"fmt"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	shards "plastic-engine-core/internal/core/search/shards"
)

var (
	// ErrShardNotLoaded is returned when the requested shard is unknown to the registry.
	ErrShardNotLoaded = errors.New("shard not loaded in registry")
)

// ShardRegistry exposes read access to shards managed in-memory.
type ShardRegistry interface {
	GetShard(id string) (*shards.Shard, bool)
}

// AssignmentProvider resolves assignments enriched with fresh metadata.
type AssignmentProvider struct {
	registry ShardRegistry
	resolver *MetadataResolver
}

// NewAssignmentProvider builds a provider backed by the registry and resolver.
func NewAssignmentProvider(registry ShardRegistry, resolver *MetadataResolver) *AssignmentProvider {
	return &AssignmentProvider{
		registry: registry,
		resolver: resolver,
	}
}

// AssignmentForShard returns the current assignment metadata for the shard.
func (p *AssignmentProvider) AssignmentForShard(ctx context.Context, shardID string) (shards.Assignment, indexes.IndexDefinition, error) {
	if p.registry == nil {
		return shards.Assignment{}, indexes.IndexDefinition{}, fmt.Errorf("assignment provider missing registry")
	}
	sh, ok := p.registry.GetShard(shardID)
	if !ok || sh == nil {
		return shards.Assignment{}, indexes.IndexDefinition{}, ErrShardNotLoaded
	}

	assignment := sh.Info // copy

	def, err := p.resolver.Resolve(ctx, assignment)
	if err != nil {
		return shards.Assignment{}, indexes.IndexDefinition{}, err
	}

	assignment.Analyzer = def.DefaultAnalyzer
	assignment.Tokenizer = def.DefaultTokenizer
	assignment.MappingVersion = def.MappingVersion
	assignment.Fields = def.FieldMappings
	assignment.ShardStrategy = def.ShardStrategy

	return assignment, def, nil
}
