package query

import (
	"context"
	"fmt"

	"plastic-engine-core/internal/core/search/segment"
	"plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// ShardGetter provides access to shards.
type ShardGetter interface {
	GetShard(id string) (*shards.Shard, bool)
}

// TermLookupService provides term lookup operations across shards.
type TermLookupService struct {
	shards ShardGetter
	log    logger.Logger
}

// NewTermLookupService creates a new term lookup service.
func NewTermLookupService(shards ShardGetter, log logger.Logger) *TermLookupService {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &TermLookupService{
		shards: shards,
		log:    log,
	}
}

// ListTerms returns all terms for a shard, optionally filtered by field.
func (s *TermLookupService) ListTerms(ctx context.Context, shardID, field string, limit, offset int) ([]segment.TermEntry, int64, error) {
	shard, ok := s.shards.GetShard(shardID)
	if !ok {
		return nil, 0, fmt.Errorf("shard %s not found", shardID)
	}

	if shard.Segments == nil {
		return nil, 0, fmt.Errorf("shard %s has no segment manager", shardID)
	}

	return shard.Segments.ListTerms(ctx, field, limit, offset)
}

// ListTermsByPrefix returns terms matching a prefix.
func (s *TermLookupService) ListTermsByPrefix(ctx context.Context, shardID, field, prefix string, limit int) ([]segment.TermEntry, error) {
	shard, ok := s.shards.GetShard(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	if shard.Segments == nil {
		return nil, fmt.Errorf("shard %s has no segment manager", shardID)
	}

	return shard.Segments.ListTermsByPrefix(ctx, field, prefix, limit)
}

// ListTermsByFuzzy returns terms within Levenshtein distance.
func (s *TermLookupService) ListTermsByFuzzy(ctx context.Context, shardID, field, query string, maxDistance, limit int) ([]segment.TermEntry, error) {
	shard, ok := s.shards.GetShard(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	if shard.Segments == nil {
		return nil, fmt.Errorf("shard %s has no segment manager", shardID)
	}

	return shard.Segments.ListTermsByFuzzy(ctx, field, query, maxDistance, limit)
}

// ListTermsByRegex returns terms matching a regex pattern.
func (s *TermLookupService) ListTermsByRegex(ctx context.Context, shardID, field, pattern string, limit int) ([]segment.TermEntry, error) {
	shard, ok := s.shards.GetShard(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	if shard.Segments == nil {
		return nil, fmt.Errorf("shard %s has no segment manager", shardID)
	}

	return shard.Segments.ListTermsByRegex(ctx, field, pattern, limit)
}
