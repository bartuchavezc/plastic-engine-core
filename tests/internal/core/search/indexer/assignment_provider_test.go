package indexer_test

import (
	"context"
	"errors"
	"testing"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/search/indexer"
	searchshard "plastic-engine-core/internal/core/search/shard"
)

func TestAssignmentProviderReturnsEnrichedAssignment(t *testing.T) {
	t.Parallel()

	shard := &searchshard.Shard{
		ID: "shard-1",
		Info: searchshard.Assignment{
			ID:             "shard-1",
			IndexID:        "idx",
			ShardKey:       "default",
			Analyzer:       "simple",
			Tokenizer:      "whitespace",
			MappingVersion: 1,
			ShardStrategy:  coreindex.ShardStrategyAutomatic,
			Fields:         []coreindex.FieldMapping{{Name: "order_id", Type: coreindex.FieldTypeKeyword, Indexed: true}},
		},
	}

	registry := &stubRegistry{
		shards: map[string]*searchshard.Shard{
			"shard-1": shard,
		},
	}

	resolver := indexer.NewMetadataResolver(nil)
	provider := indexer.NewAssignmentProvider(registry, resolver)

	assignment, def, err := provider.AssignmentForShard(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("AssignmentForShard: %v", err)
	}

	if assignment.IndexID != "idx" || def.ID != "idx" {
		t.Fatalf("expected index id idx, got assignment %s def %s", assignment.IndexID, def.ID)
	}

	if len(assignment.Fields) != 1 || assignment.Fields[0].Name != "order_id" {
		t.Fatalf("unexpected fields: %+v", assignment.Fields)
	}
}

func TestAssignmentProviderReturnsErrorWhenShardMissing(t *testing.T) {
	t.Parallel()

	provider := indexer.NewAssignmentProvider(&stubRegistry{shards: map[string]*searchshard.Shard{}}, indexer.NewMetadataResolver(nil))

	_, _, err := provider.AssignmentForShard(context.Background(), "missing")
	if err == nil {
		t.Fatalf("expected error for missing shard")
	}
	if !errors.Is(err, indexer.ErrShardNotLoaded) {
		t.Fatalf("expected ErrShardNotLoaded, got %v", err)
	}
}

type stubRegistry struct {
	shards map[string]*searchshard.Shard
}

func (s *stubRegistry) GetShard(id string) (*searchshard.Shard, bool) {
	sh, ok := s.shards[id]
	return sh, ok
}
