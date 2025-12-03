package document_test

import (
	"context"
	"errors"
	"testing"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	shards "plastic-engine-core/internal/core/search/shards"
)

func TestAssignmentProviderReturnsEnrichedAssignment(t *testing.T) {
	t.Parallel()

	shard := &shards.Shard{
		ID: "shard-1",
		Info: shards.Assignment{
			ID:             "shard-1",
			IndexID:        "idx",
			ShardKey:       "default",
			Analyzer:       "simple",
			Tokenizer:      "whitespace",
			MappingVersion: 1,
			ShardStrategy:  indexes.ShardStrategyAutomatic,
			Fields:         []indexes.FieldMapping{{Name: "order_id", Type: indexes.FieldTypeKeyword, Indexed: true}},
		},
	}

	registry := &stubRegistry{
		shards: map[string]*shards.Shard{
			"shard-1": shard,
		},
	}

	resolver := document.NewMetadataResolver(nil)
	provider := document.NewAssignmentProvider(registry, resolver)

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

	provider := document.NewAssignmentProvider(&stubRegistry{shards: map[string]*shards.Shard{}}, document.NewMetadataResolver(nil))

	_, _, err := provider.AssignmentForShard(context.Background(), "missing")
	if err == nil {
		t.Fatalf("expected error for missing shard")
	}
	if !errors.Is(err, document.ErrShardNotLoaded) {
		t.Fatalf("expected ErrShardNotLoaded, got %v", err)
	}
}

type stubRegistry struct {
	shards map[string]*shards.Shard
}

func (s *stubRegistry) GetShard(id string) (*shards.Shard, bool) {
	sh, ok := s.shards[id]
	return sh, ok
}
