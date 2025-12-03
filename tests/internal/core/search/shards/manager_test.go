package shards_test

import (
	"testing"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/shards"
)

func TestManagerSyncOpensNewShard(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	manager := shard.NewManager(root)
	t.Cleanup(func() {
		_ = manager.Close()
	})

	assignments := []shard.Assignment{
		{
			ID:             "idx-1-2025-10",
			IndexID:        "idx-1",
			ShardKey:       "2025-10",
			ShardStrategy:  indexes.ShardStrategyAutomatic,
			Analyzer:       "simple",
			Tokenizer:      "whitespace",
			MappingVersion: 1,
			Fields: []indexes.FieldMapping{
				{Name: "value", Type: indexes.FieldTypeInteger, Indexed: true},
			},
		},
	}

	if err := manager.Sync(assignments); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	ids := manager.ListShardIDs()
	if len(ids) != 1 || ids[0] != "idx-1-2025-10" {
		t.Fatalf("ListShardIDs = %v, want [idx-1-2025-10]", ids)
	}
}

func TestManagerSyncSkipsExistingShard(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	manager := shard.NewManager(root)
	t.Cleanup(func() {
		_ = manager.Close()
	})

	assignments := []shard.Assignment{{
		ID:             "idx-1-2025-10",
		IndexID:        "idx-1",
		ShardKey:       "2025-10",
		ShardStrategy:  indexes.ShardStrategyAutomatic,
		Analyzer:       "simple",
		Tokenizer:      "whitespace",
		MappingVersion: 1,
		Fields: []indexes.FieldMapping{
			{Name: "value", Type: indexes.FieldTypeInteger, Indexed: true},
		},
	}}
	if err := manager.Sync(assignments); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	if err := manager.Sync(assignments); err != nil {
		t.Fatalf("second Sync: %v", err)
	}

	if got := len(manager.ListShardIDs()); got != 1 {
		t.Fatalf("ListShardIDs length = %d, want 1", got)
	}
}

func TestManagerSyncReturnsErrorOnOpenFailure(t *testing.T) {
	t.Parallel()

	manager := shard.NewManager("/invalid/path/that/should/fail")
	t.Cleanup(func() {
		_ = manager.Close()
	})
	if err := manager.Sync([]shard.Assignment{{
		ID:             "shard-fail",
		IndexID:        "idx",
		ShardKey:       "default",
		ShardStrategy:  indexes.ShardStrategyAutomatic,
		Analyzer:       "simple",
		Tokenizer:      "whitespace",
		MappingVersion: 1,
		Fields: []indexes.FieldMapping{
			{Name: "value", Type: indexes.FieldTypeInteger, Indexed: true},
		},
	}}); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestManagerLoadsExistingShardsFromDisk(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	initial := shard.NewManager(root)
	assignments := []shard.Assignment{{
		ID:             "idx-1-2025-10",
		IndexID:        "idx-1",
		ShardKey:       "2025-10",
		ShardStrategy:  indexes.ShardStrategyAutomatic,
		Analyzer:       "simple",
		Tokenizer:      "whitespace",
		MappingVersion: 1,
		Fields: []indexes.FieldMapping{
			{Name: "value", Type: indexes.FieldTypeInteger, Indexed: true},
		},
	}}
	if err := initial.Sync(assignments); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("Close initial manager: %v", err)
	}

	reloaded := shard.NewManager(root)
	t.Cleanup(func() {
		_ = reloaded.Close()
	})
	if err := reloaded.Sync(nil); err != nil {
		t.Fatalf("Sync after reload: %v", err)
	}

	ids := reloaded.ListShardIDs()
	if len(ids) != 1 || ids[0] != "idx-1-2025-10" {
		t.Fatalf("ListShardIDs after reload = %v, want [idx-1-2025-10]", ids)
	}
}
