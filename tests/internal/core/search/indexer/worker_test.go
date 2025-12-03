package indexer_test

import (
	"context"
	"testing"
	"time"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/search/indexer"
	searchshard "plastic-engine-core/internal/core/search/shard"
	"plastic-engine-core/internal/helpers"
)

func TestShardWorkerProcessesDocument(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := indexer.NewIndexWriter(store)

	registry := &stubShardRegistry{
		shards: map[string]*searchshard.Shard{
			"shard-1": {
				ID: "shard-1",
				Info: searchshard.Assignment{
					ID:             "shard-1",
					IndexID:        "idx",
					ShardKey:       "default",
					ShardStrategy:  coreindex.ShardStrategyAutomatic,
					Analyzer:       "simple",
					Tokenizer:      "whitespace",
					MappingVersion: 1,
					Fields: []coreindex.FieldMapping{
						{Name: "title", Type: coreindex.FieldTypeText, Indexed: true},
					},
				},
			},
		},
	}

	resolver := indexer.NewMetadataResolver(nil)
	provider := indexer.NewAssignmentProvider(registry, resolver)

	planner := indexer.NewFieldPlanner(indexer.TokenizerFactory{}, indexer.AnalyzerFactory{})

	worker := indexer.NewShardWorker(
		"shard-1",
		registry.shards["shard-1"],
		indexer.ShardWorkerConfig{MaxWorkers: 1, QueueCapacity: 2},
		planner,
		writer,
		provider,
		helpers.DefaultLogger(),
	)
	defer worker.Close()

	cmd := indexer.Command{
		DocumentID: "doc-1",
		Payload: map[string]any{
			"title": "Hello Plastic Engine",
		},
	}

	if err := worker.Submit(context.Background(), indexer.WorkItem{Command: cmd}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	requireEventually(t, 500*time.Millisecond, func() bool {
		_, err := store.Get("inv:title:hello:doc-1")
		return err == nil
	})
}

func TestShardWorkerBackpressure(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := indexer.NewIndexWriter(store)

	registry := &stubShardRegistry{
		shards: map[string]*searchshard.Shard{
			"shard-1": {
				ID: "shard-1",
				Info: searchshard.Assignment{
					ID:             "shard-1",
					IndexID:        "idx",
					ShardKey:       "default",
					ShardStrategy:  coreindex.ShardStrategyAutomatic,
					Analyzer:       "simple",
					Tokenizer:      "whitespace",
					MappingVersion: 1,
					Fields: []coreindex.FieldMapping{
						{Name: "title", Type: coreindex.FieldTypeText, Indexed: true},
					},
				},
			},
		},
	}

	resolver := indexer.NewMetadataResolver(nil)
	provider := indexer.NewAssignmentProvider(registry, resolver)
	planner := indexer.NewFieldPlanner(indexer.TokenizerFactory{}, indexer.AnalyzerFactory{})

	worker := indexer.NewShardWorker(
		"shard-1",
		registry.shards["shard-1"],
		indexer.ShardWorkerConfig{MaxWorkers: 1, QueueCapacity: 1},
		planner,
		writer,
		provider,
		helpers.DefaultLogger(),
	)
	defer worker.Close()

	ctx := context.Background()
	if err := worker.Submit(ctx, indexer.WorkItem{Command: indexer.Command{DocumentID: "doc-1", Payload: map[string]any{"title": "hello"}}}); err != nil {
		t.Fatalf("Submit first: %v", err)
	}

	if err := worker.Submit(ctx, indexer.WorkItem{Command: indexer.Command{DocumentID: "doc-2", Payload: map[string]any{"title": "world"}}}); err == nil {
		t.Fatalf("expected backpressure error for second item")
	}
}

type stubShardRegistry struct {
	shards map[string]*searchshard.Shard
}

func (s *stubShardRegistry) GetShard(id string) (*searchshard.Shard, bool) {
	sh, ok := s.shards[id]
	return sh, ok
}
