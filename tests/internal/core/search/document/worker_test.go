package document_test

import (
	"context"
	"testing"
	"time"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

func TestShardWorkerProcessesDocument(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store, nil)

	registry := &stubShardRegistry{
		shards: map[string]*shards.Shard{
			"shard-1": {
				ID: "shard-1",
				Info: shards.Assignment{
					ID:             "shard-1",
					IndexID:        "idx",
					ShardKey:       "default",
					ShardStrategy:  indexes.ShardStrategyAutomatic,
					Analyzer:       "simple",
					Tokenizer:      "whitespace",
					MappingVersion: 1,
					Fields: []indexes.FieldMapping{
						{Name: "title", Type: indexes.FieldTypeText, Indexed: true},
					},
				},
			},
		},
	}

	resolver := document.NewMetadataResolver(nil)
	provider := document.NewAssignmentProvider(registry, resolver)

	planner := document.NewFieldPlanner(document.TokenizerFactory{}, document.AnalyzerFactory{})

	worker := document.NewShardWorker(
		"shard-1",
		registry.shards["shard-1"],
		document.ShardWorkerConfig{MaxWorkers: 1, QueueCapacity: 2},
		planner,
		writer,
		provider,
		logger.DefaultLogger(),
	)
	defer worker.Close()

	cmd := document.Command{
		DocumentID: "doc-1",
		RawPayload: []byte(`{"title": "Hello Plastic Engine"}`),
	}

	if err := worker.Submit(context.Background(), document.WorkItem{Command: cmd}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Wait for forward index to be written (async worker)
	requireEventually(t, 500*time.Millisecond, func() bool {
		_, err := store.Get("fwd:doc-1")
		return err == nil
	})
}

func TestShardWorkerBackpressure(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store, nil)

	registry := &stubShardRegistry{
		shards: map[string]*shards.Shard{
			"shard-1": {
				ID: "shard-1",
				Info: shards.Assignment{
					ID:             "shard-1",
					IndexID:        "idx",
					ShardKey:       "default",
					ShardStrategy:  indexes.ShardStrategyAutomatic,
					Analyzer:       "simple",
					Tokenizer:      "whitespace",
					MappingVersion: 1,
					Fields: []indexes.FieldMapping{
						{Name: "title", Type: indexes.FieldTypeText, Indexed: true},
					},
				},
			},
		},
	}

	resolver := document.NewMetadataResolver(nil)
	provider := document.NewAssignmentProvider(registry, resolver)
	planner := document.NewFieldPlanner(document.TokenizerFactory{}, document.AnalyzerFactory{})

	worker := document.NewShardWorker(
		"shard-1",
		registry.shards["shard-1"],
		document.ShardWorkerConfig{MaxWorkers: 1, QueueCapacity: 1},
		planner,
		writer,
		provider,
		logger.DefaultLogger(),
	)
	defer worker.Close()

	ctx := context.Background()
	if err := worker.Submit(ctx, document.WorkItem{Command: document.Command{DocumentID: "doc-1", RawPayload: []byte(`{"title": "hello"}`)}}); err != nil {
		t.Fatalf("Submit first: %v", err)
	}

	if err := worker.Submit(ctx, document.WorkItem{Command: document.Command{DocumentID: "doc-2", RawPayload: []byte(`{"title": "world"}`)}}); err == nil {
		t.Fatalf("expected backpressure error for second item")
	}
}

type stubShardRegistry struct {
	shards map[string]*shards.Shard
}

func (s *stubShardRegistry) GetShard(id string) (*shards.Shard, bool) {
	sh, ok := s.shards[id]
	return sh, ok
}
