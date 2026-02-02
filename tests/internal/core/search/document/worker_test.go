package document_test

import (
	"context"
	"testing"
	"time"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/core/search/segment"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

func TestShardWorkerProcessesDocument(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	segMgr, err := segment.NewManager(segment.Config{
		FlushThreshold:      1000,
		MaxSegmentsPerLevel: 5,
		LevelSizeMultiplier: 10,
		DataDir:             dir,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer segMgr.Close()

	writer := document.NewSegmentIndexWriter(segMgr, nil)

	registry := &stubShardRegistry{
		shards: map[string]*shards.Shard{
			"shard-1": {
				ID:       "shard-1",
				Segments: segMgr,
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
		document.ShardWorkerConfig{MaxWorkers: 1, QueueCapacity: 16},
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

	// Wait for document to be indexed
	requireEventually(t, 500*time.Millisecond, func() bool {
		hits, err := segMgr.Search(context.Background(), "title", "hello")
		return err == nil && len(hits) > 0
	})
}

func TestShardWorkerBackpressure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	segMgr, err := segment.NewManager(segment.Config{
		FlushThreshold:      1000,
		MaxSegmentsPerLevel: 5,
		LevelSizeMultiplier: 10,
		DataDir:             dir,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer segMgr.Close()

	writer := document.NewSegmentIndexWriter(segMgr, nil)

	registry := &stubShardRegistry{
		shards: map[string]*shards.Shard{
			"shard-1": {
				ID:       "shard-1",
				Segments: segMgr,
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

	// Use very small queue to trigger backpressure
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

	// Fill the queue quickly with multiple items
	// Since queue capacity is 1, we should hit backpressure quickly
	var backpressureHit bool
	for i := 0; i < 10; i++ {
		err := worker.Submit(ctx, document.WorkItem{
			Command: document.Command{
				DocumentID: "doc-" + string(rune('0'+i)),
				RawPayload: []byte(`{"title": "hello world"}`),
			},
		})
		if err != nil {
			backpressureHit = true
			break
		}
	}

	if !backpressureHit {
		t.Fatalf("expected backpressure error when queue is full")
	}
}

type stubShardRegistry struct {
	shards map[string]*shards.Shard
}

func (s *stubShardRegistry) GetShard(id string) (*shards.Shard, bool) {
	sh, ok := s.shards[id]
	return sh, ok
}
