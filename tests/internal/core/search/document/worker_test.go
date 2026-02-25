package document_test

import (
	"context"
	"testing"

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
		document.ShardWorkerConfig{MaxWorkers: 1},
		planner,
		writer,
		provider,
		logger.DefaultLogger(),
	)
	defer worker.Close()

	cmd := document.Command{
		IndexID:    "idx",
		ShardID:    "shard-1",
		DocumentID: "doc-1",
		RawPayload: []byte(`{"title": "Hello Plastic Engine"}`),
	}

	errs := worker.Process(context.Background(), []document.Command{cmd})
	if len(errs) > 0 && errs[0] != nil {
		t.Fatalf("Process: %v", errs[0])
	}

	// Document must be immediately searchable (synchronous indexing).
	hits, err := segMgr.Search(context.Background(), "title", "hello")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatalf("expected hits for 'hello', got none")
	}
}

func TestShardWorkerBulkProcess(t *testing.T) {
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
		document.ShardWorkerConfig{MaxWorkers: 2},
		planner,
		writer,
		provider,
		logger.DefaultLogger(),
	)
	defer worker.Close()

	cmds := []document.Command{
		{IndexID: "idx", ShardID: "shard-1", DocumentID: "doc-1", RawPayload: []byte(`{"title": "alpha beta"}`)},
		{IndexID: "idx", ShardID: "shard-1", DocumentID: "doc-2", RawPayload: []byte(`{"title": "gamma delta"}`)},
		{IndexID: "idx", ShardID: "shard-1", DocumentID: "doc-3", RawPayload: []byte(`{"title": "epsilon zeta"}`)},
	}

	errs := worker.Process(context.Background(), cmds)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Process cmd[%d]: %v", i, err)
		}
	}

	// All documents must be immediately searchable.
	for _, term := range []string{"alpha", "gamma", "epsilon"} {
		hits, err := segMgr.Search(context.Background(), "title", term)
		if err != nil {
			t.Fatalf("Search %q: %v", term, err)
		}
		if len(hits) == 0 {
			t.Fatalf("expected hits for %q, got none", term)
		}
	}
}

type stubShardRegistry struct {
	shards map[string]*shards.Shard
}

func (s *stubShardRegistry) GetShard(id string) (*shards.Shard, bool) {
	sh, ok := s.shards[id]
	return sh, ok
}
