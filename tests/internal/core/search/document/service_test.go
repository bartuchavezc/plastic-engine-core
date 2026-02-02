package document_test

import (
	"context"
	"errors"
	"testing"
	"time"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/core/search/segment"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

func TestServiceIndexPersistsDocument(t *testing.T) {
	t.Parallel()

	service, segMgr := newTestService(t)

	cmd := document.Command{
		IndexID:    "idx",
		ShardID:    "shard-1",
		DocumentID: "doc-1",
		RawPayload: []byte(`{"title": "Hello Plastic Engine"}`),
	}

	if err := service.Index(context.Background(), cmd); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Wait for document to be indexed (async worker)
	requireEventually(t, 500*time.Millisecond, func() bool {
		hits, err := segMgr.Search(context.Background(), "title", "hello")
		return err == nil && len(hits) > 0
	})

	// Verify we can search for the term
	hits, err := segMgr.Search(context.Background(), "title", "hello")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatalf("expected at least 1 hit")
	}
	if hits[0].DocID != "doc-1" {
		t.Fatalf("expected doc-1, got %s", hits[0].DocID)
	}
}

func TestServiceIndexValidation(t *testing.T) {
	t.Parallel()

	service, _ := newTestService(t)

	invalid := []document.Command{
		{ShardID: "shard", DocumentID: "doc"},
		{IndexID: "idx", DocumentID: "doc"},
		{IndexID: "idx", ShardID: "shard"},
	}

	for _, cmd := range invalid {
		if err := service.Index(context.Background(), cmd); err == nil {
			t.Fatalf("expected error for cmd %+v", cmd)
		}
	}
}

func TestServiceIndexMissingShard(t *testing.T) {
	t.Parallel()

	service, _ := newTestService(t)

	err := service.Index(context.Background(), document.Command{
		IndexID:    "idx",
		ShardID:    "missing",
		DocumentID: "doc",
	})
	if err == nil {
		t.Fatalf("expected error for missing shard")
	}
	if !errors.Is(err, document.ErrShardNotLoaded) {
		t.Fatalf("expected ErrShardNotLoaded, got %v", err)
	}
}

func newTestService(t *testing.T) (*document.Service, *segment.Manager) {
	t.Helper()

	dir := t.TempDir()
	manager := shards.NewManager(dir)

	assignment := shards.Assignment{
		ID:             "shard-1",
		IndexID:        "idx",
		ShardKey:       "default",
		ShardStrategy:  indexes.ShardStrategyAutomatic,
		Analyzer:       "simple",
		Tokenizer:      "whitespace",
		MappingVersion: 1,
		Fields: []indexes.FieldMapping{
			{Name: "title", Type: indexes.FieldTypeText, Analyzer: "simple", Tokenizer: "whitespace", Indexed: true},
		},
	}

	if err := manager.Sync([]shards.Assignment{assignment}); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	sh, ok := manager.GetShard("shard-1")
	if !ok {
		t.Fatalf("shard not loaded")
	}

	resolver := document.NewMetadataResolver(nil)
	provider := document.NewAssignmentProvider(manager, resolver)
	planner := document.NewFieldPlanner(document.TokenizerFactory{}, document.AnalyzerFactory{})

	writerFactory := func(s *shards.Shard) document.DocumentIndexWriter {
		return document.NewSegmentIndexWriter(s.Segments, nil)
	}

	cfg := document.ShardWorkerConfig{
		MaxWorkers:    1,
		QueueCapacity: 16,
	}

	service := document.NewService(manager, provider, planner, writerFactory, cfg, logger.DefaultLogger())
	t.Cleanup(service.Close)
	t.Cleanup(func() { manager.Close() })

	return service, sh.Segments
}
