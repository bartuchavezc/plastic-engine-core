package indexer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/search/indexer"
	searchshard "plastic-engine-core/internal/core/search/shard"
	"plastic-engine-core/internal/core/storage/pebble"
	"plastic-engine-core/internal/helpers"
)

func TestServiceIndexPersistsDocument(t *testing.T) {
	t.Parallel()

	service, store := newTestService(t)

	cmd := indexer.Command{
		IndexID:    "idx",
		ShardID:    "shard-1",
		DocumentID: "doc-1",
		Payload: map[string]any{
			"title": "Hello Plastic Engine",
		},
	}

	if err := service.Index(context.Background(), cmd); err != nil {
		t.Fatalf("Index: %v", err)
	}

	requireEventually(t, 500*time.Millisecond, func() bool {
		_, err := store.Get("inv:title:hello:doc-1")
		return err == nil
	})
}

func TestServiceIndexValidation(t *testing.T) {
	t.Parallel()

	service, _ := newTestService(t)

	invalid := []indexer.Command{
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

	err := service.Index(context.Background(), indexer.Command{
		IndexID:    "idx",
		ShardID:    "missing",
		DocumentID: "doc",
	})
	if err == nil {
		t.Fatalf("expected error for missing shard")
	}
	if !errors.Is(err, indexer.ErrShardNotLoaded) {
		t.Fatalf("expected ErrShardNotLoaded, got %v", err)
	}
}

func newTestService(t *testing.T) (*indexer.Service, *pebble.PebbleStore) {
	t.Helper()

	dir := t.TempDir()
	manager := searchshard.NewManager(dir)

	assignment := searchshard.Assignment{
		ID:             "shard-1",
		IndexID:        "idx",
		ShardKey:       "default",
		ShardStrategy:  coreindex.ShardStrategyAutomatic,
		Analyzer:       "simple",
		Tokenizer:      "whitespace",
		MappingVersion: 1,
		Fields: []coreindex.FieldMapping{
			{Name: "title", Type: coreindex.FieldTypeText, Analyzer: "simple", Tokenizer: "whitespace", Indexed: true},
		},
	}

	if err := manager.Sync([]searchshard.Assignment{assignment}); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	sh, ok := manager.GetShard("shard-1")
	if !ok {
		t.Fatalf("shard not loaded")
	}

	resolver := indexer.NewMetadataResolver(nil)
	provider := indexer.NewAssignmentProvider(manager, resolver)
	planner := indexer.NewFieldPlanner(indexer.TokenizerFactory{}, indexer.AnalyzerFactory{})

	writerFactory := func(s *searchshard.Shard) *indexer.IndexWriter {
		return indexer.NewIndexWriter(s.Store)
	}

	cfg := indexer.ShardWorkerConfig{
		MaxWorkers:    1,
		QueueCapacity: 16,
	}

	service := indexer.NewService(manager, provider, planner, writerFactory, cfg, helpers.DefaultLogger())
	t.Cleanup(service.Close)

	return service, sh.Store
}
