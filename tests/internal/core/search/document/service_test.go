package document_test

import (
	"context"
	"errors"
	"testing"
	"time"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/adapters/storage/pebble"
	"plastic-engine-core/internal/pkg/logger"
)

func TestServiceIndexPersistsDocument(t *testing.T) {
	t.Parallel()

	service, store := newTestService(t)

	cmd := document.Command{
		IndexID:    "idx",
		ShardID:    "shard-1",
		DocumentID: "doc-1",
		RawPayload: []byte(`{"title": "Hello Plastic Engine"}`),
	}

	if err := service.Index(context.Background(), cmd); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Wait for forward index to be written (async worker)
	requireEventually(t, 500*time.Millisecond, func() bool {
		_, err := store.Get("fwd:doc-1")
		return err == nil
	})

	// Verify term registry was created
	_, err := store.Get(pebble.TermRegistryKey("title", "hello"))
	if err != nil {
		t.Fatalf("expected term registry entry, got %v", err)
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

func newTestService(t *testing.T) (*document.Service, *pebble.PebbleStore) {
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

	writerFactory := func(s *shards.Shard) *document.IndexWriter {
		return document.NewIndexWriter(s.Store, nil)
	}

	cfg := document.ShardWorkerConfig{
		MaxWorkers:    1,
		QueueCapacity: 16,
	}

	service := document.NewService(manager, provider, planner, writerFactory, cfg, logger.DefaultLogger())
	t.Cleanup(service.Close)

	return service, sh.Store
}
