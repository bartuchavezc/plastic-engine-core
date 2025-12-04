package hydration_test

import (
	"context"
	"testing"

	"plastic-engine-core/internal/core/search/hydration"
	"plastic-engine-core/internal/core/search/hydration/connector"
	"plastic-engine-core/internal/pkg/logger"
)

func TestHydratorWithInternalConnector(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	conn := connector.NewInternalConnector(store)
	ctx := context.Background()

	// Store documents
	conn.Store(ctx, "doc-1", map[string]any{"title": "Hello World"})
	conn.Store(ctx, "doc-2", map[string]any{"title": "Goodbye World"})

	// Create hydrator with factory that returns our connector
	factory := func(connType string, settings map[string]string) (hydration.Connector, error) {
		return conn, nil
	}

	hydrator := hydration.NewHydrator(factory, nil, logger.DefaultLogger())
	defer hydrator.Close()

	// Hydrate hits
	req := hydration.HydrateRequest{
		IndexID: "test-index",
		Config: hydration.Config{
			Enabled:   true,
			Connector: "internal",
			KeyField:  "_id", // Use doc_id as key
		},
		Hits: []hydration.Hit{
			{DocID: "doc-1", ShardID: "shard-1", Score: 4.5},
			{DocID: "doc-2", ShardID: "shard-1", Score: 3.2},
			{DocID: "doc-3", ShardID: "shard-1", Score: 2.1}, // Missing
		},
	}

	results, err := hydrator.Hydrate(ctx, req)
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Check first hit
	if !results[0].Found {
		t.Error("expected doc-1 to be found")
	}
	if results[0].Source["title"] != "Hello World" {
		t.Errorf("expected title=Hello World, got %v", results[0].Source["title"])
	}
	if results[0].Score != 4.5 {
		t.Errorf("expected score=4.5, got %f", results[0].Score)
	}

	// Check second hit
	if !results[1].Found {
		t.Error("expected doc-2 to be found")
	}

	// Check missing doc
	if results[2].Found {
		t.Error("expected doc-3 to NOT be found")
	}
	if results[2].DocID != "doc-3" {
		t.Errorf("expected DocID=doc-3, got %s", results[2].DocID)
	}
}

func TestHydratorDisabled(t *testing.T) {
	t.Parallel()

	hydrator := hydration.NewHydrator(nil, nil, nil)
	defer hydrator.Close()

	ctx := context.Background()
	req := hydration.HydrateRequest{
		IndexID: "test-index",
		Config: hydration.Config{
			Enabled: false,
		},
		Hits: []hydration.Hit{
			{DocID: "doc-1", ShardID: "shard-1", Score: 4.5},
		},
	}

	results, err := hydrator.Hydrate(ctx, req)
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Source should be nil when disabled
	if results[0].Source != nil {
		t.Error("expected nil source when hydration disabled")
	}
	if results[0].Found {
		t.Error("expected Found=false when hydration disabled")
	}
}

func TestHydratorWithConnector(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	conn := connector.NewInternalConnector(store)
	ctx := context.Background()

	// Store documents
	conn.Store(ctx, "doc-1", map[string]any{"content": "Test content"})

	hydrator := hydration.NewHydrator(nil, nil, nil)
	defer hydrator.Close()

	hits := []hydration.Hit{
		{DocID: "doc-1", ShardID: "shard-1", Score: 5.0},
	}

	results, err := hydrator.HydrateWithConnector(ctx, conn, "_id", hits)
	if err != nil {
		t.Fatalf("HydrateWithConnector: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !results[0].Found {
		t.Error("expected doc-1 to be found")
	}
	if results[0].Source["content"] != "Test content" {
		t.Errorf("expected content=Test content, got %v", results[0].Source["content"])
	}
}

func TestHydrationConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  hydration.Config
		wantErr bool
	}{
		{
			name:    "disabled is always valid",
			config:  hydration.Config{Enabled: false},
			wantErr: false,
		},
		{
			name: "valid internal config",
			config: hydration.Config{
				Enabled:   true,
				Connector: "internal",
				KeyField:  "_id",
			},
			wantErr: false,
		},
		{
			name: "missing connector",
			config: hydration.Config{
				Enabled:  true,
				KeyField: "_id",
			},
			wantErr: true,
		},
		{
			name: "missing key field",
			config: hydration.Config{
				Enabled:   true,
				Connector: "http",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfigs(t *testing.T) {
	t.Parallel()

	// Default should be disabled
	def := hydration.DefaultConfig()
	if def.Enabled {
		t.Error("default config should be disabled")
	}

	// Internal should be enabled
	internal := hydration.InternalConfig()
	if !internal.Enabled {
		t.Error("internal config should be enabled")
	}
	if internal.Connector != "internal" {
		t.Errorf("internal connector should be 'internal', got %s", internal.Connector)
	}
}

