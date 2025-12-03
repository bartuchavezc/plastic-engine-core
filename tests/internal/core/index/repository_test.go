package index_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"plastic-engine-core/internal/core/cluster/coordinator"
	coreindex "plastic-engine-core/internal/core/index"
)

func TestRepositoryCreateAndGetIndex(t *testing.T) {
	t.Parallel()

	repo := newTestRepository(t)

	req := coreindex.CreateIndexRequest{
		ID:               "idx-orders",
		Name:             "orders",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []coreindex.FieldMapping{
			{
				Name:     "order_id",
				Type:     coreindex.FieldTypeKeyword,
				Stored:   true,
				Required: true,
				Indexed:  true,
			},
			{
				Name:      "description",
				Type:      coreindex.FieldTypeText,
				Analyzer:  "simple",
				Tokenizer: "whitespace",
				Indexed:   true,
			},
			{
				Name:     "created_at",
				Type:     coreindex.FieldTypeDate,
				Required: true,
				Indexed:  true,
			},
		},
		ShardConfig: coreindex.ShardConfig{
			Strategy: coreindex.ShardStrategyDate,
			Date: &coreindex.DateShardConfig{
				Field:       "created_at",
				Granularity: coreindex.DateGranularityMonth,
			},
		},
	}

	ctx := context.Background()
	resp, err := repo.CreateIndex(ctx, req)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	if resp.Definition.ID != req.ID {
		t.Fatalf("definition ID = %q, want %q", resp.Definition.ID, req.ID)
	}

	stored, err := repo.GetIndex(ctx, req.ID)
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}

	if stored.Name != req.Name {
		t.Fatalf("stored.Name = %q, want %q", stored.Name, req.Name)
	}
	if stored.ShardConfig.Strategy != req.ShardConfig.Strategy {
		t.Fatalf("stored shard strategy = %q, want %q", stored.ShardConfig.Strategy, req.ShardConfig.Strategy)
	}
	if stored.ShardConfig.Date == nil {
		t.Fatalf("stored date shard config is nil")
	}
	if stored.ShardConfig.Date.Field != req.ShardConfig.Date.Field {
		t.Fatalf("date shard field = %q, want %q", stored.ShardConfig.Date.Field, req.ShardConfig.Date.Field)
	}
	if stored.ShardConfig.Date.Granularity != req.ShardConfig.Date.Granularity {
		t.Fatalf("date granularity = %q, want %q", stored.ShardConfig.Date.Granularity, req.ShardConfig.Date.Granularity)
	}
	if len(stored.FieldMappings) != len(req.FieldMappings) {
		t.Fatalf("field mappings len = %d, want %d", len(stored.FieldMappings), len(req.FieldMappings))
	}

	wantMappings := make(map[string]coreindex.FieldMapping)
	for _, fm := range req.FieldMappings {
		wantMappings[fm.Name] = fm
	}

	for _, fm := range stored.FieldMappings {
		want, ok := wantMappings[fm.Name]
		if !ok {
			t.Fatalf("unexpected field mapping %q", fm.Name)
		}
		if fm.Type != want.Type {
			t.Fatalf("field %q type = %q, want %q", fm.Name, fm.Type, want.Type)
		}
		if fm.Stored != want.Stored {
			t.Fatalf("field %q stored = %v, want %v", fm.Name, fm.Stored, want.Stored)
		}
		if fm.Required != want.Required {
			t.Fatalf("field %q required = %v, want %v", fm.Name, fm.Required, want.Required)
		}
		if fm.Indexed != want.Indexed {
			t.Fatalf("field %q indexed = %v, want %v", fm.Name, fm.Indexed, want.Indexed)
		}
	}

}

func TestRepositoryCreateIndexDuplicateName(t *testing.T) {
	t.Parallel()

	repo := newTestRepository(t)

	req := coreindex.CreateIndexRequest{
		ID:               "idx-products",
		Name:             "products",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []coreindex.FieldMapping{
			{
				Name:    "sku",
				Type:    coreindex.FieldTypeKeyword,
				Stored:  true,
				Indexed: true,
			},
		},
		ShardConfig: coreindex.ShardConfig{
			Strategy: coreindex.ShardStrategyAutomatic,
			Automatic: &coreindex.AutomaticShardConfig{
				ShardCount: 1,
			},
		},
	}

	ctx := context.Background()
	if _, err := repo.CreateIndex(ctx, req); err != nil {
		t.Fatalf("CreateIndex first call: %v", err)
	}

	reqDuplicate := req
	reqDuplicate.ID = "idx-products-2"

	if _, err := repo.CreateIndex(ctx, reqDuplicate); !errors.Is(err, coreindex.ErrIndexNameExists) {
		t.Fatalf("want ErrIndexNameExists, got %v", err)
	}
}

func newTestRepository(t *testing.T) *coreindex.Repository {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "coordinator.db")
	db, err := coordinator.OpenMetadataDB(dbPath)
	if err != nil {
		t.Fatalf("OpenMetadataDB: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return coreindex.NewRepository(db)
}
