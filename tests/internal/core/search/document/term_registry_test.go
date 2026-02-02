package document_test

import (
	"context"
	"testing"

	"plastic-engine-core/internal/adapters/storage/pebble"
	"plastic-engine-core/internal/core/search/document"
)

func TestTermRegistryGetOrCreate(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	registry := document.NewTermRegistry(store)
	ctx := context.Background()

	// First call should create
	entry1, err := registry.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	if entry1.TermID == "" {
		t.Error("expected non-empty TermID")
	}

	// DF is stored separately, check via GetDF
	df, err := registry.GetDF(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetDF: %v", err)
	}
	if df != 0 {
		t.Errorf("expected DF=0 for new term, got %d", df)
	}

	if entry1.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}

	// Second call should return same entry
	entry2, err := registry.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate second: %v", err)
	}

	if entry1.TermID != entry2.TermID {
		t.Errorf("TermID mismatch: %q != %q", entry1.TermID, entry2.TermID)
	}
}

func TestTermRegistryGet(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	registry := document.NewTermRegistry(store)
	ctx := context.Background()

	// Get non-existent term
	_, found, err := registry.Get(ctx, "title", "nonexistent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Error("expected found=false for non-existent term")
	}

	// Create term
	created, err := registry.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Get existing term
	entry, found, err := registry.Get(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Error("expected found=true for existing term")
	}
	if entry.TermID != created.TermID {
		t.Errorf("TermID mismatch: %q != %q", entry.TermID, created.TermID)
	}
}

func TestTermRegistryIncrementDF(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	registry := document.NewTermRegistry(store)
	ctx := context.Background()

	// Create term
	_, err := registry.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Increment DF
	if err := registry.IncrementDF(ctx, "title", "hello"); err != nil {
		t.Fatalf("IncrementDF: %v", err)
	}

	// Verify DF increased via GetDF
	df, err := registry.GetDF(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetDF: %v", err)
	}
	if df != 1 {
		t.Errorf("expected DF=1, got %d", df)
	}

	// Increment again
	if err := registry.IncrementDF(ctx, "title", "hello"); err != nil {
		t.Fatalf("IncrementDF second: %v", err)
	}

	df, _ = registry.GetDF(ctx, "title", "hello")
	if df != 2 {
		t.Errorf("expected DF=2, got %d", df)
	}
}

func TestTermRegistryDecrementDF(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	registry := document.NewTermRegistry(store)
	ctx := context.Background()

	// Create term and increment
	_, err := registry.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	registry.IncrementDF(ctx, "title", "hello")
	registry.IncrementDF(ctx, "title", "hello")

	// Decrement
	if err := registry.DecrementDF(ctx, "title", "hello"); err != nil {
		t.Fatalf("DecrementDF: %v", err)
	}

	df, _ := registry.GetDF(ctx, "title", "hello")
	if df != 1 {
		t.Errorf("expected DF=1 after decrement, got %d", df)
	}

	// Decrement below zero - with merge operator, this will go negative
	// The application layer should handle negative DF as 0
	registry.DecrementDF(ctx, "title", "hello")
	registry.DecrementDF(ctx, "title", "hello")

	df, _ = registry.GetDF(ctx, "title", "hello")
	// Note: With merge operator, DF can go negative. In practice, this should be treated as 0.
	if df > 0 {
		t.Errorf("expected DF<=0 after multiple decrements, got %d", df)
	}
}

func TestTermRegistryDifferentFieldsSameTerm(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	registry := document.NewTermRegistry(store)
	ctx := context.Background()

	// Same term in different fields should have different term_ids
	entry1, _ := registry.GetOrCreate(ctx, "title", "hello")
	entry2, _ := registry.GetOrCreate(ctx, "body", "hello")

	if entry1.TermID == entry2.TermID {
		t.Errorf("same term in different fields should have different IDs: %q == %q",
			entry1.TermID, entry2.TermID)
	}
}

func TestBatchTermRegistry(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	batch := document.NewBatchTermRegistry(store)
	ctx := context.Background()

	// Create multiple terms in batch
	entry1, err := batch.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	entry2, err := batch.GetOrCreate(ctx, "title", "world")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Increment in batch
	batch.IncrementDF("title", "hello")
	batch.IncrementDF("title", "hello")
	batch.IncrementDF("title", "world")

	// Commit
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify persisted
	registry := document.NewTermRegistry(store)

	persisted1, found, _ := registry.Get(ctx, "title", "hello")
	if !found {
		t.Fatal("hello not found after commit")
	}
	if persisted1.TermID != entry1.TermID {
		t.Errorf("TermID mismatch after commit")
	}

	// Verify DF via GetDF
	df1, _ := registry.GetDF(ctx, "title", "hello")
	if df1 != 2 {
		t.Errorf("expected DF=2 for hello, got %d", df1)
	}

	persisted2, found, _ := registry.Get(ctx, "title", "world")
	if !found {
		t.Fatal("world not found after commit")
	}
	if persisted2.TermID != entry2.TermID {
		t.Errorf("TermID mismatch after commit")
	}

	df2, _ := registry.GetDF(ctx, "title", "world")
	if df2 != 1 {
		t.Errorf("expected DF=1 for world, got %d", df2)
	}
}

func TestTermRegistryTermIDDeterministic(t *testing.T) {
	t.Parallel()

	// Term IDs should be deterministic based on field+term
	id1 := pebble.GenerateTermID("title", "hello")
	id2 := pebble.GenerateTermID("title", "hello")

	if id1 != id2 {
		t.Errorf("GenerateTermID not deterministic: %q != %q", id1, id2)
	}
}
