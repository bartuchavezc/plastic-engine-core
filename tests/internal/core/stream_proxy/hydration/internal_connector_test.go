package hydration_test

import (
	"context"
	"testing"

	"plastic-engine-core/internal/core/stream_proxy/hydration"
	"plastic-engine-core/internal/core/stream_proxy/hydration/connector"
)

func TestInternalConnectorStoreAndFetch(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	conn := connector.NewInternalConnector(store)
	ctx := context.Background()

	// Store a document
	source := map[string]any{
		"title": "Hello World",
		"body":  "This is a test document",
	}

	if err := conn.Store(ctx, "doc-1", source); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Fetch it back
	doc, err := conn.Fetch(ctx, "doc-1")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if !doc.Found {
		t.Error("expected Found=true")
	}
	if doc.ID != "doc-1" {
		t.Errorf("expected ID=doc-1, got %s", doc.ID)
	}
	if doc.Source["title"] != "Hello World" {
		t.Errorf("expected title=Hello World, got %v", doc.Source["title"])
	}
}

func TestInternalConnectorFetchNotFound(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	conn := connector.NewInternalConnector(store)
	ctx := context.Background()

	doc, err := conn.Fetch(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if doc.Found {
		t.Error("expected Found=false for nonexistent doc")
	}
}

func TestInternalConnectorFetchBatch(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	conn := connector.NewInternalConnector(store)
	ctx := context.Background()

	// Store some docs
	conn.Store(ctx, "doc-1", map[string]any{"title": "Doc 1"})
	conn.Store(ctx, "doc-2", map[string]any{"title": "Doc 2"})

	// Fetch batch including a missing doc
	docs, err := conn.FetchBatch(ctx, []string{"doc-1", "doc-2", "doc-3"})
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}

	if len(docs) != 3 {
		t.Errorf("expected 3 results, got %d", len(docs))
	}

	if !docs["doc-1"].Found {
		t.Error("expected doc-1 to be found")
	}
	if !docs["doc-2"].Found {
		t.Error("expected doc-2 to be found")
	}
	if docs["doc-3"].Found {
		t.Error("expected doc-3 to NOT be found")
	}
}

func TestInternalConnectorDelete(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	conn := connector.NewInternalConnector(store)
	ctx := context.Background()

	// Store and verify
	conn.Store(ctx, "doc-1", map[string]any{"title": "Test"})
	doc, _ := conn.Fetch(ctx, "doc-1")
	if !doc.Found {
		t.Fatal("doc should exist before delete")
	}

	// Delete
	if err := conn.Delete(ctx, "doc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	doc, _ = conn.Fetch(ctx, "doc-1")
	if doc.Found {
		t.Error("doc should not exist after delete")
	}
}

func TestInternalConnectorName(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	conn := connector.NewInternalConnector(store)

	if conn.Name() != "internal" {
		t.Errorf("expected name=internal, got %s", conn.Name())
	}
}

// memoryStore is a simple in-memory store for testing.
type memoryStore struct {
	data map[string]string
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: make(map[string]string)}
}

func (s *memoryStore) Get(key string) (string, error) {
	v, ok := s.data[key]
	if !ok {
		return "", notFoundError{}
	}
	return v, nil
}

func (s *memoryStore) Set(key, value string) error {
	s.data[key] = value
	return nil
}

func (s *memoryStore) Delete(key string) error {
	delete(s.data, key)
	return nil
}

type notFoundError struct{}

func (e notFoundError) Error() string {
	return "pebble: not found"
}

// Verify interface compliance
var _ hydration.Connector = (*connector.InternalConnector)(nil)

