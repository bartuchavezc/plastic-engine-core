package document_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/adapters/storage/pebble"
)

func TestIndexWriterPersistsForwardAndInverted(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store)

	req := document.DocumentWriteRequest{
		DocumentID: "doc-1",
		Fields: []document.FieldTerms{
			{
				Field: indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{
					{Term: "hello", Position: 1},
					{Term: "world", Position: 2},
				},
			},
			{
				Field: indexes.FieldMapping{Name: "order_id", Type: indexes.FieldTypeKeyword},
				Tokens: []document.Token{
					{Term: "12345"},
				},
			},
		},
	}

	if err := writer.Index(context.Background(), req); err != nil {
		t.Fatalf("Index: %v", err)
	}

	if _, err := store.Get("inv:title:hello:doc-1"); err != nil {
		t.Fatalf("expected inverted key title/hello, got err %v", err)
	}
	if _, err := store.Get("inv:order_id:12345:doc-1"); err != nil {
		t.Fatalf("expected inverted key order_id/12345, got err %v", err)
	}

	if _, err := store.Get("fwd:doc-1"); err != nil {
		t.Fatalf("expected forward key, got err %v", err)
	}
}

func TestIndexWriterAppliesDiffOnReindex(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store)
	ctx := context.Background()

	initial := document.DocumentWriteRequest{
		DocumentID: "doc-1",
		Fields: []document.FieldTerms{
			{
				Field: indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{
					{Term: "hello", Position: 1},
					{Term: "world", Position: 2},
				},
			},
		},
	}
	if err := writer.Index(ctx, initial); err != nil {
		t.Fatalf("Index initial: %v", err)
	}

	update := document.DocumentWriteRequest{
		DocumentID: "doc-1",
		Fields: []document.FieldTerms{
			{
				Field: indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{
					{Term: "hello", Position: 1},
					{Term: "plastic", Position: 2},
				},
			},
		},
	}
	if err := writer.Index(ctx, update); err != nil {
		t.Fatalf("Index update: %v", err)
	}

	if _, err := store.Get("inv:title:hello:doc-1"); err != nil {
		t.Fatalf("expected hello term after update: %v", err)
	}

	if _, err := store.Get("inv:title:plastic:doc-1"); err != nil {
		t.Fatalf("expected plastic term after update: %v", err)
	}

	if _, err := store.Get("inv:title:world:doc-1"); err == nil {
		t.Fatalf("expected world term removed")
	} else if !pebble.IsNotFound(err) {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func newTestPebbleStore(t *testing.T) *pebble.PebbleStore {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "shard")

	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close PebbleStore: %v", err)
		}
		_ = os.RemoveAll(dir)
	})

	return store
}
