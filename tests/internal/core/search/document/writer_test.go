package document_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/adapters/storage/pebble"
)

func TestIndexWriterPersistsForwardAndInverted(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store, nil)

	req := document.DocumentWriteRequest{
		DocumentID:  "doc-1",
		NgramConfig: document.NgramConfigDisabled(), // Disable for this test
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

	// Forward index should exist
	if _, err := store.Get("fwd:doc-1"); err != nil {
		t.Fatalf("expected forward key, got err %v", err)
	}

	// Term registry entries should exist
	if _, err := store.Get(pebble.TermRegistryKey("title", "hello")); err != nil {
		t.Fatalf("expected term registry entry for title:hello, got err %v", err)
	}
	if _, err := store.Get(pebble.TermRegistryKey("order_id", "12345")); err != nil {
		t.Fatalf("expected term registry entry for order_id:12345, got err %v", err)
	}

	// Verify inverted index using prefix scan (keys now use term_id)
	helloTermID := pebble.GenerateTermID("title", "hello")
	invKeys, err := store.PrefixScanKeys(pebble.InvertedPrefix(helloTermID))
	if err != nil {
		t.Fatalf("prefix scan: %v", err)
	}
	if len(invKeys) == 0 {
		t.Fatalf("expected inverted entry for hello term")
	}
	if !strings.HasSuffix(invKeys[0], ":doc-1") {
		t.Fatalf("expected doc-1 in inverted key, got %s", invKeys[0])
	}
}

func TestIndexWriterAppliesDiffOnReindex(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store, nil)
	ctx := context.Background()

	initial := document.DocumentWriteRequest{
		DocumentID:  "doc-1",
		NgramConfig: document.NgramConfigDisabled(),
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
		DocumentID:  "doc-1",
		NgramConfig: document.NgramConfigDisabled(),
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

	// hello term should still exist
	helloTermID := pebble.GenerateTermID("title", "hello")
	helloKeys, err := store.PrefixScanKeys(pebble.InvertedPrefix(helloTermID))
	if err != nil {
		t.Fatalf("scan hello: %v", err)
	}
	if len(helloKeys) == 0 {
		t.Fatalf("expected hello term after update")
	}

	// plastic term should be added
	plasticTermID := pebble.GenerateTermID("title", "plastic")
	plasticKeys, err := store.PrefixScanKeys(pebble.InvertedPrefix(plasticTermID))
	if err != nil {
		t.Fatalf("scan plastic: %v", err)
	}
	if len(plasticKeys) == 0 {
		t.Fatalf("expected plastic term after update")
	}

	// world term should be removed
	worldTermID := pebble.GenerateTermID("title", "world")
	worldKeys, err := store.PrefixScanKeys(pebble.InvertedPrefix(worldTermID))
	if err != nil {
		t.Fatalf("scan world: %v", err)
	}
	if len(worldKeys) != 0 {
		t.Fatalf("expected world term removed, found %d entries", len(worldKeys))
	}
}

func TestIndexWriterEdgeNgrams(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store, nil)

	req := document.DocumentWriteRequest{
		DocumentID: "doc-1",
		NgramConfig: document.NgramConfig{
			Enabled:   true,
			MinLength: 2,
			MaxLength: 5,
		},
		Fields: []document.FieldTerms{
			{
				Field: indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{
					{Term: "hello", Position: 1},
				},
			},
		},
	}

	if err := writer.Index(context.Background(), req); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Check n-grams were created: he, hel, hell, hello
	expectedPrefixes := []string{"he", "hel", "hell", "hello"}
	for _, prefix := range expectedPrefixes {
		keys, err := store.PrefixScanKeys(pebble.NgramPrefix("title", prefix))
		if err != nil {
			t.Fatalf("scan ngram %s: %v", prefix, err)
		}
		if len(keys) == 0 {
			t.Errorf("expected ngram entry for prefix %q", prefix)
		}
	}
}

func TestIndexWriterMetadataUpdate(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	writer := document.NewIndexWriter(store, nil)
	ctx := context.Background()

	// Index first document
	req1 := document.DocumentWriteRequest{
		DocumentID:  "doc-1",
		NgramConfig: document.NgramConfigDisabled(),
		Fields: []document.FieldTerms{
			{
				Field:  indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{{Term: "hello"}, {Term: "world"}},
			},
		},
	}
	if err := writer.Index(ctx, req1); err != nil {
		t.Fatalf("Index doc-1: %v", err)
	}

	// Check doc count
	docCount, err := store.GetInt64(pebble.DocCountKey())
	if err != nil {
		t.Fatalf("get doc count: %v", err)
	}
	if docCount != 1 {
		t.Errorf("expected doc_count=1, got %d", docCount)
	}

	// Index second document
	req2 := document.DocumentWriteRequest{
		DocumentID:  "doc-2",
		NgramConfig: document.NgramConfigDisabled(),
		Fields: []document.FieldTerms{
			{
				Field:  indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{{Term: "foo"}, {Term: "bar"}, {Term: "baz"}},
			},
		},
	}
	if err := writer.Index(ctx, req2); err != nil {
		t.Fatalf("Index doc-2: %v", err)
	}

	docCount, err = store.GetInt64(pebble.DocCountKey())
	if err != nil {
		t.Fatalf("get doc count after doc-2: %v", err)
	}
	if docCount != 2 {
		t.Errorf("expected doc_count=2, got %d", docCount)
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
