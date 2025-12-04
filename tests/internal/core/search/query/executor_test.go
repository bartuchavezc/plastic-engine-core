package query_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"plastic-engine-core/internal/adapters/storage/pebble"
	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/core/search/query"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
)

func TestExecutorTermQuery(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	indexDocuments(t, store, []testDoc{
		{ID: "doc-1", Title: "hello world"},
		{ID: "doc-2", Title: "hello plastic engine"},
		{ID: "doc-3", Title: "goodbye world"},
	})

	executor := query.NewExecutor("shard-1", store)

	req := query.Request{
		IndexID: "test",
		Query: query.Clause{
			Term: &query.TermQuery{
				Field: "title",
				Value: "hello",
			},
		},
		Limit: 10,
	}
	req.Normalize()

	hits, total, err := executor.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if total != 2 {
		t.Errorf("expected total=2, got %d", total)
	}
	if len(hits) != 2 {
		t.Errorf("expected 2 hits, got %d", len(hits))
	}

	// Verify doc IDs
	docIDs := make(map[string]bool)
	for _, hit := range hits {
		docIDs[hit.DocID] = true
	}
	if !docIDs["doc-1"] || !docIDs["doc-2"] {
		t.Error("expected doc-1 and doc-2 in results")
	}
}

func TestExecutorMatchQuery(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	indexDocuments(t, store, []testDoc{
		{ID: "doc-1", Title: "hello world"},
		{ID: "doc-2", Title: "hello plastic engine"},
		{ID: "doc-3", Title: "goodbye world"},
	})

	executor := query.NewExecutor("shard-1", store)

	req := query.Request{
		IndexID: "test",
		Query: query.Clause{
			Match: &query.MatchQuery{
				Field: "title",
				Value: "hello world",
			},
		},
		Limit: 10,
	}
	req.Normalize()

	hits, _, err := executor.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// doc-1 should score highest (has both terms)
	if len(hits) == 0 {
		t.Fatal("expected at least one hit")
	}
	if hits[0].DocID != "doc-1" {
		t.Errorf("expected doc-1 as top hit, got %s", hits[0].DocID)
	}
}

func TestExecutorPrefixQuery(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	// Use ngrams for prefix search
	indexDocumentsWithNgrams(t, store, []testDoc{
		{ID: "doc-1", Title: "hello"},
		{ID: "doc-2", Title: "help"},
		{ID: "doc-3", Title: "helicopter"},
		{ID: "doc-4", Title: "goodbye"},
	})

	executor := query.NewExecutor("shard-1", store)

	req := query.Request{
		IndexID: "test",
		Query: query.Clause{
			Prefix: &query.PrefixQuery{
				Field: "title",
				Value: "hel",
			},
		},
		Limit: 10,
	}
	req.Normalize()

	hits, total, err := executor.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if total != 3 {
		t.Errorf("expected total=3 (hello, help, helicopter), got %d", total)
	}
	if len(hits) != 3 {
		t.Errorf("expected 3 hits, got %d", len(hits))
	}
}

func TestExecutorWithLimit(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	indexDocuments(t, store, []testDoc{
		{ID: "doc-1", Title: "test"},
		{ID: "doc-2", Title: "test"},
		{ID: "doc-3", Title: "test"},
		{ID: "doc-4", Title: "test"},
		{ID: "doc-5", Title: "test"},
	})

	executor := query.NewExecutor("shard-1", store)

	req := query.Request{
		IndexID: "test",
		Query: query.Clause{
			Term: &query.TermQuery{
				Field: "title",
				Value: "test",
			},
		},
		Limit: 3,
	}
	req.Normalize()

	hits, total, err := executor.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if total != 5 {
		t.Errorf("expected total=5, got %d", total)
	}
	if len(hits) != 3 {
		t.Errorf("expected 3 hits (limit), got %d", len(hits))
	}
}

func TestExecutorNoResults(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	indexDocuments(t, store, []testDoc{
		{ID: "doc-1", Title: "hello world"},
	})

	executor := query.NewExecutor("shard-1", store)

	req := query.Request{
		IndexID: "test",
		Query: query.Clause{
			Term: &query.TermQuery{
				Field: "title",
				Value: "nonexistent",
			},
		},
		Limit: 10,
	}
	req.Normalize()

	hits, total, err := executor.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if total != 0 {
		t.Errorf("expected total=0, got %d", total)
	}
	if len(hits) != 0 {
		t.Errorf("expected 0 hits, got %d", len(hits))
	}
}

func TestExecutorScoresDescending(t *testing.T) {
	t.Parallel()

	store := newTestPebbleStore(t)
	// Create documents with different term frequencies
	indexDocumentsCustom(t, store, []document.DocumentWriteRequest{
		{
			DocumentID: "doc-1",
			Fields: []document.FieldTerms{{
				Field:  indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{{Term: "test", Position: 0}},
			}},
		},
		{
			DocumentID: "doc-2",
			Fields: []document.FieldTerms{{
				Field:  indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: []document.Token{
					{Term: "test", Position: 0},
					{Term: "test", Position: 1},
					{Term: "test", Position: 2},
				},
			}},
		},
	})

	executor := query.NewExecutor("shard-1", store)

	req := query.Request{
		IndexID: "test",
		Query: query.Clause{
			Term: &query.TermQuery{
				Field: "title",
				Value: "test",
			},
		},
		Limit: 10,
	}
	req.Normalize()

	hits, _, err := executor.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(hits) < 2 {
		t.Fatal("expected at least 2 hits")
	}

	// doc-2 has higher TF, should score higher
	if hits[0].DocID != "doc-2" {
		t.Errorf("expected doc-2 (higher TF) as top hit, got %s", hits[0].DocID)
	}

	// Verify descending order
	for i := 1; i < len(hits); i++ {
		if hits[i-1].Score < hits[i].Score {
			t.Errorf("scores not descending: %f < %f", hits[i-1].Score, hits[i].Score)
		}
	}
}

// Test helpers

type testDoc struct {
	ID    string
	Title string
}

func indexDocuments(t *testing.T, store *pebble.PebbleStore, docs []testDoc) {
	t.Helper()
	writer := document.NewIndexWriter(store)
	ctx := context.Background()

	for _, doc := range docs {
		tokens := tokenize(doc.Title)
		req := document.DocumentWriteRequest{
			DocumentID:  doc.ID,
			NgramConfig: document.NgramConfigDisabled(),
			Fields: []document.FieldTerms{{
				Field:  indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: tokens,
			}},
		}
		if err := writer.Index(ctx, req); err != nil {
			t.Fatalf("index %s: %v", doc.ID, err)
		}
	}
}

func indexDocumentsWithNgrams(t *testing.T, store *pebble.PebbleStore, docs []testDoc) {
	t.Helper()
	writer := document.NewIndexWriter(store)
	ctx := context.Background()

	ngramCfg := document.NgramConfig{
		Enabled:   true,
		MinLength: 2,
		MaxLength: 10,
	}

	for _, doc := range docs {
		tokens := tokenize(doc.Title)
		req := document.DocumentWriteRequest{
			DocumentID:  doc.ID,
			NgramConfig: ngramCfg,
			Fields: []document.FieldTerms{{
				Field:  indexes.FieldMapping{Name: "title", Type: indexes.FieldTypeText},
				Tokens: tokens,
			}},
		}
		if err := writer.Index(ctx, req); err != nil {
			t.Fatalf("index %s: %v", doc.ID, err)
		}
	}
}

func indexDocumentsCustom(t *testing.T, store *pebble.PebbleStore, reqs []document.DocumentWriteRequest) {
	t.Helper()
	writer := document.NewIndexWriter(store)
	ctx := context.Background()

	for _, req := range reqs {
		req.NgramConfig = document.NgramConfigDisabled()
		if err := writer.Index(ctx, req); err != nil {
			t.Fatalf("index %s: %v", req.DocumentID, err)
		}
	}
}

func tokenize(text string) []document.Token {
	var tokens []document.Token
	words := splitWords(text)
	for i, word := range words {
		tokens = append(tokens, document.Token{
			Term:     word,
			Position: i,
		})
	}
	return tokens
}

func splitWords(s string) []string {
	var words []string
	var current []rune
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' {
			if len(current) > 0 {
				words = append(words, string(current))
				current = nil
			}
		} else {
			// Lowercase
			if r >= 'A' && r <= 'Z' {
				r = r + 32
			}
			current = append(current, r)
		}
	}
	if len(current) > 0 {
		words = append(words, string(current))
	}
	return words
}

func newTestPebbleStore(t *testing.T) *pebble.PebbleStore {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "shard")

	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}

	// Initialize doc count
	store.SetInt64(pebble.DocCountKey(), 0)

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Logf("close PebbleStore: %v", err)
		}
		_ = os.RemoveAll(dir)
	})

	return store
}

// Ensure store satisfies the interface
var _ query.ShardStore = (*pebble.PebbleStore)(nil)

func init() {
	// For debugging - ensure json is imported
	_ = json.Marshal
}

