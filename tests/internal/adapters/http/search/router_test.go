package searchhttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	searchhttp "plastic-engine-core/internal/adapters/http/search"
	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/core/search/segment"
	"plastic-engine-core/internal/pkg/logger"
)

func TestNewRouterHealthz(t *testing.T) {
	t.Parallel()

	router := searchhttp.NewRouter(&stubIndexer{}, nil, nil, logger.DefaultLogger())
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rr.Code, http.StatusOK)
	}

	if got := rr.Body.String(); got != "ok" {
		t.Fatalf("response body = %q, want %q", got, "ok")
	}
}

func TestDocumentsEndpointCallsIndexer(t *testing.T) {
	t.Parallel()

	idx := &stubIndexer{}
	router := searchhttp.NewRouter(idx, nil, nil, logger.DefaultLogger())

	body := []byte(`{
		"index_id": "idx",
		"shard_id": "shard-1",
		"document_id": "doc-1",
		"payload": {"title": "hello"}
	}`)

	req := httptest.NewRequest(http.MethodPost, "/documents", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d", rr.Code, http.StatusAccepted)
	}

	if len(idx.commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(idx.commands))
	}
	if idx.commands[0].DocumentID != "doc-1" {
		t.Fatalf("document id = %s, want doc-1", idx.commands[0].DocumentID)
	}
}

type stubIndexer struct {
	commands []document.Command
	err      error
}

func (s *stubIndexer) Index(ctx context.Context, cmd document.Command) error {
	s.commands = append(s.commands, cmd)
	return s.err
}

func TestTermLookupEndpoint(t *testing.T) {
	t.Parallel()

	lookup := &stubTermLookup{
		terms: []segment.TermEntry{
			{Field: "title", Term: "hello", TermID: "t1", DF: 5},
			{Field: "title", Term: "help", TermID: "t2", DF: 3},
			{Field: "title", Term: "helicopter", TermID: "t3", DF: 1},
		},
		total: 3,
	}

	router := searchhttp.NewRouterWithConfig(searchhttp.RouterConfig{
		Indexer:    &stubIndexer{},
		TermLookup: lookup,
		Logger:     logger.DefaultLogger(),
	})

	// Test basic list
	req := httptest.NewRequest(http.MethodGet, "/terms?shard_id=shard-1&field=title", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d, body: %s", rr.Code, http.StatusOK, rr.Body.String())
	}

	var resp struct {
		Terms []segment.TermEntry `json:"terms"`
		Total int64               `json:"total"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(resp.Terms) != 3 {
		t.Fatalf("expected 3 terms, got %d", len(resp.Terms))
	}
	if resp.Total != 3 {
		t.Fatalf("expected total=3, got %d", resp.Total)
	}
}

func TestTermLookupPrefixEndpoint(t *testing.T) {
	t.Parallel()

	lookup := &stubTermLookup{
		terms: []segment.TermEntry{
			{Field: "title", Term: "hello", TermID: "t1", DF: 5},
			{Field: "title", Term: "help", TermID: "t2", DF: 3},
		},
	}

	router := searchhttp.NewRouterWithConfig(searchhttp.RouterConfig{
		Indexer:    &stubIndexer{},
		TermLookup: lookup,
		Logger:     logger.DefaultLogger(),
	})

	req := httptest.NewRequest(http.MethodGet, "/terms?shard_id=shard-1&field=title&prefix=hel", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rr.Code, http.StatusOK)
	}

	if lookup.lastPrefix != "hel" {
		t.Fatalf("expected prefix=hel, got %s", lookup.lastPrefix)
	}
}

func TestTermLookupFuzzyEndpoint(t *testing.T) {
	t.Parallel()

	lookup := &stubTermLookup{
		terms: []segment.TermEntry{
			{Field: "title", Term: "hello", TermID: "t1", DF: 5},
		},
	}

	router := searchhttp.NewRouterWithConfig(searchhttp.RouterConfig{
		Indexer:    &stubIndexer{},
		TermLookup: lookup,
		Logger:     logger.DefaultLogger(),
	})

	req := httptest.NewRequest(http.MethodGet, "/terms?shard_id=shard-1&field=title&fuzzy=helo&distance=2", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rr.Code, http.StatusOK)
	}

	if lookup.lastFuzzy != "helo" {
		t.Fatalf("expected fuzzy=helo, got %s", lookup.lastFuzzy)
	}
	if lookup.lastDistance != 2 {
		t.Fatalf("expected distance=2, got %d", lookup.lastDistance)
	}
}

func TestTermLookupRegexEndpoint(t *testing.T) {
	t.Parallel()

	lookup := &stubTermLookup{
		terms: []segment.TermEntry{
			{Field: "title", Term: "hello", TermID: "t1", DF: 5},
		},
	}

	router := searchhttp.NewRouterWithConfig(searchhttp.RouterConfig{
		Indexer:    &stubIndexer{},
		TermLookup: lookup,
		Logger:     logger.DefaultLogger(),
	})

	req := httptest.NewRequest(http.MethodGet, "/terms?shard_id=shard-1&field=title&regex=hel.*", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rr.Code, http.StatusOK)
	}

	if lookup.lastRegex != "hel.*" {
		t.Fatalf("expected regex=hel.*, got %s", lookup.lastRegex)
	}
}

func TestTermLookupRequiresShardID(t *testing.T) {
	t.Parallel()

	router := searchhttp.NewRouterWithConfig(searchhttp.RouterConfig{
		Indexer:    &stubIndexer{},
		TermLookup: &stubTermLookup{},
		Logger:     logger.DefaultLogger(),
	})

	req := httptest.NewRequest(http.MethodGet, "/terms?field=title", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

type stubTermLookup struct {
	terms        []segment.TermEntry
	total        int64
	lastPrefix   string
	lastFuzzy    string
	lastDistance int
	lastRegex    string
}

func (s *stubTermLookup) ListTerms(ctx context.Context, shardID, field string, limit, offset int) ([]segment.TermEntry, int64, error) {
	return s.terms, s.total, nil
}

func (s *stubTermLookup) ListTermsByPrefix(ctx context.Context, shardID, field, prefix string, limit int) ([]segment.TermEntry, error) {
	s.lastPrefix = prefix
	return s.terms, nil
}

func (s *stubTermLookup) ListTermsByFuzzy(ctx context.Context, shardID, field, query string, maxDistance, limit int) ([]segment.TermEntry, error) {
	s.lastFuzzy = query
	s.lastDistance = maxDistance
	return s.terms, nil
}

func (s *stubTermLookup) ListTermsByRegex(ctx context.Context, shardID, field, pattern string, limit int) ([]segment.TermEntry, error) {
	s.lastRegex = pattern
	return s.terms, nil
}
