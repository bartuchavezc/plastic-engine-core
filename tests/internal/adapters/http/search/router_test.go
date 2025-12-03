package searchhttp_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	searchhttp "plastic-engine-core/internal/adapters/http/search"
	"plastic-engine-core/internal/core/search/indexer"
	"plastic-engine-core/internal/helpers"
)

func TestNewRouterHealthz(t *testing.T) {
	t.Parallel()

	router := searchhttp.NewRouter(&stubIndexer{}, helpers.DefaultLogger())
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
	router := searchhttp.NewRouter(idx, helpers.DefaultLogger())

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
	commands []indexer.Command
	err      error
}

func (s *stubIndexer) Index(ctx context.Context, cmd indexer.Command) error {
	s.commands = append(s.commands, cmd)
	return s.err
}
