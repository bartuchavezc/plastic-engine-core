package clusterhttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	clusterhttp "plastic-engine-core/internal/adapters/http/cluster"
	"plastic-engine-core/internal/core/cluster"
	"plastic-engine-core/internal/core/cluster/nodes"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
)

func TestSearchEndpointValidatesPayload(t *testing.T) {
	t.Parallel()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, cluster.NewJoinService(coord))

	req := httptest.NewRequest(http.MethodPost, "/search", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSearchEndpointIndexNotFound(t *testing.T) {
	t.Parallel()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, cluster.NewJoinService(coord))

	payload := map[string]any{
		"index_id": "idx-missing",
		"query": map[string]any{
			"term": map[string]any{
				"field": "status",
				"value": "ready",
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/search", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestSearchEndpointReturnsNotImplementedForNow(t *testing.T) {
	t.Parallel()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, cluster.NewJoinService(coord))

	ctx := context.Background()
	_, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-orders",
		Name:             "orders",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:      "title",
				Type:      indexes.FieldTypeText,
				Analyzer:  "simple",
				Tokenizer: "whitespace",
				Indexed:   true,
			},
		},
		ShardConfig: indexes.ShardConfig{
			Strategy: indexes.ShardStrategyAutomatic,
			Automatic: &indexes.AutomaticShardConfig{
				ShardCount: 1,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	payload := map[string]any{
		"index_id": "idx-orders",
		"query": map[string]any{
			"match": map[string]any{
				"field": "title^10",
				"value": "sql",
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/search", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
}

func newSearchTestCoordinator(t *testing.T) *cluster.Coordinator {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "coord.db")
	coord, err := cluster.NewCoordinator("coordinator", "0", dbPath)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	t.Cleanup(func() {
		_ = coord.Close()
	})

	return coord
}
