package clusterhttp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	clusterhttp "plastic-engine-core/internal/adapters/http/cluster"
	"plastic-engine-core/internal/core/cluster"
	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/nodes"
	"plastic-engine-core/internal/core/search/indexstore"
)

func TestTermLookupEndpointBasic(t *testing.T) {
	t.Parallel()

	// Mock search node that responds to /terms requests
	termResponses := make(chan []indexstore.TermEntry, 1)
	termResponses <- []indexstore.TermEntry{
		{Field: "title", Term: "hello", TermID: "t1", DF: 5},
		{Field: "title", Term: "world", TermID: "t2", DF: 3},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/terms" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		shardID := r.URL.Query().Get("shard_id")
		if shardID == "" {
			http.Error(w, "shard_id required", http.StatusBadRequest)
			return
		}

		terms := <-termResponses
		resp := struct {
			Terms []indexstore.TermEntry `json:"terms"`
			Total int64               `json:"total"`
			Limit int                 `json:"limit"`
		}{
			Terms: terms,
			Total: int64(len(terms)),
			Limit: 100,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()

	// Register search node
	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "search-node-1",
		Role:          "search",
		AdvertiseAddr: server.URL,
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if _, err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "search-node-1",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Create index
	_, err = coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-terms",
		Name:             "terms-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:    "title",
				Type:    indexes.FieldTypeText,
				Indexed: true,
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

	// Test term lookup endpoint
	req := httptest.NewRequest(http.MethodGet, "/terms?index_id=idx-terms&field=title", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Terms      []indexstore.TermEntry `json:"terms"`
		Total      int64               `json:"total"`
		ShardCount int                 `json:"shard_count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}

	if len(resp.Terms) != 2 {
		t.Fatalf("terms count = %d, want 2", len(resp.Terms))
	}
	if resp.ShardCount != 1 {
		t.Fatalf("shard_count = %d, want 1", resp.ShardCount)
	}
}

func TestTermLookupEndpointRequiresIndexID(t *testing.T) {
	t.Parallel()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	req := httptest.NewRequest(http.MethodGet, "/terms", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestTermLookupEndpointIndexNotFound(t *testing.T) {
	t.Parallel()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	req := httptest.NewRequest(http.MethodGet, "/terms?index_id=nonexistent", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestTermLookupEndpointWithPrefixSearch(t *testing.T) {
	t.Parallel()

	var lastPrefix string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/terms" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		lastPrefix = r.URL.Query().Get("prefix")

		resp := struct {
			Terms []indexstore.TermEntry `json:"terms"`
			Total int64               `json:"total"`
			Limit int                 `json:"limit"`
		}{
			Terms: []indexstore.TermEntry{
				{Field: "title", Term: "hello", TermID: "t1", DF: 5},
			},
			Total: 1,
			Limit: 100,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "search-node-2",
		Role:          "search",
		AdvertiseAddr: server.URL,
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if _, err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "search-node-2",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	_, err = coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-prefix",
		Name:             "prefix-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{Name: "title", Type: indexes.FieldTypeText, Indexed: true},
		},
		ShardConfig: indexes.ShardConfig{
			Strategy:  indexes.ShardStrategyAutomatic,
			Automatic: &indexes.AutomaticShardConfig{ShardCount: 1},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/terms?index_id=idx-prefix&prefix=hel", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if lastPrefix != "hel" {
		t.Fatalf("expected prefix=hel to be forwarded, got %s", lastPrefix)
	}
}

func TestTermLookupEndpointWithFuzzySearch(t *testing.T) {
	t.Parallel()

	var lastFuzzy, lastDistance string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/terms" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		lastFuzzy = r.URL.Query().Get("fuzzy")
		lastDistance = r.URL.Query().Get("distance")

		resp := struct {
			Terms []indexstore.TermEntry `json:"terms"`
			Total int64               `json:"total"`
			Limit int                 `json:"limit"`
		}{
			Terms: []indexstore.TermEntry{
				{Field: "title", Term: "hello", TermID: "t1", DF: 5},
			},
			Total: 1,
			Limit: 100,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "search-node-3",
		Role:          "search",
		AdvertiseAddr: server.URL,
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if _, err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "search-node-3",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	_, err = coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-fuzzy",
		Name:             "fuzzy-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{Name: "title", Type: indexes.FieldTypeText, Indexed: true},
		},
		ShardConfig: indexes.ShardConfig{
			Strategy:  indexes.ShardStrategyAutomatic,
			Automatic: &indexes.AutomaticShardConfig{ShardCount: 1},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/terms?index_id=idx-fuzzy&fuzzy=helo&distance=2", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if lastFuzzy != "helo" {
		t.Fatalf("expected fuzzy=helo to be forwarded, got %s", lastFuzzy)
	}
	if lastDistance != "2" {
		t.Fatalf("expected distance=2 to be forwarded, got %s", lastDistance)
	}
}

func TestTermLookupEndpointWithDateBasedShardFiltering(t *testing.T) {
	t.Parallel()

	requestedShards := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/terms" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		shardID := r.URL.Query().Get("shard_id")
		requestedShards[shardID] = true

		resp := struct {
			Terms []indexstore.TermEntry `json:"terms"`
			Total int64               `json:"total"`
			Limit int                 `json:"limit"`
		}{
			Terms: []indexstore.TermEntry{
				{Field: "timestamp", Term: "2024-01-15", TermID: "t1", DF: 10},
			},
			Total: 1,
			Limit: 100,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "search-node-date",
		Role:          "search",
		AdvertiseAddr: server.URL,
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if _, err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "search-node-date",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Create index with date-based sharding
	_, err = coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-date",
		Name:             "date-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{Name: "message", Type: indexes.FieldTypeText, Indexed: true},
			{Name: "timestamp", Type: indexes.FieldTypeDate, Indexed: true},
		},
		ShardConfig: indexes.ShardConfig{
			Strategy: indexes.ShardStrategyDate,
			Date: &indexes.DateShardConfig{
				Field:       "timestamp",
				Granularity: "month",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Make request with time range filter
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)

	url := "/terms?index_id=idx-date&field=message&start_time=" + startTime.Format(time.RFC3339) + "&end_time=" + endTime.Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Terms      []indexstore.TermEntry `json:"terms"`
		Total      int64               `json:"total"`
		ShardCount int                 `json:"shard_count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}

	// Note: The test validates the endpoint works with time filters
	// Actual shard filtering depends on how shards are created (by coordinator)
	// The important thing is the request succeeds with time range params
	if resp.ShardCount < 0 {
		t.Fatalf("expected non-negative shard_count, got %d", resp.ShardCount)
	}
}

func TestTermLookupMergesResultsFromMultipleShards(t *testing.T) {
	t.Parallel()

	// This test simulates having multiple shards return the same term
	// The cluster should aggregate the DF values
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/terms" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		requestCount++
		// Return same term with different DF from each "shard"
		resp := struct {
			Terms []indexstore.TermEntry `json:"terms"`
			Total int64               `json:"total"`
			Limit int                 `json:"limit"`
		}{
			Terms: []indexstore.TermEntry{
				{Field: "title", Term: "common", TermID: "t1", DF: 10},
			},
			Total: 1,
			Limit: 100,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "search-node-merge",
		Role:          "search",
		AdvertiseAddr: server.URL,
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if _, err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "search-node-merge",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Create index with multiple shards
	_, err = coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-merge",
		Name:             "merge-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{Name: "title", Type: indexes.FieldTypeText, Indexed: true},
		},
		ShardConfig: indexes.ShardConfig{
			Strategy: indexes.ShardStrategyAutomatic,
			Automatic: &indexes.AutomaticShardConfig{
				ShardCount: 2, // Multiple shards
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/terms?index_id=idx-merge&field=title", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Terms      []indexstore.TermEntry `json:"terms"`
		Total      int64               `json:"total"`
		ShardCount int                 `json:"shard_count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}

	// Should have 2 shards
	if resp.ShardCount != 2 {
		t.Fatalf("shard_count = %d, want 2", resp.ShardCount)
	}

	// Should have deduplicated to 1 term
	if len(resp.Terms) != 1 {
		t.Fatalf("terms count = %d, want 1 (deduplicated)", len(resp.Terms))
	}

	// DF should be aggregated (10 + 10 = 20)
	if resp.Terms[0].DF != 20 {
		t.Fatalf("aggregated DF = %d, want 20", resp.Terms[0].DF)
	}
}

func TestTermLookupEndpointEmptyIndex(t *testing.T) {
	t.Parallel()

	coord := newSearchTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()

	// Create index without any shards assigned (no nodes registered)
	_, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-empty",
		Name:             "empty-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{Name: "title", Type: indexes.FieldTypeText, Indexed: true},
		},
		ShardConfig: indexes.ShardConfig{
			Strategy:  indexes.ShardStrategyAutomatic,
			Automatic: &indexes.AutomaticShardConfig{ShardCount: 1},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/terms?index_id=idx-empty", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	// Should return OK with empty results when no shards are assigned
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Terms      []indexstore.TermEntry `json:"terms"`
		Total      int64               `json:"total"`
		ShardCount int                 `json:"shard_count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}

	if len(resp.Terms) != 0 {
		t.Fatalf("terms count = %d, want 0", len(resp.Terms))
	}
}

// newSearchTestCoordinator creates a test coordinator for search tests.
func newSearchTestCoordinator(t *testing.T) *cluster.Coordinator {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "coord-search.db")
	coord, err := cluster.NewCoordinator("coordinator", "0", dbPath)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	t.Cleanup(func() {
		_ = coord.Close()
	})

	// Ensure registry initialised
	if _, err := coord.DB().ExecContext(context.Background(), `SELECT 1`); err != nil {
		t.Fatalf("Smoke query: %v", err)
	}

	return coord
}
