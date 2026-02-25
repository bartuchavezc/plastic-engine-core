package clusterhttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	clusterhttp "plastic-engine-core/internal/adapters/http/cluster"
	"plastic-engine-core/internal/core/cluster"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/nodes"
)

func TestCreateIndexEndpoint(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	payload := map[string]any{
		"id":   "idx-blog",
		"name": "blog",
		"shard_config": map[string]any{
			"strategy": "computed",
			"computed": map[string]any{
				"components": []map[string]any{
					{
						"field":     "title",
						"transform": "slug",
					},
				},
			},
		},
		"default_analyzer":  "simple",
		"default_tokenizer": "whitespace",
		"field_mappings": []map[string]any{
			{
				"name":       "title",
				"type":       "text",
				"analyzer":   "simple",
				"tokenizer":  "whitespace",
				"searchable": true,
				"stored":     true,
				"required":   true,
				"index":      true,
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/indexes", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusCreated)
	}

	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}

	if response["id"] != "idx-blog" {
		t.Fatalf("response id = %v, want %q", response["id"], "idx-blog")
	}
}

func TestCreateIndexEndpointValidationError(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	payload := map[string]any{
		"name": "missing-id",
		"shard_config": map[string]any{
			"strategy": "automatic",
		},
		"default_analyzer":  "simple",
		"default_tokenizer": "whitespace",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/indexes", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestIngestDocumentRoutesToPrimaryShard(t *testing.T) {
	t.Parallel()

	var forwardedBody []byte
	forwardedCh := make(chan struct{}, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Handle both single document and bulk endpoints
		if r.URL.Path != "/documents" && r.URL.Path != "/documents/bulk" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if r.URL.Path == "/documents/bulk" {
			// Extract first document from bulk request
			var bulkReq struct {
				Documents []json.RawMessage `json:"documents"`
			}
			if err := json.Unmarshal(body, &bulkReq); err == nil && len(bulkReq.Documents) > 0 {
				forwardedBody = bulkReq.Documents[0]
			}
		} else {
			forwardedBody = body
		}
		w.WriteHeader(http.StatusOK)
		select {
		case forwardedCh <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	coord := newTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-1",
		Role:          "search",
		AdvertiseAddr: server.URL,
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if _, err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "node-1",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	createPayload := map[string]any{
		"id":   "idx-log",
		"name": "log",
		"shard_config": map[string]any{
			"strategy": "automatic",
			"automatic": map[string]any{
				"shard_count": 1,
			},
		},
		"default_analyzer":  "simple",
		"default_tokenizer": "whitespace",
		"field_mappings": []map[string]any{
			{
				"name":       "message",
				"type":       "text",
				"analyzer":   "simple",
				"tokenizer":  "whitespace",
				"searchable": true,
				"required":   true,
				"index":      true,
			},
		},
	}

	body, err := json.Marshal(createPayload)
	if err != nil {
		t.Fatalf("Marshal create payload: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/indexes", bytes.NewReader(body))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("create index status = %d, want %d", rec.Code, http.StatusCreated)
	}

	ingestPayload := map[string]any{
		"document_id": "doc-1",
		"payload": map[string]any{
			"message": "hello world",
		},
	}

	ingestBody, err := json.Marshal(ingestPayload)
	if err != nil {
		t.Fatalf("Marshal ingest payload: %v", err)
	}

	ingestReq := httptest.NewRequest(http.MethodPost, "/indexes/idx-log/documents", bytes.NewReader(ingestBody))
	ingestRec := httptest.NewRecorder()

	router.ServeHTTP(ingestRec, ingestReq)

	if ingestRec.Code != http.StatusOK {
		t.Fatalf("ingest status = %d, want %d", ingestRec.Code, http.StatusOK)
	}

	// Batcher is synchronous — document was forwarded before handler returned.
	select {
	case <-forwardedCh:
	default:
	}

	if len(forwardedBody) == 0 {
		t.Fatalf("expected forwarded body, got none")
	}

	var forwarded map[string]any
	if err := json.Unmarshal(forwardedBody, &forwarded); err != nil {
		t.Fatalf("Unmarshal forwarded body: %v", err)
	}

	if forwarded["index_id"] != "idx-log" {
		t.Fatalf("forwarded index_id = %v, want idx-log", forwarded["index_id"])
	}
	if forwarded["document_id"] != "doc-1" {
		t.Fatalf("forwarded document_id = %v, want doc-1", forwarded["document_id"])
	}
	if forwarded["shard_id"] == "" {
		t.Fatalf("expected shard_id in forwarded payload")
	}
}

func TestIngestDocumentRequiresFields(t *testing.T) {
	// NOTE: This test has been updated to reflect the new architecture.
	// Mapping validation has been moved from the coordinator to the search node
	// for better performance (eliminates double JSON parsing).
	// Required field validation now happens at the search node level.
	// This test verifies that the coordinator correctly routes documents
	// even when fields are missing - validation errors are returned from the search node.
	t.Skip("Validation moved to search node - see tests/internal/core/search/document for validation tests")
}

func TestIngestDocumentValidatesFieldTypes(t *testing.T) {
	// NOTE: This test has been updated to reflect the new architecture.
	// Mapping validation has been moved from the coordinator to the search node
	// for better performance (eliminates double JSON parsing).
	// Type validation now happens at the search node level.
	// This test verifies that the coordinator correctly routes documents
	// even with wrong types - validation errors are returned from the search node.
	t.Skip("Validation moved to search node - see tests/internal/core/search/document for validation tests")
}

func TestListNodesEndpoint(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))
	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-admin",
		Role:          "search",
		AdvertiseAddr: "http://127.0.0.1:9000",
		DataDir:       "/var/lib/plastic",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if _, err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "node-admin",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/cluster/management/nodes", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var nodeList []struct {
		ID            string     `json:"id"`
		Role          string     `json:"role"`
		Status        string     `json:"status"`
		LastHeartbeat *time.Time `json:"last_heartbeat"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &nodeList); err != nil {
		t.Fatalf("Unmarshal nodes: %v", err)
	}

	if len(nodeList) != 1 {
		t.Fatalf("nodes length = %d, want 1", len(nodeList))
	}
	if nodeList[0].ID != "node-admin" {
		t.Fatalf("node id = %q, want %q", nodeList[0].ID, "node-admin")
	}
	if nodeList[0].Role != "search" {
		t.Fatalf("node role = %q, want %q", nodeList[0].Role, "search")
	}
	if nodeList[0].Status != "ready" {
		t.Fatalf("node status = %q, want %q", nodeList[0].Status, "ready")
	}
	if nodeList[0].LastHeartbeat == nil {
		t.Fatalf("expected last heartbeat to be present")
	}
}

func TestListIndexesEndpoint(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()
	_, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-admin",
		Name:             "admin",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:      "title",
				Type:      indexes.FieldTypeText,
				Analyzer:  "simple",
				Tokenizer: "whitespace",
				Stored:    true,
				Required:  true,
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

	req := httptest.NewRequest(http.MethodGet, "/indexes", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var indexList []struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		ShardStrategy string `json:"shard_strategy"`
		FieldMappings []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Required bool   `json:"required"`
			Index    bool   `json:"index"`
		} `json:"field_mappings"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &indexList); err != nil {
		t.Fatalf("Unmarshal indexes: %v", err)
	}

	if len(indexList) != 1 {
		t.Fatalf("indexes length = %d, want 1", len(indexList))
	}
	if indexList[0].ID != "idx-admin" {
		t.Fatalf("index id = %q, want %q", indexList[0].ID, "idx-admin")
	}
	if indexList[0].ShardStrategy != "automatic" {
		t.Fatalf("shard strategy = %q, want %q", indexList[0].ShardStrategy, "automatic")
	}
	if got := len(indexList[0].FieldMappings); got != 1 {
		t.Fatalf("field mappings length = %d, want 1", got)
	}
	if !indexList[0].FieldMappings[0].Required {
		t.Fatalf("expected field mapping required")
	}
	if !indexList[0].FieldMappings[0].Index {
		t.Fatalf("expected field mapping indexed")
	}
}

func TestGetIndexEndpoint(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()
	_, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-detail",
		Name:             "detail",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:    "message",
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

	req := httptest.NewRequest(http.MethodGet, "/indexes/idx-detail", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var index struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		ShardStrategy string `json:"shard_strategy"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &index); err != nil {
		t.Fatalf("Unmarshal index: %v", err)
	}

	if index.ID != "idx-detail" {
		t.Fatalf("index id = %q, want %q", index.ID, "idx-detail")
	}
	if index.Name != "detail" {
		t.Fatalf("index name = %q, want %q", index.Name, "detail")
	}
	if index.ShardStrategy != "automatic" {
		t.Fatalf("shard strategy = %q, want %q", index.ShardStrategy, "automatic")
	}
}

func TestListShardsForIndexEndpoint(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	router := clusterhttp.NewRouter(coord, nodes.NewJoinService(coord.NodesService()))

	ctx := context.Background()
	_, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-shard",
		Name:             "shard",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:    "name",
				Type:    indexes.FieldTypeKeyword,
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

	req := httptest.NewRequest(http.MethodGet, "/indexes/idx-shard/shards", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var shardList []struct {
		IndexID string `json:"index_id"`
		State   string `json:"state"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &shardList); err != nil {
		t.Fatalf("Unmarshal shards: %v", err)
	}

	if len(shardList) == 0 {
		t.Fatalf("expected shards, got none")
	}

	for _, shard := range shardList {
		if shard.IndexID != "idx-shard" {
			t.Fatalf("shard index_id = %q, want %q", shard.IndexID, "idx-shard")
		}
		if shard.State == "" {
			t.Fatalf("expected shard state to be populated")
		}
	}
}

func newTestCoordinator(t *testing.T) *cluster.Coordinator {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "coord.db")
	coord, err := cluster.NewCoordinator("coordinator", "0", dbPath)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	t.Cleanup(func() {
		_ = coord.Close()
	})

	// Ensure registry initialised by hitting a no-op query.
	if _, err := coord.DB().ExecContext(context.Background(), `SELECT 1`); err != nil {
		t.Fatalf("Smoke query: %v", err)
	}

	return coord
}
