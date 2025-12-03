package coordinator_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"plastic-engine-core/internal/core/cluster"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/pkg/logger"
)

func TestHandlerReturnsShardNotFound(t *testing.T) {
	handler := cluster.Handler{
		IndexRepo:  &staticIndexRepo{},
		DB:         openTestDB(t),
		HTTPClient: http.DefaultClient,
	}

	req := cluster.Request{
		IndexID:    "idx-test",
		DocumentID: "doc-1",
		Payload:    json.RawMessage(`{"title":"hello","attempts":"5","created_at":"2006-01-02T15:04:05Z"}`),
	}

	err := handler.Handle(context.Background(), req)
	if err == nil || !errors.Is(err, cluster.ErrShardNotFound) {
		t.Fatalf("expected ErrShardNotFound, got %v", err)
	}
}

func TestHandlerForwardsDocument(t *testing.T) {
	db := openTestDB(t)
	seedIndexRow(t, db, "idx-test")
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/documents" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)
	seedNode(t, db, "node-1", server.URL)
	seedShard(t, db, "idx-test", "idx-test-default", "node-1")

	handler := cluster.Handler{
		IndexRepo:  &staticIndexRepo{},
		DB:         db,
		HTTPClient: server.Client(),
		Logger:     logger.DefaultLogger(),
	}

	req := cluster.Request{
		IndexID:    "idx-test",
		DocumentID: "doc-1",
		Payload:    json.RawMessage(`{"title":"hello","attempts":"5","created_at":"2006-01-02T15:04:05Z"}`),
	}

	if err := handler.Handle(context.Background(), req); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(capturedBody) == 0 {
		t.Fatalf("expected forwarded payload")
	}

	var forwarded struct {
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(capturedBody, &forwarded); err != nil {
		t.Fatalf("unmarshal forwarded: %v", err)
	}
	if forwarded.Payload["title"] != "hello" {
		t.Fatalf("title = %v, want %q", forwarded.Payload["title"], "hello")
	}
	if forwarded.Payload["attempts"] != float64(5) {
		t.Fatalf("attempts = %v, want 5", forwarded.Payload["attempts"])
	}
	if _, err := time.Parse(time.RFC3339Nano, forwarded.Payload["created_at"].(string)); err != nil {
		t.Fatalf("created_at invalid: %v", err)
	}
}

func TestHandlerValidatesRequiredFields(t *testing.T) {
	db := openTestDB(t)
	seedIndexRow(t, db, "idx-test")
	seedNode(t, db, "node-1", "http://example.com")
	seedShard(t, db, "idx-test", "idx-test-default", "node-1")

	handler := cluster.Handler{
		IndexRepo:  &staticIndexRepo{},
		DB:         db,
		HTTPClient: http.DefaultClient,
		Logger:     logger.DefaultLogger(),
	}

	req := cluster.Request{
		IndexID:    "idx-test",
		DocumentID: "doc-1",
		Payload:    json.RawMessage(`{"title": ""}`),
	}

	err := handler.Handle(context.Background(), req)
	var validationErr *indexes.ValidationError
	if err == nil || !errors.As(err, &validationErr) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

type staticIndexRepo struct{}

func (staticIndexRepo) GetIndex(context.Context, string) (indexes.IndexDefinition, error) {
	return indexes.IndexDefinition{
		ID:            "idx-test",
		Name:          "test",
		ShardStrategy: indexes.ShardStrategyAutomatic,
		FieldMappings: []indexes.FieldMapping{
			{Name: "title", Type: indexes.FieldTypeText, Required: true, Indexed: true},
			{Name: "attempts", Type: indexes.FieldTypeInteger, Indexed: true},
			{Name: "created_at", Type: indexes.FieldTypeDate, Indexed: true},
		},
		ShardConfig: indexes.ShardConfig{
			Strategy:  indexes.ShardStrategyAutomatic,
			Automatic: &indexes.AutomaticShardConfig{ShardCount: 1},
		},
	}, nil
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "ingest.db")
	db, err := cluster.OpenMetadataDB(dbPath)
	if err != nil {
		t.Fatalf("OpenMetadataDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedShard(t *testing.T, db *sql.DB, indexID, shardID, nodeID string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO shards(id, index_id, shard_key, primary_node, state)
		VALUES(?, ?, ?, ?, 'assigned')`, shardID, indexID, "default", nodeID)
	if err != nil {
		t.Fatalf("insert shard: %v", err)
	}
}

func seedNode(t *testing.T, db *sql.DB, nodeID, addr string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO nodes(id, role, advertise_addr, status)
		VALUES(?, 'search', ?, 'ready')`, nodeID, addr)
	if err != nil {
		t.Fatalf("insert node: %v", err)
	}
}

func seedIndexRow(t *testing.T, db *sql.DB, indexID string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO indexes(
		id, name, shard_strategy, shard_template, shard_config,
		default_analyzer, default_tokenizer, mapping_version, created_at, updated_at)
		VALUES(?, ?, 'automatic', '', '{}', 'simple', 'whitespace', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, indexID, indexID)
	if err != nil {
		t.Fatalf("insert index: %v", err)
	}
}
