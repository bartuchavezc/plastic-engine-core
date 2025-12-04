package cluster_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"plastic-engine-core/internal/core/cluster"
)

func TestLocalApplier_IsAlwaysLeader(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)

	if !applier.IsLeader() {
		t.Error("LocalApplier should always be leader")
	}

	if addr := applier.LeaderAddr(); addr != "" {
		t.Errorf("LocalApplier.LeaderAddr() = %q, want empty string", addr)
	}
}

func TestLocalApplier_WaitForLeader(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	addr, err := applier.WaitForLeader(ctx)
	if err != nil {
		t.Errorf("WaitForLeader() error = %v", err)
	}
	if addr != "" {
		t.Errorf("WaitForLeader() = %q, want empty string", addr)
	}
}

func TestLocalApplier_CreateIndex(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)

	payload := cluster.CreateIndexPayload{
		ID:               "test-index",
		Name:             "Test Index",
		ShardStrategy:    "automatic",
		DefaultAnalyzer:  "standard",
		DefaultTokenizer: "standard",
		MappingVersion:   1,
		CreatedAt:        time.Now().UTC(),
	}

	cmd, err := cluster.NewCommand(cluster.CmdCreateIndex, payload)
	if err != nil {
		t.Fatalf("NewCommand() error = %v", err)
	}

	ctx := context.Background()
	if err := applier.Apply(ctx, cmd); err != nil {
		t.Errorf("Apply(CreateIndex) error = %v", err)
	}

	// Verify index was created
	var count int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM indexes WHERE id = ?`, "test-index").Scan(&count)
	if err != nil {
		t.Fatalf("query index count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 index, got %d", count)
	}
}

func TestLocalApplier_CreateIndexWithFields(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)

	fields := []map[string]interface{}{
		{"name": "title", "type": "text", "analyzer": "standard", "indexed": true},
		{"name": "date", "type": "date", "stored": true},
	}
	fieldsJSON, _ := json.Marshal(fields)

	payload := cluster.CreateIndexPayload{
		ID:               "test-index-fields",
		Name:             "Test Index With Fields",
		ShardStrategy:    "automatic",
		DefaultAnalyzer:  "standard",
		DefaultTokenizer: "standard",
		MappingVersion:   1,
		FieldMappings:    fieldsJSON,
		CreatedAt:        time.Now().UTC(),
	}

	cmd, err := cluster.NewCommand(cluster.CmdCreateIndex, payload)
	if err != nil {
		t.Fatalf("NewCommand() error = %v", err)
	}

	ctx := context.Background()
	if err := applier.Apply(ctx, cmd); err != nil {
		t.Errorf("Apply(CreateIndex) error = %v", err)
	}

	// Verify fields were created
	var fieldCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_fields WHERE index_id = ?`, "test-index-fields").Scan(&fieldCount)
	if err != nil {
		t.Fatalf("query field count: %v", err)
	}
	if fieldCount != 2 {
		t.Errorf("expected 2 fields, got %d", fieldCount)
	}
}

func TestLocalApplier_RegisterNode(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)

	payload := cluster.RegisterNodePayload{
		NodeID:        "node-1",
		Role:          "search",
		AdvertiseAddr: "192.168.1.10:8080",
		DataDir:       "/data/shards",
	}

	cmd, err := cluster.NewCommand(cluster.CmdRegisterNode, payload)
	if err != nil {
		t.Fatalf("NewCommand() error = %v", err)
	}

	ctx := context.Background()
	if err := applier.Apply(ctx, cmd); err != nil {
		t.Errorf("Apply(RegisterNode) error = %v", err)
	}

	// Verify node was created
	var status string
	err = db.QueryRowContext(ctx, `SELECT status FROM nodes WHERE id = ?`, "node-1").Scan(&status)
	if err != nil {
		t.Fatalf("query node: %v", err)
	}
	if status != "joining" {
		t.Errorf("node status = %q, want 'joining'", status)
	}
}

func TestLocalApplier_UpdateHeartbeat(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)
	ctx := context.Background()

	// First register a node
	registerCmd, _ := cluster.NewCommand(cluster.CmdRegisterNode, cluster.RegisterNodePayload{
		NodeID: "node-hb",
		Role:   "search",
	})
	if err := applier.Apply(ctx, registerCmd); err != nil {
		t.Fatalf("register node: %v", err)
	}

	// Update heartbeat
	heartbeatCmd, _ := cluster.NewCommand(cluster.CmdUpdateHeartbeat, cluster.UpdateHeartbeatPayload{
		NodeID:    "node-hb",
		Timestamp: time.Now().UTC(),
	})
	if err := applier.Apply(ctx, heartbeatCmd); err != nil {
		t.Errorf("Apply(UpdateHeartbeat) error = %v", err)
	}

	// Verify status changed to ready
	var status string
	err := db.QueryRowContext(ctx, `SELECT status FROM nodes WHERE id = ?`, "node-hb").Scan(&status)
	if err != nil {
		t.Fatalf("query node: %v", err)
	}
	if status != "ready" {
		t.Errorf("node status = %q, want 'ready'", status)
	}
}

func TestLocalApplier_UpdateHeartbeat_NodeNotFound(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)
	ctx := context.Background()

	cmd, _ := cluster.NewCommand(cluster.CmdUpdateHeartbeat, cluster.UpdateHeartbeatPayload{
		NodeID:    "nonexistent-node",
		Timestamp: time.Now().UTC(),
	})

	err := applier.Apply(ctx, cmd)
	if err == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestLocalApplier_CreateShards(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)
	ctx := context.Background()

	// First create an index
	indexCmd, _ := cluster.NewCommand(cluster.CmdCreateIndex, cluster.CreateIndexPayload{
		ID:               "idx-shards",
		Name:             "Index for Shards",
		ShardStrategy:    "automatic",
		DefaultAnalyzer:  "standard",
		DefaultTokenizer: "standard",
		MappingVersion:   1,
		CreatedAt:        time.Now().UTC(),
	})
	if err := applier.Apply(ctx, indexCmd); err != nil {
		t.Fatalf("create index: %v", err)
	}

	// Create shards
	shardsCmd, _ := cluster.NewCommand(cluster.CmdCreateShards, cluster.CreateShardsPayload{
		IndexID: "idx-shards",
		Keys:    []string{"shard-0", "shard-1", "shard-2"},
	})
	if err := applier.Apply(ctx, shardsCmd); err != nil {
		t.Errorf("Apply(CreateShards) error = %v", err)
	}

	// Verify shards were created
	var shardCount int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM shards WHERE index_id = ?`, "idx-shards").Scan(&shardCount)
	if err != nil {
		t.Fatalf("query shard count: %v", err)
	}
	if shardCount != 3 {
		t.Errorf("expected 3 shards, got %d", shardCount)
	}
}

func TestLocalApplier_UnknownCommand(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	applier := cluster.NewLocalApplier(db, nil)

	cmd := cluster.Command{
		Type:      cluster.CommandType(255), // Unknown type
		Timestamp: time.Now().UTC(),
		Payload:   []byte("{}"),
	}

	err := applier.Apply(context.Background(), cmd)
	if err == nil {
		t.Error("expected error for unknown command type")
	}
}

// Helper functions

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := cluster.OpenMetadataDB(dbPath)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return db, cleanup
}

