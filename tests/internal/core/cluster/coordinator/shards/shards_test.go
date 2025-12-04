package shards_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"plastic-engine-core/internal/core/cluster"
	shardspkg "plastic-engine-core/internal/core/cluster/shards"
)

func TestAssignShardsToNodeTxAssignsPendingShard(t *testing.T) {
	db := openShardTestDB(t)
	seedIndex(t, db, "idx-test")
	seedShardRow(t, db, "shard-1", "idx-test")
	seedNode(t, db, "node-1")

	repo := shardspkg.NewRepository(db)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()

	assignments, err := repo.AssignToNodeTx(context.Background(), tx, "node-1", 1)
	if err != nil {
		t.Fatalf("AssignToNodeTx: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("assignments len = %d, want 1", len(assignments))
	}
	if assignments[0].ShardKey != "default" {
		t.Fatalf("ShardKey = %q, want default", assignments[0].ShardKey)
	}
}

func TestLookupPrimaryShardErrorWhenMissing(t *testing.T) {
	db := openShardTestDB(t)
	repo := shardspkg.NewRepository(db)

	if _, err := repo.LookupPrimaryShard(context.Background(), "idx", "default"); err == nil {
		t.Fatalf("expected error when shard missing")
	}
}

func TestLookupPrimaryShardReturnsInfo(t *testing.T) {
	db := openShardTestDB(t)
	repo := shardspkg.NewRepository(db)

	seedIndex(t, db, "idx-test")
	seedNode(t, db, "node-1")
	seedShardAssigned(t, db, "shard-1", "idx-test", "node-1")

	info, err := repo.LookupPrimaryShard(context.Background(), "idx-test", "default")
	if err != nil {
		t.Fatalf("LookupPrimaryShard: %v", err)
	}
	if info.PrimaryNode != "node-1" {
		t.Fatalf("PrimaryNode = %q, want node-1", info.PrimaryNode)
	}
}

func openShardTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "shards.db")
	db, err := cluster.OpenMetadataDB(dbPath)
	if err != nil {
		t.Fatalf("OpenMetadataDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedIndex(t *testing.T, db *sql.DB, indexID string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO indexes(id, name, shard_strategy, shard_template, shard_config, default_analyzer, default_tokenizer, mapping_version, created_at, updated_at)
		VALUES(?, ?, 'automatic', '', '{}', 'simple', 'whitespace', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, indexID, indexID)
	if err != nil {
		t.Fatalf("insert index: %v", err)
	}
}

func seedShardRow(t *testing.T, db *sql.DB, shardID, indexID string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO shards(id, index_id, shard_key, state, version, created_at)
		VALUES(?, ?, 'default', 'pending', 0, CURRENT_TIMESTAMP)`, shardID, indexID)
	if err != nil {
		t.Fatalf("insert shard: %v", err)
	}
}

func seedNode(t *testing.T, db *sql.DB, nodeID string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO nodes(id, role, status) VALUES(?, 'search', 'ready')`, nodeID)
	if err != nil {
		t.Fatalf("insert node: %v", err)
	}
}

func seedShardAssigned(t *testing.T, db *sql.DB, shardID, indexID, nodeID string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO shards(id, index_id, shard_key, primary_node, state, version, created_at)
		VALUES(?, ?, 'default', ?, 'assigned', 0, CURRENT_TIMESTAMP)`, shardID, indexID, nodeID)
	if err != nil {
		t.Fatalf("insert assigned shard: %v", err)
	}
}
