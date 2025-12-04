package coordinator_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"plastic-engine-core/internal/core/cluster"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/nodes"
)

func TestStartHealthMonitorMarksStaleNodes(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)

	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-1",
		Role:          "search",
		AdvertiseAddr: "localhost:0",
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "node-1",
		Shards: nil,
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	monitorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	coord.StartHealthMonitor(monitorCtx, 50*time.Millisecond, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	row := coord.DB().QueryRow(`SELECT status FROM nodes WHERE id = ?`, "node-1")
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatalf("Scan status: %v", err)
	}
	if want := "unreachable"; status != want {
		t.Fatalf("node status = %q, want %q", status, want)
	}
}

func TestCreateIndexAssignsShardsToReadyNodes(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)

	ctx := context.Background()

	joinResp, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-ready",
		Role:          "search",
		AdvertiseAddr: "localhost:0",
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if len(joinResp.Shards) != 0 {
		t.Fatalf("expected no shards before index creation")
	}

	if err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "node-ready",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	createResp, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-metrics",
		Name:             "metrics",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:    "value",
				Type:    indexes.FieldTypeInteger,
				Stored:  true,
				Indexed: false,
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

	if createResp.Definition.ID != "idx-metrics" {
		t.Fatalf("unexpected index id %q", createResp.Definition.ID)
	}

	row := coord.DB().QueryRow(`SELECT primary_node, state FROM shards WHERE index_id = ?`, "idx-metrics")
	var primary, state string
	if err := row.Scan(&primary, &state); err != nil {
		t.Fatalf("Scan shard: %v", err)
	}
	if primary != "node-ready" {
		t.Fatalf("primary node = %q, want %q", primary, "node-ready")
	}
	if state != "assigned" {
		t.Fatalf("shard state = %q, want %q", state, "assigned")
	}
}

func TestJoinAssignsPendingShards(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	_, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-orders",
		Name:             "orders",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:     "order_id",
				Type:     indexes.FieldTypeKeyword,
				Required: true,
				Indexed:  true,
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

	resp, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-assign",
		Role:          "search",
		AdvertiseAddr: "localhost:0",
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if len(resp.Shards) == 0 {
		t.Fatalf("expected shard assignments, got none")
	}
}

func TestJoinReturnsExistingAssignments(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	initialResp, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-rejoin",
		Role:          "search",
		AdvertiseAddr: "localhost:0",
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("initial Join: %v", err)
	}
	if len(initialResp.Shards) != 0 {
		t.Fatalf("expected no shards on first join, got %d", len(initialResp.Shards))
	}

	if err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{
		NodeID: "node-rejoin",
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	_, err = coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-rejoin",
		Name:             "rejoin",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{
				Name:    "description",
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

	rejoinResp, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-rejoin",
		Role:          "search",
		AdvertiseAddr: "localhost:0",
		DataDir:       "/tmp",
	})
	if err != nil {
		t.Fatalf("rejoin: %v", err)
	}

	if len(rejoinResp.Shards) == 0 {
		t.Fatalf("expected assigned shards on rejoin")
	}

	found := false
	for _, shard := range rejoinResp.Shards {
		if shard.IndexID == "idx-rejoin" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected shard for idx-rejoin in response: %+v", rejoinResp.Shards)
	}
}

func TestJoinRequiresRole(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	if _, err := coord.NodesService().Join(ctx, nodes.JoinRequest{}); err == nil {
		t.Fatalf("expected error for missing role")
	}
}

func TestHeartbeatUpdatesStatus(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-heartbeat",
		Role:          "search",
		AdvertiseAddr: "http://127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{NodeID: "node-heartbeat"}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	var status string
	if err := coord.DB().QueryRow(`SELECT status FROM nodes WHERE id = ?`, "node-heartbeat").Scan(&status); err != nil {
		t.Fatalf("Scan status: %v", err)
	}
	if status != "ready" {
		t.Fatalf("status = %q, want ready", status)
	}
}

func TestOpenMetadataDBInitializesSchema(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "schema.db")
	db, err := cluster.OpenMetadataDB(dbPath)
	if err != nil {
		t.Fatalf("OpenMetadataDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var name string
	if err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='nodes'`).Scan(&name); err != nil {
		t.Fatalf("query schema: %v", err)
	}
	if name != "nodes" {
		t.Fatalf("nodes table missing")
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
		if err := coord.Close(); err != nil {
			t.Fatalf("Close coordinator: %v", err)
		}
	})

	return coord
}
