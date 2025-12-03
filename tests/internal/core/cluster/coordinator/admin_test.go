package coordinator_test

import (
	"context"
	"testing"

	"plastic-engine-core/internal/core/cluster/coordinator"
	coreindex "plastic-engine-core/internal/core/index"
)

func TestListShardsFiltersByIndex(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	_, err := coord.CreateIndex(ctx, coreindex.CreateIndexRequest{
		ID:               "idx-admin-test",
		Name:             "admin-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []coreindex.FieldMapping{
			{Name: "name", Type: coreindex.FieldTypeKeyword, Indexed: true},
		},
		ShardConfig: coreindex.ShardConfig{
			Strategy: coreindex.ShardStrategyAutomatic,
			Automatic: &coreindex.AutomaticShardConfig{
				ShardCount: 1,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	shards, err := coord.ListShards(ctx, coordinator.ShardFilter{IndexID: "idx-admin-test"})
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	if len(shards) != 1 {
		t.Fatalf("shards len = %d, want 1", len(shards))
	}
	if shards[0].IndexID != "idx-admin-test" {
		t.Fatalf("shard IndexID = %q, want %q", shards[0].IndexID, "idx-admin-test")
	}
}

func TestListNodesReturnsHeartbeat(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	_, err := coord.Join(ctx, coordinator.JoinRequest{
		NodeID:        "node-admin-test",
		Role:          "search",
		AdvertiseAddr: "http://127.0.0.1:0",
		DataDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if err := coord.Heartbeat(ctx, coordinator.HeartbeatRequest{NodeID: "node-admin-test"}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	nodes, err := coord.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes))
	}
	if nodes[0].LastHeartbeat.IsZero() {
		t.Fatalf("expected last heartbeat to be set")
	}
}
