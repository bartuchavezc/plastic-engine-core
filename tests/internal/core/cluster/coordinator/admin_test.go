package coordinator_test

import (
	"context"
	"testing"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/nodes"
	"plastic-engine-core/internal/core/cluster/shards"
)

func TestListShardsFiltersByIndex(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	_, err := coord.CreateIndex(ctx, indexes.CreateIndexRequest{
		ID:               "idx-admin-test",
		Name:             "admin-test",
		DefaultAnalyzer:  "simple",
		DefaultTokenizer: "whitespace",
		FieldMappings: []indexes.FieldMapping{
			{Name: "name", Type: indexes.FieldTypeKeyword, Indexed: true},
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

	shardList, err := coord.ListShards(ctx, shards.ShardFilter{IndexID: "idx-admin-test"})
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	if len(shardList) != 1 {
		t.Fatalf("shards len = %d, want 1", len(shardList))
	}
	if shardList[0].IndexID != "idx-admin-test" {
		t.Fatalf("shard IndexID = %q, want %q", shardList[0].IndexID, "idx-admin-test")
	}
}

func TestListNodesReturnsHeartbeat(t *testing.T) {
	t.Parallel()

	coord := newTestCoordinator(t)
	ctx := context.Background()

	_, err := coord.NodesService().Join(ctx, nodes.JoinRequest{
		NodeID:        "node-admin-test",
		Role:          "search",
		AdvertiseAddr: "http://127.0.0.1:0",
		DataDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if err := coord.NodesService().Heartbeat(ctx, nodes.HeartbeatRequest{NodeID: "node-admin-test"}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	nodeList, err := coord.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodeList) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodeList))
	}
	if nodeList[0].LastHeartbeat.IsZero() {
		t.Fatalf("expected last heartbeat to be set")
	}
}
