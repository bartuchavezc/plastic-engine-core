package search_test

import (
	"context"
	"path/filepath"
	"testing"

	coordinator "plastic-engine-core/internal/core/cluster/coordinator"
	searchsvc "plastic-engine-core/internal/core/cluster/search"
)

func TestJoinServiceJoinAndHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("integration-style test")
	}

	coord := newTestCoordinator(t)
	service := searchsvc.NewJoinService(coord)
	ctx := context.Background()

	resp, err := service.Join(ctx, coordinator.JoinRequest{
		Role:          "search",
		AdvertiseAddr: "http://127.0.0.1:0",
		DataDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if resp.NodeID == "" {
		t.Fatalf("expected node id in response")
	}

	if err := service.Heartbeat(ctx, coordinator.HeartbeatRequest{NodeID: resp.NodeID}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
}

func newTestCoordinator(t *testing.T) *coordinator.Coordinator {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "search_join.db")
	coord, err := coordinator.NewCoordinator("coordinator", "0", dbPath)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	t.Cleanup(func() {
		_ = coord.Close()
	})
	return coord
}
