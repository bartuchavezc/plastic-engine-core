package node_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	node "plastic-engine-core/internal/core/search"
	"plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

func TestNodeInitialisePersistsNodeID(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	manager := shards.NewManager(tempDir)

	fakeClient := &stubClusterClient{
		joinResp: node.JoinResponse{
			NodeID: "node-123",
		},
	}

	n := node.New(
		node.NodeInfo{Role: "search", DataDir: tempDir},
		"http://localhost:8080",
		fakeClient,
		manager,
		logger.DefaultLogger(),
	)

	if err := n.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if got := n.Info.ID; got != "node-123" {
		t.Fatalf("node.Info.ID = %q, want %q", got, "node-123")
	}

	data, err := os.ReadFile(filepath.Join(tempDir, "node_id"))
	if err != nil {
		t.Fatalf("ReadFile node_id: %v", err)
	}
	if got := string(data); got != "node-123" {
		t.Fatalf("persisted node id = %q, want %q", got, "node-123")
	}
}

func TestNodeStartHeartbeatStopsWithContext(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	manager := shards.NewManager(tempDir)

	fakeClient := &stubClusterClient{
		joinResp: node.JoinResponse{NodeID: "node-abc"},
	}

	n := node.New(
		node.NodeInfo{Role: "search", DataDir: tempDir},
		"http://localhost:8080",
		fakeClient,
		manager,
		logger.DefaultLogger(),
	)

	if err := n.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n.StartHeartbeat(ctx, 10*time.Millisecond)

	if err := waitForCalls(fakeClient, 1, 200*time.Millisecond); err != nil {
		t.Fatalf("waitForCalls: %v", err)
	}

	cancel()
	if err := waitForStop(fakeClient, 200*time.Millisecond); err != nil {
		t.Fatalf("waitForStop: %v", err)
	}
}

type stubClusterClient struct {
	mu             sync.Mutex
	joinResp       node.JoinResponse
	joinErr        error
	heartbeatErr   error
	heartbeatCalls []node.HeartbeatReport
	stopped        bool
}

func (s *stubClusterClient) Join(ctx context.Context, info node.NodeInfo) (node.JoinResponse, error) {
	return s.joinResp, s.joinErr
}

func (s *stubClusterClient) Heartbeat(ctx context.Context, report node.HeartbeatReport) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatCalls = append(s.heartbeatCalls, report)
	return s.heartbeatErr
}

func waitForCalls(client *stubClusterClient, minCalls int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		count := len(client.heartbeatCalls)
		client.mu.Unlock()
		if count >= minCalls {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func waitForStop(client *stubClusterClient, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	client.mu.Lock()
	lastCount := len(client.heartbeatCalls)
	client.mu.Unlock()

	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)

		client.mu.Lock()
		current := len(client.heartbeatCalls)
		client.mu.Unlock()

		if current == lastCount {
			return nil
		}
		lastCount = current
	}

	return fmt.Errorf("heartbeats still arriving after context cancellation")
}
