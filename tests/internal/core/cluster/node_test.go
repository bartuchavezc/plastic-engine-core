package cluster_test

import (
	"testing"

	cluster "plastic-engine-core/internal/core/cluster"
)

func TestNewNode(t *testing.T) {
	node := cluster.NewNode("search", "8080", "coordinator:9000")
	if node.Role != "search" {
		t.Fatalf("Role = %q, want search", node.Role)
	}
	if node.Port != "8080" {
		t.Fatalf("Port = %q, want 8080", node.Port)
	}
	if node.JoinAddress != "coordinator:9000" {
		t.Fatalf("JoinAddress = %q, want coordinator:9000", node.JoinAddress)
	}
}
