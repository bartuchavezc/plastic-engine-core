package nodes

import (
	"time"

	searchshards "plastic-engine-core/internal/core/search/shards"
)

// NodeRecord represents a row in the nodes table.
type NodeRecord struct {
	ID            string
	Role          string
	AdvertiseAddr string
	DataDir       string
	LastHeartbeat time.Time
	Status        string
}

// JoinRequest captures the information a node provides when joining the cluster.
type JoinRequest struct {
	NodeID        string
	Role          string
	AdvertiseAddr string
	DataDir       string
}

// JoinResponse details the response to a join request.
type JoinResponse struct {
	NodeID string
	Shards []searchshards.Assignment
}

// HeartbeatRequest captures heartbeat data reported by a cluster node.
type HeartbeatRequest struct {
	NodeID string   `json:"node_id"`
	Shards []string `json:"shards"`
}

// HeartbeatResponse contains information returned to the node after a heartbeat.
type HeartbeatResponse struct {
	Status string `json:"status"`
	// MappingUpdates maps index IDs to their current mapping versions.
	// Nodes should compare with their local versions and fetch updated mappings if needed.
	MappingUpdates map[string]int `json:"mapping_updates,omitempty"`
}

