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

