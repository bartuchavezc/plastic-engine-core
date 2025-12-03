package coordinator

import (
	"time"
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

// ShardRecord represents a row in the shards table.
type ShardRecord struct {
	ID          string
	IndexID     string
	ShardKey    string
	PrimaryNode string
	State       string
	Version     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ShardReplicaRecord links nodes to shard replicas.
type ShardReplicaRecord struct {
	ShardID  string
	NodeID   string
	State    string
	LastSync time.Time
}
