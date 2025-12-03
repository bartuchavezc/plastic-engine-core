package indexes

import "time"

// ShardState enumerates lifecycle states for shards owned by an index.
type ShardState string

const (
	// ShardStatePending indicates the shard exists but is not fully assigned.
	ShardStatePending ShardState = "pending"
	// ShardStateAssigned marks the shard as having an active primary node.
	ShardStateAssigned ShardState = "assigned"
)

// ShardRecord represents a shard persisted in the coordinator metadata store.
type ShardRecord struct {
	ID           string
	IndexID      string
	Key          string
	State        ShardState
	PrimaryNode  string
	Version      int
	CreatedAt    time.Time
	LastModified time.Time
}

// NewShard captures the information required to persist a shard.
type NewShard struct {
	IndexID string
	Key     string
}

