package shards

import (
	"errors"
	"time"
)

const DefaultTimestampFormat = "2006-01-02 15:04:05"

// ShardRecord represents a shard in the metadata store.
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

// ShardFilter allows narrowing shard queries when listing.
type ShardFilter struct {
	IndexID string
	NodeID  string
	State   string
}

// Info represents metadata for a shard lookup.
type Info struct {
	ID          string
	PrimaryNode string
}

// NodeInfo represents node information for routing.
type NodeInfo struct {
	ID            string
	AdvertiseAddr string
}

// ErrPrimaryNotFound indicates that the shard has no primary assignment.
var ErrPrimaryNotFound = errors.New("primary shard not found")

