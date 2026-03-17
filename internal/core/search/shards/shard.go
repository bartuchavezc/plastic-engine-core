package shards

import (
	"plastic-engine-core/internal/core/search/indexstore"
)

// Shard represents a shard with its segment-based index.
type Shard struct {
	ID       string
	Segments *indexstore.Manager
	Info     Assignment
}
