package shards

import (
	"plastic-engine-core/internal/core/search/segment"
)

// Shard represents a shard with its segment-based index.
type Shard struct {
	ID       string
	Segments *segment.Manager
	Info     Assignment
}
