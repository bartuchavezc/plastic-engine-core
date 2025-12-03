package shards

import (
	"plastic-engine-core/internal/adapters/storage/pebble"
)

// Shard represents a shard already opened locally with Pebble.
type Shard struct {
	ID    string
	Store *pebble.PebbleStore
	Info  Assignment
}
