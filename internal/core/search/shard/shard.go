package shard

import (
	"plastic-engine-core/internal/core/storage/pebble"
)

// Shard represents a shard already opened locally with Pebble.
type Shard struct {
	ID    string
	Store *pebble.PebbleStore
	Info  Assignment
}
