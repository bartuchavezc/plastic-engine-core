package documents

import (
	"encoding/json"
	"errors"
)

// Request represents an ingestion command accepted by the cluster.
type Request struct {
	IndexID    string
	DocumentID string
	Routing    map[string]string
	Payload    json.RawMessage
}

// ErrShardNotFound signals the coordinator could not route the document.
var ErrShardNotFound = errors.New("shard not found for routing metadata")

