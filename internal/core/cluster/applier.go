package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// CommandType identifies the type of state mutation command.
type CommandType uint8

const (
	// CmdUnknown represents an unknown or invalid command.
	CmdUnknown CommandType = iota
	// CmdCreateIndex creates a new index definition.
	CmdCreateIndex
	// CmdUpdateIndex updates an existing index definition.
	CmdUpdateIndex
	// CmdDeleteIndex removes an index definition.
	CmdDeleteIndex
	// CmdRegisterNode registers a new node in the cluster.
	CmdRegisterNode
	// CmdUpdateHeartbeat updates a node's heartbeat timestamp.
	CmdUpdateHeartbeat
	// CmdMarkNodeUnreachable marks a node as unreachable.
	CmdMarkNodeUnreachable
	// CmdCreateShards creates new shard records.
	CmdCreateShards
	// CmdAssignShard assigns a shard to a node.
	CmdAssignShard
	// CmdUpdateShardState updates the state of a shard.
	CmdUpdateShardState
)

// String returns a human-readable name for the command type.
func (c CommandType) String() string {
	switch c {
	case CmdCreateIndex:
		return "CreateIndex"
	case CmdUpdateIndex:
		return "UpdateIndex"
	case CmdDeleteIndex:
		return "DeleteIndex"
	case CmdRegisterNode:
		return "RegisterNode"
	case CmdUpdateHeartbeat:
		return "UpdateHeartbeat"
	case CmdMarkNodeUnreachable:
		return "MarkNodeUnreachable"
	case CmdCreateShards:
		return "CreateShards"
	case CmdAssignShard:
		return "AssignShard"
	case CmdUpdateShardState:
		return "UpdateShardState"
	default:
		return "Unknown"
	}
}

// Command represents a state mutation that can be applied to the cluster.
// Commands are serializable for replication via Raft.
type Command struct {
	Type      CommandType     `json:"type"`
	Timestamp time.Time       `json:"ts"`
	Payload   json.RawMessage `json:"payload"`
}

// NewCommand creates a new command with the given type and payload.
// The payload is serialized to JSON.
func NewCommand(cmdType CommandType, payload any) (Command, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Command{}, err
	}
	return Command{
		Type:      cmdType,
		Timestamp: time.Now().UTC(),
		Payload:   data,
	}, nil
}

// Unmarshal deserializes the command payload into the provided target.
func (c *Command) Unmarshal(target any) error {
	return json.Unmarshal(c.Payload, target)
}

// StateApplier defines how state mutations are applied to the cluster.
// The implementation determines whether changes go directly to SQLite
// (standalone mode) or through Raft consensus (HA mode).
type StateApplier interface {
	// Apply executes a command that modifies cluster state.
	// In standalone mode, this writes directly to SQLite.
	// In HA mode, this goes through Raft consensus first.
	Apply(ctx context.Context, cmd Command) error

	// IsLeader returns true if this node can accept write operations.
	// In standalone mode, this always returns true.
	// In HA mode, this returns true only for the Raft leader.
	IsLeader() bool

	// LeaderAddr returns the address of the current leader.
	// In standalone mode, this returns an empty string.
	// In HA mode, this returns the advertised address of the Raft leader.
	LeaderAddr() string

	// WaitForLeader blocks until a leader is elected or the context is cancelled.
	// Returns the leader address or an error if no leader is found.
	WaitForLeader(ctx context.Context) (string, error)
}

// Common errors for StateApplier implementations.
var (
	// ErrNotLeader is returned when a write is attempted on a non-leader node.
	ErrNotLeader = errors.New("not the leader")

	// ErrNoLeader is returned when no leader is available in the cluster.
	ErrNoLeader = errors.New("no leader available")

	// ErrApplyTimeout is returned when a command application times out.
	ErrApplyTimeout = errors.New("apply timeout")

	// ErrUnknownCommand is returned for unrecognized command types.
	ErrUnknownCommand = errors.New("unknown command type")
)

// NotLeaderError wraps ErrNotLeader with the current leader's address.
type NotLeaderError struct {
	Leader string
}

func (e NotLeaderError) Error() string {
	if e.Leader != "" {
		return "not the leader; leader is at " + e.Leader
	}
	return "not the leader; leader unknown"
}

func (e NotLeaderError) Unwrap() error {
	return ErrNotLeader
}

// Command payloads for different operations.
// These structs are serialized as the Command.Payload.

// CreateIndexPayload contains data for creating a new index.
type CreateIndexPayload struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	ShardStrategy    string          `json:"shard_strategy"`
	ShardTemplate    string          `json:"shard_template,omitempty"`
	ShardConfig      json.RawMessage `json:"shard_config,omitempty"`
	DefaultAnalyzer  string          `json:"default_analyzer"`
	DefaultTokenizer string          `json:"default_tokenizer"`
	MappingVersion   int             `json:"mapping_version"`
	FieldMappings    json.RawMessage `json:"field_mappings,omitempty"`
	RefreshTime      time.Duration   `json:"refresh_time,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

// RegisterNodePayload contains data for registering a node.
type RegisterNodePayload struct {
	NodeID        string `json:"node_id"`
	Role          string `json:"role"`
	AdvertiseAddr string `json:"advertise_addr"`
	DataDir       string `json:"data_dir,omitempty"`
}

// UpdateHeartbeatPayload contains data for updating a node's heartbeat.
type UpdateHeartbeatPayload struct {
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
}

// MarkNodeUnreachablePayload contains data for marking nodes unreachable.
type MarkNodeUnreachablePayload struct {
	Timeout time.Duration `json:"timeout"`
}

// CreateShardsPayload contains data for creating shards.
type CreateShardsPayload struct {
	IndexID string   `json:"index_id"`
	Keys    []string `json:"keys"`
}

// AssignShardPayload contains data for assigning a shard to a node.
type AssignShardPayload struct {
	ShardID string `json:"shard_id"`
	NodeID  string `json:"node_id"`
}

// UpdateShardStatePayload contains data for updating shard state.
type UpdateShardStatePayload struct {
	ShardID string `json:"shard_id"`
	State   string `json:"state"`
}

// DeleteIndexPayload contains data for deleting an index.
type DeleteIndexPayload struct {
	ID string `json:"id"`
}

