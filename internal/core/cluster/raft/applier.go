package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"plastic-engine-core/internal/core/cluster"
	"plastic-engine-core/internal/pkg/logger"
)

// Applier implements cluster.StateApplier using Raft consensus.
// All state mutations go through Raft before being applied to SQLite.
type Applier struct {
	node    *Node
	timeout time.Duration
	log     logger.Logger
}

// ApplierConfig holds configuration for the Raft applier.
type ApplierConfig struct {
	// Timeout is the maximum time to wait for a command to be applied.
	// Defaults to 5 seconds.
	Timeout time.Duration
}

// DefaultApplierConfig returns default applier configuration.
func DefaultApplierConfig() ApplierConfig {
	return ApplierConfig{
		Timeout: 5 * time.Second,
	}
}

// NewApplier creates a new Raft-based state applier.
func NewApplier(node *Node, cfg ApplierConfig, log logger.Logger) *Applier {
	if log == nil {
		log = logger.DefaultLogger()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &Applier{
		node:    node,
		timeout: cfg.Timeout,
		log:     log,
	}
}

// Apply sends a command through Raft consensus.
// Returns NotLeaderError if this node is not the leader.
func (a *Applier) Apply(ctx context.Context, cmd cluster.Command) error {
	if !a.node.IsLeader() {
		return cluster.NotLeaderError{Leader: a.node.LeaderAddr()}
	}

	a.log.Debug("applying command via raft",
		logger.Field{Key: "type", Value: cmd.Type.String()},
		logger.Field{Key: "timestamp", Value: cmd.Timestamp},
	)

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal raft command: %w", err)
	}

	// Determine timeout from context or use default
	timeout := a.timeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	if err := a.node.Apply(data, timeout); err != nil {
		// Check if we lost leadership during apply
		if !a.node.IsLeader() {
			return cluster.NotLeaderError{Leader: a.node.LeaderAddr()}
		}
		return fmt.Errorf("raft apply: %w", err)
	}

	return nil
}

// IsLeader returns true if this node is the current Raft leader.
func (a *Applier) IsLeader() bool {
	return a.node.IsLeader()
}

// LeaderAddr returns the address of the current leader.
func (a *Applier) LeaderAddr() string {
	return a.node.LeaderAddr()
}

// WaitForLeader blocks until a leader is elected or the context is cancelled.
func (a *Applier) WaitForLeader(ctx context.Context) (string, error) {
	return a.node.WaitForLeader(ctx)
}

// Node returns the underlying Raft node for advanced operations.
func (a *Applier) Node() *Node {
	return a.node
}

// Ensure Applier implements StateApplier
var _ cluster.StateApplier = (*Applier)(nil)

