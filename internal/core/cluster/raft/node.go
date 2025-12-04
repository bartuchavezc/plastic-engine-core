package raft

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"plastic-engine-core/internal/pkg/logger"
)

// Node wraps a Raft instance and provides a higher-level API.
type Node struct {
	raft   *raft.Raft
	fsm    *FSM
	config Config
	log    logger.Logger

	// Stores that need to be closed on shutdown
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	transport   *raft.NetworkTransport
}

// NewNode creates and starts a new Raft node.
// The node will use the provided SQLite database as its state machine backend.
func NewNode(cfg Config, db *sql.DB, log logger.Logger) (*Node, error) {
	if log == nil {
		log = logger.DefaultLogger()
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft data directory: %w", err)
	}

	// Create FSM backed by SQLite
	fsm := NewFSM(db, log)

	// Configure Raft
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftConfig.ElectionTimeout = cfg.ElectionTimeout
	raftConfig.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	raftConfig.CommitTimeout = cfg.CommitTimeout
	raftConfig.SnapshotInterval = cfg.SnapshotInterval
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold

	// Create BoltDB-based log store
	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("create raft log store: %w", err)
	}

	// Create BoltDB-based stable store
	stableStorePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		logStore.Close()
		return nil, fmt.Errorf("create raft stable store: %w", err)
	}

	// Create file-based snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("create raft snapshot store: %w", err)
	}

	// Create TCP transport
	advertiseAddr := cfg.GetAdvertiseAddr()
	addr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("resolve raft address: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("create raft transport: %w", err)
	}

	// Create the Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		transport.Close()
		return nil, fmt.Errorf("create raft node: %w", err)
	}

	node := &Node{
		raft:        r,
		fsm:         fsm,
		config:      cfg,
		log:         log,
		logStore:    logStore,
		stableStore: stableStore,
		transport:   transport,
	}

	// Bootstrap if this is the first node
	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(advertiseAddr),
				},
			},
		}

		future := r.BootstrapCluster(configuration)
		if err := future.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			log.Error("failed to bootstrap raft cluster", logger.Field{Key: "error", Value: err})
		} else if err == nil {
			log.Info("raft cluster bootstrapped", logger.Field{Key: "node_id", Value: cfg.NodeID})
		}
	}

	log.Info("raft node started",
		logger.Field{Key: "node_id", Value: cfg.NodeID},
		logger.Field{Key: "bind_addr", Value: cfg.BindAddr},
		logger.Field{Key: "advertise_addr", Value: advertiseAddr},
		logger.Field{Key: "bootstrap", Value: cfg.Bootstrap},
	)

	return node, nil
}

// Apply sends a command to the Raft cluster for consensus.
// Returns an error if this node is not the leader.
func (n *Node) Apply(data []byte, timeout time.Duration) error {
	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return err
	}

	// Check if the FSM returned an error
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
// Returns an empty string if no leader is known.
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the ID of the current leader.
func (n *Node) LeaderID() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// State returns the current Raft state (Leader, Follower, Candidate).
func (n *Node) State() raft.RaftState {
	return n.raft.State()
}

// WaitForLeader blocks until a leader is elected or the context is cancelled.
func (n *Node) WaitForLeader(ctx context.Context) (string, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			if addr := n.LeaderAddr(); addr != "" {
				return addr, nil
			}
		}
	}
}

// AddVoter adds a new voting member to the Raft cluster.
// Must be called on the leader.
func (n *Node) AddVoter(nodeID, addr string) error {
	future := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	return future.Error()
}

// RemoveServer removes a member from the Raft cluster.
// Must be called on the leader.
func (n *Node) RemoveServer(nodeID string) error {
	future := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
}

// GetConfiguration returns the current Raft cluster configuration.
func (n *Node) GetConfiguration() (raft.Configuration, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return future.Configuration(), nil
}

// Stats returns Raft statistics.
func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

// Shutdown gracefully stops the Raft node.
func (n *Node) Shutdown() error {
	n.log.Info("shutting down raft node")

	// Shutdown Raft
	future := n.raft.Shutdown()
	if err := future.Error(); err != nil {
		n.log.Error("error shutting down raft", logger.Field{Key: "error", Value: err})
	}

	// Close stores
	var errs []error
	if n.transport != nil {
		if err := n.transport.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close transport: %w", err))
		}
	}
	if n.logStore != nil {
		if err := n.logStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close log store: %w", err))
		}
	}
	if n.stableStore != nil {
		if err := n.stableStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close stable store: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}

	return nil
}

// LeaderCh returns a channel that signals leadership changes.
func (n *Node) LeaderCh() <-chan bool {
	return n.raft.LeaderCh()
}

// NodeID returns the configured node ID.
func (n *Node) NodeID() string {
	return n.config.NodeID
}

// DataDir returns the configured data directory.
func (n *Node) DataDir() string {
	return n.config.DataDir
}

