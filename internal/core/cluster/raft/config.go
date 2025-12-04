package raft

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds the configuration for a Raft node.
type Config struct {
	// NodeID is the unique identifier for this node in the Raft cluster.
	// If empty, defaults to the hostname.
	NodeID string

	// BindAddr is the address to bind the Raft transport to (e.g., "0.0.0.0:7000").
	BindAddr string

	// AdvertiseAddr is the address advertised to other nodes (e.g., "192.168.1.10:7000").
	// If empty, defaults to BindAddr.
	AdvertiseAddr string

	// DataDir is the directory where Raft stores its logs and snapshots.
	DataDir string

	// Peers is a list of initial peer addresses (e.g., ["host1:7000", "host2:7000"]).
	// Used when joining an existing cluster.
	Peers []string

	// Bootstrap indicates whether this node should bootstrap a new cluster.
	// Should be true only for the first node in a new cluster.
	Bootstrap bool

	// SnapshotInterval is how often to check if a snapshot should be taken.
	SnapshotInterval time.Duration

	// SnapshotThreshold is the number of log entries between snapshots.
	SnapshotThreshold uint64

	// HeartbeatTimeout is the time without a leader heartbeat before starting an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout is the time to wait for election to complete.
	ElectionTimeout time.Duration

	// LeaderLeaseTimeout is the time a leader will wait to verify leadership.
	LeaderLeaseTimeout time.Duration

	// CommitTimeout is the time to wait for a commit to complete.
	CommitTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
// Override specific values as needed.
func DefaultConfig() Config {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "coordinator"
	}

	return Config{
		NodeID:             hostname,
		BindAddr:           "0.0.0.0:7000",
		AdvertiseAddr:      "",
		DataDir:            "./raft-data",
		Peers:              nil,
		Bootstrap:          false,
		SnapshotInterval:   30 * time.Second,
		SnapshotThreshold:  8192,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
	}
}

// ConfigFromEnv creates a Config from environment variables.
// Environment variables take precedence over defaults.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()

	if nodeID := os.Getenv("RAFT_NODE_ID"); nodeID != "" {
		cfg.NodeID = nodeID
	}

	if port := os.Getenv("RAFT_PORT"); port != "" {
		cfg.BindAddr = fmt.Sprintf("0.0.0.0:%s", port)
	}

	if advertise := os.Getenv("RAFT_ADVERTISE_ADDR"); advertise != "" {
		cfg.AdvertiseAddr = advertise
	}

	if dataDir := os.Getenv("RAFT_DATA_DIR"); dataDir != "" {
		cfg.DataDir = dataDir
	}

	if peers := os.Getenv("RAFT_PEERS"); peers != "" {
		cfg.Peers = parsePeers(peers)
	}

	if bootstrap := os.Getenv("RAFT_BOOTSTRAP"); bootstrap == "true" || bootstrap == "1" {
		cfg.Bootstrap = true
	}

	return cfg
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("raft: node_id is required")
	}
	if c.BindAddr == "" {
		return fmt.Errorf("raft: bind_addr is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("raft: data_dir is required")
	}
	return nil
}

// GetAdvertiseAddr returns the advertise address, defaulting to BindAddr if not set.
func (c *Config) GetAdvertiseAddr() string {
	if c.AdvertiseAddr != "" {
		return c.AdvertiseAddr
	}
	return c.BindAddr
}

func parsePeers(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	peers := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			peers = append(peers, p)
		}
	}
	return peers
}

