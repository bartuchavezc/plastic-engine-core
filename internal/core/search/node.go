package search

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// ClusterClient abstracts the coordinator API the search node depends on.
type ClusterClient interface {
	Join(ctx context.Context, info NodeInfo) (JoinResponse, error)
	Heartbeat(ctx context.Context, report HeartbeatReport) (HeartbeatResponse, error)
}

// NodeInfo captures how this node identifies itself to the cluster.
type NodeInfo struct {
	ID            string
	Role          string
	AdvertiseAddr string
	DataDir       string
}

// JoinResponse contains information returned by the coordinator when joining.
type JoinResponse struct {
	NodeID string
	Shards []shards.Assignment
}

// HeartbeatReport represents the state snapshot reported back to the cluster.
type HeartbeatReport struct {
	NodeID string
	Shards []string
}

// HeartbeatResponse contains information returned from the coordinator after a heartbeat.
type HeartbeatResponse struct {
	Status string
	// MappingUpdates maps index IDs to their current mapping versions.
	// Nodes should compare with their local versions and fetch updated mappings if needed.
	MappingUpdates map[string]int
}

// MappingRefresher handles refreshing index mappings when updates are detected.
type MappingRefresher interface {
	RefreshMapping(ctx context.Context, indexID string) error
	GetLocalVersion(indexID string) int
}

// SearchNode encapsulates the behaviour of a search role node.
type SearchNode struct {
	Info             NodeInfo
	JoinAddress      string
	ClusterClient    ClusterClient
	ShardManager     *shards.Manager
	MappingRefresher MappingRefresher // Optional: handles mapping updates
	Logger           logger.Logger
}

// New instantiates a SearchNode ready to initialise.
func New(info NodeInfo, joinAddr string, client ClusterClient, manager *shards.Manager, logger logger.Logger) *SearchNode {
	return &SearchNode{
		Info:          info,
		JoinAddress:   joinAddr,
		ClusterClient: client,
		ShardManager:  manager,
		Logger:        logger,
	}
}

// NewWithMappingRefresher creates a SearchNode with mapping refresh capability.
func NewWithMappingRefresher(info NodeInfo, joinAddr string, client ClusterClient, manager *shards.Manager, mappingRefresher MappingRefresher, logger logger.Logger) *SearchNode {
	return &SearchNode{
		Info:             info,
		JoinAddress:      joinAddr,
		ClusterClient:    client,
		ShardManager:     manager,
		MappingRefresher: mappingRefresher,
		Logger:           logger,
	}
}

// Initialize performs the join flow and syncs assigned shards.
func (n *SearchNode) Initialize() error {
	ctx := context.Background()

	if n.Info.DataDir != "" {
		if err := os.MkdirAll(n.Info.DataDir, 0o755); err != nil {
			return err
		}
		if n.Info.ID == "" {
			if diskID, err := n.loadNodeID(); err != nil {
				n.Logger.Error("failed to load node id from disk", logger.Field{Key: "error", Value: err})
			} else if diskID != "" {
				n.Info.ID = diskID
				n.Logger.Info("loaded node id from disk", logger.Field{Key: "node_id", Value: diskID})
			}
		}
	}

	resp, err := n.ClusterClient.Join(ctx, n.Info)
	if err != nil {
		return err
	}

	if resp.NodeID != "" {
		n.Info.ID = resp.NodeID
	}

	if err := n.persistNodeID(resp.NodeID); err != nil {
		n.Logger.Error("failed to persist node id", logger.Field{Key: "error", Value: err})
	}

	return n.ShardManager.Sync(resp.Shards)
}

// StartHeartbeat begins sending periodic heartbeats until the context is cancelled.
func (n *SearchNode) StartHeartbeat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				n.Logger.Info("heartbeat loop stopped")
				return
			case <-ticker.C:
				report := HeartbeatReport{
					NodeID: n.Info.ID,
					Shards: n.ShardManager.ListShardIDs(),
				}

				resp, err := n.ClusterClient.Heartbeat(ctx, report)
				if err != nil {
					n.Logger.Error("heartbeat failed", logger.Field{Key: "error", Value: err})
					continue
				}

				// Process mapping updates if available
				if len(resp.MappingUpdates) > 0 {
					n.processMappingUpdates(ctx, resp.MappingUpdates)
				}
			}
		}
	}()
}

// processMappingUpdates checks for mapping version changes and triggers refreshes.
func (n *SearchNode) processMappingUpdates(ctx context.Context, updates map[string]int) {
	if n.MappingRefresher == nil {
		return
	}

	for indexID, newVersion := range updates {
		localVersion := n.MappingRefresher.GetLocalVersion(indexID)
		if newVersion > localVersion {
			n.Logger.Info("mapping update detected",
				logger.Field{Key: "index_id", Value: indexID},
				logger.Field{Key: "local_version", Value: localVersion},
				logger.Field{Key: "new_version", Value: newVersion},
			)

			if err := n.MappingRefresher.RefreshMapping(ctx, indexID); err != nil {
				n.Logger.Error("failed to refresh mapping",
					logger.Field{Key: "index_id", Value: indexID},
					logger.Field{Key: "error", Value: err},
				)
			}
		}
	}
}

func (n *SearchNode) nodeIDFilePath() string {
	if n.Info.DataDir == "" {
		return ""
	}
	return filepath.Join(n.Info.DataDir, "node_id")
}

func (n *SearchNode) loadNodeID() (string, error) {
	path := n.nodeIDFilePath()
	if path == "" {
		return "", nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}

func (n *SearchNode) persistNodeID(id string) error {
	path := n.nodeIDFilePath()
	if path == "" || id == "" {
		return nil
	}

	return os.WriteFile(path, []byte(id), 0o644)
}
