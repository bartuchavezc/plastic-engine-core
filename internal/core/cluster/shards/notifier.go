package shards

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	searchshards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// Notifier handles push notifications to search nodes.
type Notifier struct {
	httpClient *http.Client
	repo       *Repository
	log        logger.Logger
}

// NewNotifier creates a Notifier with the provided HTTP client and repository.
func NewNotifier(httpClient *http.Client, repo *Repository, log logger.Logger) *Notifier {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Notifier{
		httpClient: httpClient,
		repo:       repo,
		log:        log,
	}
}

// shardSyncRequest matches the request format expected by the search node.
type shardSyncRequest struct {
	Assignments []searchshards.Assignment `json:"assignments"`
}

// NotifyNode sends shard assignments to a search node.
// If the notification fails, it logs the error but does not return it,
// since the shard assignment is already persisted and the node will
// receive the assignments on next Join.
func (n *Notifier) NotifyNode(ctx context.Context, nodeID string, assignments []searchshards.Assignment) error {
	if len(assignments) == 0 {
		return nil
	}

	nodeInfo, err := n.repo.LookupNode(ctx, nodeID)
	if err != nil {
		n.log.Error("failed to lookup node for notification",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "error", Value: err},
		)
		return nil // Don't fail, node will sync on next Join
	}

	if nodeInfo.AdvertiseAddr == "" {
		n.log.Warn("node has no advertise address, skipping notification",
			logger.Field{Key: "node_id", Value: nodeID},
		)
		return nil
	}

	reqBody := shardSyncRequest{Assignments: assignments}
	body, err := json.Marshal(reqBody)
	if err != nil {
		n.log.Error("failed to marshal shard sync request",
			logger.Field{Key: "error", Value: err},
		)
		return nil
	}

	url := strings.TrimRight(nodeInfo.AdvertiseAddr, "/") + "/shards/sync"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		n.log.Error("failed to create shard sync request",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "error", Value: err},
		)
		return nil
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(httpReq)
	if err != nil {
		n.log.Warn("failed to notify node of new shards (node may be down)",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "url", Value: url},
			logger.Field{Key: "error", Value: err},
		)
		return nil // Don't fail, node will sync on next Join
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		n.log.Warn("node returned non-OK status for shard sync",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "status", Value: resp.StatusCode},
		)
		return nil
	}

	n.log.Info("notified node of new shards",
		logger.Field{Key: "node_id", Value: nodeID},
		logger.Field{Key: "count", Value: len(assignments)},
	)

	return nil
}

// indexDeletedRequest matches the request format expected by the search node.
type indexDeletedRequest struct {
	IndexID string `json:"index_id"`
}

// NotifyIndexDeleted notifies a search node that an index has been deleted.
// If the notification fails, it logs the error but does not return it,
// since the delete is already persisted and the node will detect the
// missing index on next heartbeat.
func (n *Notifier) NotifyIndexDeleted(ctx context.Context, nodeID, indexID string) error {
	nodeInfo, err := n.repo.LookupNode(ctx, nodeID)
	if err != nil {
		n.log.Error("failed to lookup node for delete notification",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "error", Value: err},
		)
		return nil
	}

	if nodeInfo.AdvertiseAddr == "" {
		n.log.Warn("node has no advertise address, skipping delete notification",
			logger.Field{Key: "node_id", Value: nodeID},
		)
		return nil
	}

	reqBody := indexDeletedRequest{IndexID: indexID}
	body, err := json.Marshal(reqBody)
	if err != nil {
		n.log.Error("failed to marshal index deleted request",
			logger.Field{Key: "error", Value: err},
		)
		return nil
	}

	url := strings.TrimRight(nodeInfo.AdvertiseAddr, "/") + "/indexes/deleted"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		n.log.Error("failed to create index deleted request",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "error", Value: err},
		)
		return nil
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(httpReq)
	if err != nil {
		n.log.Warn("failed to notify node of deleted index (node may be down)",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "index_id", Value: indexID},
			logger.Field{Key: "url", Value: url},
			logger.Field{Key: "error", Value: err},
		)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		n.log.Warn("node returned non-OK status for index deleted",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "status", Value: resp.StatusCode},
		)
		return nil
	}

	n.log.Info("notified node of deleted index",
		logger.Field{Key: "node_id", Value: nodeID},
		logger.Field{Key: "index_id", Value: indexID},
	)

	return nil
}

// GetNodesWithShardsForIndex returns node IDs that have shards for the given index.
func (n *Notifier) GetNodesWithShardsForIndex(ctx context.Context, indexID string) ([]string, error) {
	shards, err := n.repo.List(ctx, ShardFilter{IndexID: indexID})
	if err != nil {
		return nil, fmt.Errorf("list shards for index: %w", err)
	}

	nodeSet := make(map[string]struct{})
	for _, shard := range shards {
		if shard.PrimaryNode != "" {
			nodeSet[shard.PrimaryNode] = struct{}{}
		}
	}

	nodes := make([]string, 0, len(nodeSet))
	for nodeID := range nodeSet {
		nodes = append(nodes, nodeID)
	}

	return nodes, nil
}
