package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	shardspkg "plastic-engine-core/internal/core/cluster/shards"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

const defaultShardAssignmentBatch = 32

func (c *Coordinator) planInitialShards(ctx context.Context, def indexes.IndexDefinition) error {
	if c == nil || c.db == nil {
		return fmt.Errorf("coordinator not initialised")
	}

	planner, err := c.shardPlannerFactory.Get(def.ShardStrategy)
	if err != nil {
		return err
	}

	specs, err := planner.PlanInitialShards(ctx, def, nil)
	if err != nil {
		return err
	}

	if len(specs) == 0 {
		return nil
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin shard planning tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC().Format(shardspkg.DefaultTimestampFormat)

	for _, spec := range specs {
		shardID := fmt.Sprintf("%s-%s", def.ID, spec.Key)
		if err := shardspkg.InsertShard(ctx, tx, shardID, def.ID, spec.Key, now); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit shard planning tx: %w", err)
	}

	c.Logger().Info("planned initial shards",
		logger.Field{Key: "index_id", Value: def.ID},
		logger.Field{Key: "shards", Value: len(specs)},
	)

	return nil
}

func (c *Coordinator) assignShardsToNode(ctx context.Context, nodeID string, limit int) ([]shards.Assignment, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("coordinator not initialised")
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin assign shards tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	assignments, err := shardspkg.AssignShardsToNodeTx(ctx, tx, nodeID, limit)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit assign shards tx: %w", err)
	}

	return assignments, nil
}

func assignShardsToNodeTx(ctx context.Context, tx *sql.Tx, nodeID string, limit int) ([]shards.Assignment, error) {
	return shardspkg.AssignShardsToNodeTx(ctx, tx, nodeID, limit)
}

func loadAssignmentsForNodeTx(ctx context.Context, tx *sql.Tx, nodeID string) ([]shards.Assignment, error) {
	return shardspkg.LoadAssignmentsForNodeTx(ctx, tx, nodeID)
}

func (c *Coordinator) assignPendingShardsToReadyNodes(ctx context.Context) error {
	nodes, err := c.listEligibleNodes(ctx)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		return nil
	}

	for _, node := range nodes {
		assignments, err := c.assignShardsToNode(ctx, node.ID, defaultShardAssignmentBatch)
		if err != nil {
			return err
		}

		if len(assignments) > 0 {
			c.Logger().Info("assigned shards to node",
				logger.Field{Key: "node_id", Value: node.ID},
				logger.Field{Key: "count", Value: len(assignments)},
			)
		}
	}

	return nil
}

func (c *Coordinator) listEligibleNodes(ctx context.Context) ([]NodeRecord, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT id, role, advertise_addr, data_dir, status
		FROM nodes
		WHERE role = 'search' AND status IN ('ready', 'joining')`)
	if err != nil {
		return nil, fmt.Errorf("query eligible nodes: %w", err)
	}
	defer rows.Close()

	var nodes []NodeRecord
	for rows.Next() {
		var node NodeRecord
		if err := rows.Scan(&node.ID, &node.Role, &node.AdvertiseAddr, &node.DataDir, &node.Status); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		nodes = append(nodes, node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate nodes: %w", err)
	}

	return nodes, nil
}
