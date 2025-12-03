package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/helpers"
)

// ShardFilter allows narrowing shard queries when listing metadata.
type ShardFilter struct {
	IndexID string
	NodeID  string
	State   string
}

// ListNodes returns all nodes registered in the metadata store.
func (c *Coordinator) ListNodes(ctx context.Context) ([]NodeRecord, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("coordinator not initialised")
	}

	rows, err := c.db.QueryContext(ctx, `SELECT
		id, role, advertise_addr, data_dir, last_heartbeat, status
		FROM nodes
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query nodes: %w", err)
	}
	defer rows.Close()

	var nodes []NodeRecord
	for rows.Next() {
		var (
			record NodeRecord
			hb     sql.NullTime
		)

		if err := rows.Scan(
			&record.ID,
			&record.Role,
			&record.AdvertiseAddr,
			&record.DataDir,
			&hb,
			&record.Status,
		); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}

		if hb.Valid {
			record.LastHeartbeat = hb.Time.UTC()
		}

		nodes = append(nodes, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate nodes: %w", err)
	}

	return nodes, nil
}

// ListShards returns shard metadata filtered by the provided filter.
func (c *Coordinator) ListShards(ctx context.Context, filter ShardFilter) ([]ShardRecord, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("coordinator not initialised")
	}

	query := `SELECT
		id, index_id, shard_key, primary_node, state, version, created_at, updated_at
		FROM shards`

	var (
		conditions []string
		args       []any
	)

	if strings.TrimSpace(filter.IndexID) != "" {
		conditions = append(conditions, "index_id = ?")
		args = append(args, filter.IndexID)
	}
	if strings.TrimSpace(filter.NodeID) != "" {
		conditions = append(conditions, "primary_node = ?")
		args = append(args, filter.NodeID)
	}
	if strings.TrimSpace(filter.State) != "" {
		conditions = append(conditions, "state = ?")
		args = append(args, filter.State)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY index_id, shard_key"

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query shards: %w", err)
	}
	defer rows.Close()

	var shards []ShardRecord
	for rows.Next() {
		var (
			record      ShardRecord
			primaryNode sql.NullString
			updated     sql.NullTime
		)
		if err := rows.Scan(
			&record.ID,
			&record.IndexID,
			&record.ShardKey,
			&primaryNode,
			&record.State,
			&record.Version,
			&record.CreatedAt,
			&updated,
		); err != nil {
			return nil, fmt.Errorf("scan shard: %w", err)
		}
		if primaryNode.Valid {
			record.PrimaryNode = primaryNode.String
		}
		if updated.Valid {
			record.UpdatedAt = updated.Time.UTC()
		} else {
			record.UpdatedAt = record.CreatedAt
		}
		shards = append(shards, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate shards: %w", err)
	}

	return shards, nil
}

// ListIndexes returns all index definitions registered in the coordinator.
func (c *Coordinator) ListIndexes(ctx context.Context) ([]coreindex.IndexDefinition, error) {
	if c == nil || c.indexRepo == nil {
		return nil, errors.New("coordinator not initialised")
	}

	defs, err := c.indexRepo.ListIndexes(ctx)
	if err != nil {
		c.log.Error("list indexes failed", helpers.Field{Key: "error", Value: err})
		return nil, err
	}
	return defs, nil
}
