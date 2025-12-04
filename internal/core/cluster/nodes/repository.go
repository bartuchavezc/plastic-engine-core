package nodes

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Repository handles node persistence in SQLite.
type Repository struct {
	db *sql.DB
}

// NewRepository creates a Repository backed by the provided database.
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// UpsertNode inserts or updates a node record.
func (r *Repository) UpsertNode(ctx context.Context, tx *sql.Tx, nodeID string, req JoinRequest) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE nodes
		 SET role = ?, advertise_addr = ?, data_dir = ?, last_heartbeat = CURRENT_TIMESTAMP, status = 'joining'
		 WHERE id = ?`,
		req.Role,
		req.AdvertiseAddr,
		req.DataDir,
		nodeID,
	)
	if err != nil {
		return fmt.Errorf("update node metadata: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected update: %w", err)
	}

	if rows > 0 {
		return nil
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO nodes (id, role, advertise_addr, data_dir, last_heartbeat, status)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 'joining')`,
		nodeID,
		req.Role,
		req.AdvertiseAddr,
		req.DataDir,
	)
	if err != nil {
		return fmt.Errorf("insert node metadata: %w", err)
	}
	return nil
}

// UpdateHeartbeat updates the last heartbeat timestamp and sets status to ready.
func (r *Repository) UpdateHeartbeat(ctx context.Context, tx *sql.Tx, nodeID string, timestamp string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE nodes SET last_heartbeat = ?, status = 'ready' WHERE id = ?`,
		timestamp, nodeID,
	)
	if err != nil {
		return fmt.Errorf("update node heartbeat: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("node %s not registered", nodeID)
	}

	return nil
}

// MarkStaleNodes marks nodes without recent heartbeats as unreachable.
func (r *Repository) MarkStaleNodes(ctx context.Context, timeout time.Duration) (int64, error) {
	threshold := time.Now().UTC().Add(-timeout).Format("2006-01-02 15:04:05")

	res, err := r.db.ExecContext(ctx,
		`UPDATE nodes
		 SET status = 'unreachable'
		 WHERE status <> 'unreachable'
		   AND (last_heartbeat IS NULL OR last_heartbeat <= ?)`,
		threshold,
	)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// List returns all nodes registered in the metadata store.
func (r *Repository) List(ctx context.Context) ([]NodeRecord, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT
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

// ListEligible returns nodes that can receive shard assignments.
func (r *Repository) ListEligible(ctx context.Context) ([]NodeRecord, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id, role, advertise_addr, data_dir, status
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

// DB returns the underlying database handle for transactions.
func (r *Repository) DB() *sql.DB {
	return r.db
}

