package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"plastic-engine-core/internal/pkg/logger"
)

// HeartbeatRequest captures heartbeat data reported by a cluster node.
type HeartbeatRequest struct {
	NodeID string   `json:"node_id"`
	Shards []string `json:"shards"`
}

// Heartbeat updates metadata for an existing node.
func (c *Coordinator) Heartbeat(ctx context.Context, req HeartbeatRequest) error {
	ctx, span := otel.Tracer("coordinator").Start(ctx, "Coordinator.Heartbeat")
	defer span.End()

	if c == nil || c.db == nil {
		return errors.New("coordinator is not initialised")
	}

	if req.NodeID == "" {
		return errors.New("heartbeat missing node id")
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin heartbeat tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05")

	if err := updateNodeHeartbeat(tx, req.NodeID, timestamp); err != nil {
		return err
	}

	span.SetAttributes(
		attribute.String("node.id", req.NodeID),
		attribute.Int("node.shards.count", len(req.Shards)),
	)

	c.Logger().Info("heartbeat received",
		logger.Field{Key: "node_id", Value: req.NodeID},
		logger.Field{Key: "shards", Value: req.Shards},
	)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit heartbeat tx: %w", err)
	}

	return nil
}

func updateNodeHeartbeat(tx *sql.Tx, nodeID string, timestamp string) error {
	res, err := tx.Exec(`UPDATE nodes SET last_heartbeat = ?, status = 'ready' WHERE id = ?`, timestamp, nodeID)
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
