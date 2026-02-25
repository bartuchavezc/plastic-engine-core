package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"plastic-engine-core/internal/pkg/logger"
)

// LocalApplier implements StateApplier by writing directly to SQLite.
// This is used in standalone mode where there is only a single coordinator node.
type LocalApplier struct {
	db  *sql.DB
	log logger.Logger
}

// NewLocalApplier creates a LocalApplier backed by the provided database.
func NewLocalApplier(db *sql.DB, log logger.Logger) *LocalApplier {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &LocalApplier{db: db, log: log}
}

// Apply executes a command directly against SQLite.
// In standalone mode, all commands are executed immediately without replication.
func (a *LocalApplier) Apply(ctx context.Context, cmd Command) error {
	a.log.Debug("applying command",
		logger.Field{Key: "type", Value: cmd.Type.String()},
		logger.Field{Key: "timestamp", Value: cmd.Timestamp},
	)

	switch cmd.Type {
	case CmdCreateIndex:
		return a.applyCreateIndex(ctx, cmd.Payload)
	case CmdDeleteIndex:
		return a.applyDeleteIndex(ctx, cmd.Payload)
	case CmdRegisterNode:
		return a.applyRegisterNode(ctx, cmd.Payload)
	case CmdUpdateHeartbeat:
		return a.applyUpdateHeartbeat(ctx, cmd.Payload)
	case CmdMarkNodeUnreachable:
		return a.applyMarkNodeUnreachable(ctx, cmd.Payload)
	case CmdCreateShards:
		return a.applyCreateShards(ctx, cmd.Payload)
	case CmdAssignShard:
		return a.applyAssignShard(ctx, cmd.Payload)
	case CmdUpdateShardState:
		return a.applyUpdateShardState(ctx, cmd.Payload)
	default:
		return fmt.Errorf("%w: %d", ErrUnknownCommand, cmd.Type)
	}
}

// IsLeader always returns true for LocalApplier since there is only one node.
func (a *LocalApplier) IsLeader() bool {
	return true
}

// LeaderAddr returns an empty string since LocalApplier is the only node.
func (a *LocalApplier) LeaderAddr() string {
	return ""
}

// WaitForLeader returns immediately since LocalApplier is always the leader.
func (a *LocalApplier) WaitForLeader(ctx context.Context) (string, error) {
	return "", nil
}

func (a *LocalApplier) applyCreateIndex(ctx context.Context, payload json.RawMessage) error {
	var p CreateIndexPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal create index payload: %w", err)
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create index tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := p.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}

	shardConfig := ""
	if len(p.ShardConfig) > 0 {
		shardConfig = string(p.ShardConfig)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO indexes (
			id, name, shard_strategy, shard_template, shard_config,
			default_analyzer, default_tokenizer, mapping_version,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.ID, p.Name, p.ShardStrategy, p.ShardTemplate, shardConfig,
		p.DefaultAnalyzer, p.DefaultTokenizer, p.MappingVersion,
		now, now,
	)
	if err != nil {
		return fmt.Errorf("insert index: %w", err)
	}

	// Insert field mappings if provided
	if len(p.FieldMappings) > 0 {
		var fields []struct {
			Name       string `json:"name"`
			Type       string `json:"type"`
			Analyzer   string `json:"analyzer,omitempty"`
			Tokenizer  string `json:"tokenizer,omitempty"`
			Stored     bool   `json:"stored,omitempty"`
			Required   bool   `json:"required,omitempty"`
			Indexed    bool   `json:"indexed,omitempty"`
		}
		if err := json.Unmarshal(p.FieldMappings, &fields); err != nil {
			return fmt.Errorf("unmarshal field mappings: %w", err)
		}

		for _, field := range fields {
			indexed := 1
			if !field.Indexed {
				indexed = 0
			}
			_, err := tx.ExecContext(ctx,
				`INSERT INTO index_fields (
					index_id, field_name, field_type, analyzer, tokenizer,
					is_searchable, is_stored, is_required, is_indexed
				) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)`,
				p.ID, field.Name, field.Type,
				nullableString(field.Analyzer), nullableString(field.Tokenizer),
				boolToInt(field.Stored), boolToInt(field.Required), indexed,
			)
			if err != nil {
				return fmt.Errorf("insert field mapping %s: %w", field.Name, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create index: %w", err)
	}

	a.log.Info("index created via applier",
		logger.Field{Key: "index_id", Value: p.ID},
		logger.Field{Key: "name", Value: p.Name},
	)

	return nil
}

func (a *LocalApplier) applyDeleteIndex(ctx context.Context, payload json.RawMessage) error {
	var p DeleteIndexPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal delete index payload: %w", err)
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete index tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Delete shards first (FK constraint)
	_, err = tx.ExecContext(ctx, `DELETE FROM shards WHERE index_id = ?`, p.ID)
	if err != nil {
		return fmt.Errorf("delete shards for index: %w", err)
	}

	// Delete field mappings
	_, err = tx.ExecContext(ctx, `DELETE FROM index_fields WHERE index_id = ?`, p.ID)
	if err != nil {
		return fmt.Errorf("delete field mappings for index: %w", err)
	}

	// Delete mappings if table exists
	_, _ = tx.ExecContext(ctx, `DELETE FROM mappings WHERE index_id = ?`, p.ID)

	// Delete the index
	res, err := tx.ExecContext(ctx, `DELETE FROM indexes WHERE id = ?`, p.ID)
	if err != nil {
		return fmt.Errorf("delete index: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("index %s not found", p.ID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete index: %w", err)
	}

	a.log.Info("index deleted via applier",
		logger.Field{Key: "index_id", Value: p.ID},
	)

	return nil
}

func (a *LocalApplier) applyRegisterNode(ctx context.Context, payload json.RawMessage) error {
	var p RegisterNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal register node payload: %w", err)
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin register node tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Try update first, then insert
	res, err := tx.ExecContext(ctx,
		`UPDATE nodes
		 SET role = ?, advertise_addr = ?, data_dir = ?, last_heartbeat = CURRENT_TIMESTAMP, status = 'joining'
		 WHERE id = ?`,
		p.Role, p.AdvertiseAddr, p.DataDir, p.NodeID,
	)
	if err != nil {
		return fmt.Errorf("update node: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO nodes (id, role, advertise_addr, data_dir, last_heartbeat, status)
			 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 'joining')`,
			p.NodeID, p.Role, p.AdvertiseAddr, p.DataDir,
		)
		if err != nil {
			return fmt.Errorf("insert node: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit register node: %w", err)
	}

	a.log.Info("node registered via applier",
		logger.Field{Key: "node_id", Value: p.NodeID},
		logger.Field{Key: "role", Value: p.Role},
	)

	return nil
}

func (a *LocalApplier) applyUpdateHeartbeat(ctx context.Context, payload json.RawMessage) error {
	var p UpdateHeartbeatPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal heartbeat payload: %w", err)
	}

	timestamp := p.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	res, err := a.db.ExecContext(ctx,
		`UPDATE nodes SET last_heartbeat = ?, status = 'ready' WHERE id = ?`,
		timestamp.Format("2006-01-02 15:04:05"), p.NodeID,
	)
	if err != nil {
		return fmt.Errorf("update heartbeat: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("node %s not registered", p.NodeID)
	}

	return nil
}

func (a *LocalApplier) applyMarkNodeUnreachable(ctx context.Context, payload json.RawMessage) error {
	var p MarkNodeUnreachablePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal mark unreachable payload: %w", err)
	}

	threshold := time.Now().UTC().Add(-p.Timeout).Format("2006-01-02 15:04:05")

	res, err := a.db.ExecContext(ctx,
		`UPDATE nodes
		 SET status = 'unreachable'
		 WHERE status <> 'unreachable'
		   AND (last_heartbeat IS NULL OR last_heartbeat <= ?)`,
		threshold,
	)
	if err != nil {
		return fmt.Errorf("mark nodes unreachable: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows > 0 {
		a.log.Info("marked nodes unreachable via applier",
			logger.Field{Key: "count", Value: rows},
		)
	}

	return nil
}

func (a *LocalApplier) applyCreateShards(ctx context.Context, payload json.RawMessage) error {
	var p CreateShardsPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal create shards payload: %w", err)
	}

	if len(p.Keys) == 0 {
		return nil
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create shards tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	for _, key := range p.Keys {
		shardID := uuid.NewString()
		_, err := tx.ExecContext(ctx,
			`INSERT INTO shards (id, index_id, shard_key, state, version, created_at, updated_at)
			 VALUES (?, ?, ?, 'pending', 0, ?, ?)`,
			shardID, p.IndexID, key, now, now,
		)
		if err != nil {
			return fmt.Errorf("insert shard %s: %w", key, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create shards: %w", err)
	}

	a.log.Info("shards created via applier",
		logger.Field{Key: "index_id", Value: p.IndexID},
		logger.Field{Key: "count", Value: len(p.Keys)},
	)

	return nil
}

func (a *LocalApplier) applyAssignShard(ctx context.Context, payload json.RawMessage) error {
	var p AssignShardPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal assign shard payload: %w", err)
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	res, err := a.db.ExecContext(ctx,
		`UPDATE shards
		 SET primary_node = ?, state = 'assigned', updated_at = ?
		 WHERE id = ?`,
		p.NodeID, now, p.ShardID,
	)
	if err != nil {
		return fmt.Errorf("assign shard: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("shard %s not found", p.ShardID)
	}

	a.log.Debug("shard assigned via applier",
		logger.Field{Key: "shard_id", Value: p.ShardID},
		logger.Field{Key: "node_id", Value: p.NodeID},
	)

	return nil
}

func (a *LocalApplier) applyUpdateShardState(ctx context.Context, payload json.RawMessage) error {
	var p UpdateShardStatePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal update shard state payload: %w", err)
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	res, err := a.db.ExecContext(ctx,
		`UPDATE shards
		 SET state = ?, updated_at = ?
		 WHERE id = ?`,
		p.State, now, p.ShardID,
	)
	if err != nil {
		return fmt.Errorf("update shard state: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("shard %s not found", p.ShardID)
	}

	return nil
}

// Helper functions

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

