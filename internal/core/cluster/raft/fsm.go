package raft

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"

	"plastic-engine-core/internal/core/cluster"
	"plastic-engine-core/internal/pkg/logger"
)

// FSM implements raft.FSM using SQLite as the state backend.
// All state changes are applied to the SQLite database.
type FSM struct {
	db  *sql.DB
	log logger.Logger
}

// NewFSM creates an FSM backed by the provided SQLite database.
func NewFSM(db *sql.DB, log logger.Logger) *FSM {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &FSM{db: db, log: log}
}

// Apply is called by Raft when a log entry is committed.
// It deserializes the command and applies it to SQLite.
// This method MUST be deterministic: same input → same output on all nodes.
func (f *FSM) Apply(logEntry *raft.Log) interface{} {
	var cmd cluster.Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		f.log.Error("failed to unmarshal raft command",
			logger.Field{Key: "error", Value: err},
			logger.Field{Key: "index", Value: logEntry.Index},
		)
		return err
	}

	f.log.Debug("applying raft command",
		logger.Field{Key: "type", Value: cmd.Type.String()},
		logger.Field{Key: "index", Value: logEntry.Index},
		logger.Field{Key: "term", Value: logEntry.Term},
	)

	var err error
	switch cmd.Type {
	case cluster.CmdCreateIndex:
		err = f.applyCreateIndex(cmd.Payload)
	case cluster.CmdRegisterNode:
		err = f.applyRegisterNode(cmd.Payload)
	case cluster.CmdUpdateHeartbeat:
		err = f.applyUpdateHeartbeat(cmd.Payload)
	case cluster.CmdMarkNodeUnreachable:
		err = f.applyMarkNodeUnreachable(cmd.Payload)
	case cluster.CmdCreateShards:
		err = f.applyCreateShards(cmd.Payload)
	case cluster.CmdAssignShard:
		err = f.applyAssignShard(cmd.Payload)
	case cluster.CmdUpdateShardState:
		err = f.applyUpdateShardState(cmd.Payload)
	default:
		err = fmt.Errorf("%w: %d", cluster.ErrUnknownCommand, cmd.Type)
	}

	if err != nil {
		f.log.Error("failed to apply raft command",
			logger.Field{Key: "type", Value: cmd.Type.String()},
			logger.Field{Key: "error", Value: err},
		)
	}

	return err
}

// Snapshot returns an FSMSnapshot that captures the current state.
// This is used for log compaction and to bring new nodes up to date.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.log.Info("creating raft snapshot")
	return &FSMSnapshot{db: f.db, log: f.log}, nil
}

// Restore replaces the current state with the contents of a snapshot.
// This is called when a new node joins or when recovering from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.log.Info("restoring raft snapshot")
	defer rc.Close()

	snapshot := &SQLiteSnapshot{}
	if err := snapshot.Restore(f.db, rc); err != nil {
		f.log.Error("failed to restore snapshot", logger.Field{Key: "error", Value: err})
		return err
	}

	f.log.Info("raft snapshot restored")
	return nil
}

// Command application methods

func (f *FSM) applyCreateIndex(payload json.RawMessage) error {
	var p cluster.CreateIndexPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal create index payload: %w", err)
	}

	tx, err := f.db.Begin()
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

	_, err = tx.Exec(
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

	if len(p.FieldMappings) > 0 {
		var fields []struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			Analyzer  string `json:"analyzer,omitempty"`
			Tokenizer string `json:"tokenizer,omitempty"`
			Stored    bool   `json:"stored,omitempty"`
			Required  bool   `json:"required,omitempty"`
			Indexed   bool   `json:"indexed,omitempty"`
		}
		if err := json.Unmarshal(p.FieldMappings, &fields); err != nil {
			return fmt.Errorf("unmarshal field mappings: %w", err)
		}

		for _, field := range fields {
			indexed := 1
			if !field.Indexed {
				indexed = 0
			}
			_, err := tx.Exec(
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

	return tx.Commit()
}

func (f *FSM) applyRegisterNode(payload json.RawMessage) error {
	var p cluster.RegisterNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal register node payload: %w", err)
	}

	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("begin register node tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.Exec(
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
		_, err = tx.Exec(
			`INSERT INTO nodes (id, role, advertise_addr, data_dir, last_heartbeat, status)
			 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 'joining')`,
			p.NodeID, p.Role, p.AdvertiseAddr, p.DataDir,
		)
		if err != nil {
			return fmt.Errorf("insert node: %w", err)
		}
	}

	return tx.Commit()
}

func (f *FSM) applyUpdateHeartbeat(payload json.RawMessage) error {
	var p cluster.UpdateHeartbeatPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal heartbeat payload: %w", err)
	}

	timestamp := p.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	res, err := f.db.Exec(
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

func (f *FSM) applyMarkNodeUnreachable(payload json.RawMessage) error {
	var p cluster.MarkNodeUnreachablePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal mark unreachable payload: %w", err)
	}

	threshold := time.Now().UTC().Add(-p.Timeout).Format("2006-01-02 15:04:05")

	_, err := f.db.Exec(
		`UPDATE nodes
		 SET status = 'unreachable'
		 WHERE status <> 'unreachable'
		   AND (last_heartbeat IS NULL OR last_heartbeat <= ?)`,
		threshold,
	)
	return err
}

func (f *FSM) applyCreateShards(payload json.RawMessage) error {
	var p cluster.CreateShardsPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal create shards payload: %w", err)
	}

	if len(p.Keys) == 0 {
		return nil
	}

	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("begin create shards tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	for _, key := range p.Keys {
		shardID := uuid.NewString()
		_, err := tx.Exec(
			`INSERT INTO shards (id, index_id, shard_key, state, version, created_at, updated_at)
			 VALUES (?, ?, ?, 'pending', 0, ?, ?)`,
			shardID, p.IndexID, key, now, now,
		)
		if err != nil {
			return fmt.Errorf("insert shard %s: %w", key, err)
		}
	}

	return tx.Commit()
}

func (f *FSM) applyAssignShard(payload json.RawMessage) error {
	var p cluster.AssignShardPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal assign shard payload: %w", err)
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	res, err := f.db.Exec(
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

	return nil
}

func (f *FSM) applyUpdateShardState(payload json.RawMessage) error {
	var p cluster.UpdateShardStatePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal update shard state payload: %w", err)
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	res, err := f.db.Exec(
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

func nullableString(s string) interface{} {
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

