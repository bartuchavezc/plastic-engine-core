package shards

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	searchshards "plastic-engine-core/internal/core/search/shards"
)

// Repository handles shard persistence in SQLite.
type Repository struct {
	db *sql.DB
}

// NewRepository creates a Repository backed by the provided database.
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// InsertShard persists a new shard record with pending state.
func (r *Repository) InsertShard(ctx context.Context, tx *sql.Tx, shardID, indexID, shardKey, timestamp string) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO shards (id, index_id, shard_key, state, version, created_at, updated_at)
		 VALUES (?, ?, ?, 'pending', 0, ?, ?)`,
		shardID,
		indexID,
		shardKey,
		timestamp,
		timestamp,
	)
	if err != nil {
		return fmt.Errorf("insert shard %s: %w", shardKey, err)
	}
	return nil
}

// AssignToNodeTx updates shard ownership inside the supplied transaction.
func (r *Repository) AssignToNodeTx(ctx context.Context, tx *sql.Tx, nodeID string, limit int) ([]searchshards.Assignment, error) {
	if strings.TrimSpace(nodeID) == "" {
		return nil, fmt.Errorf("assign shards: node id is required")
	}

	query := `SELECT s.id, s.index_id, s.shard_key, i.default_analyzer, i.default_tokenizer, i.mapping_version
		FROM shards s
		INNER JOIN indexes i ON i.id = s.index_id
		WHERE s.primary_node IS NULL
		ORDER BY s.created_at ASC`
	args := []any{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query pending shards: %w", err)
	}
	defer rows.Close()

	assignments := make([]searchshards.Assignment, 0)
	now := time.Now().UTC().Format(DefaultTimestampFormat)

	for rows.Next() {
		var shardID, indexID, shardKey, analyzer, tokenizer string
		var mappingVersion int
		if err := rows.Scan(&shardID, &indexID, &shardKey, &analyzer, &tokenizer, &mappingVersion); err != nil {
			return nil, fmt.Errorf("scan pending shard: %w", err)
		}

		res, err := tx.ExecContext(ctx, `UPDATE shards
			SET primary_node = ?, state = 'assigned', updated_at = ?
			WHERE id = ? AND primary_node IS NULL`,
			nodeID, now, shardID,
		)
		if err != nil {
			return nil, fmt.Errorf("assign shard %s: %w", shardID, err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("assign shard rows affected: %w", err)
		}
		if rowsAffected == 0 {
			continue
		}

		assignments = append(assignments, buildAssignment(shardID, indexID, shardKey, analyzer, tokenizer, mappingVersion))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending shards: %w", err)
	}

	return assignments, nil
}

// LoadAssignmentsForNodeTx fetches the assignments for a node inside an existing transaction.
func (r *Repository) LoadAssignmentsForNodeTx(ctx context.Context, tx *sql.Tx, nodeID string) ([]searchshards.Assignment, error) {
	if strings.TrimSpace(nodeID) == "" {
		return nil, fmt.Errorf("load assignments: node id is required")
	}

	rows, err := tx.QueryContext(ctx, `SELECT s.id, s.index_id, s.shard_key, i.default_analyzer, i.default_tokenizer, i.mapping_version
		FROM shards s
		INNER JOIN indexes i ON i.id = s.index_id
		WHERE s.primary_node = ?
		ORDER BY s.created_at`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("query assigned shards: %w", err)
	}
	defer rows.Close()

	assignments := make([]searchshards.Assignment, 0)
	for rows.Next() {
		var (
			shardID, indexID, shardKey, analyzer, tokenizer string
			mappingVersion                                  int
		)
		if err := rows.Scan(&shardID, &indexID, &shardKey, &analyzer, &tokenizer, &mappingVersion); err != nil {
			return nil, fmt.Errorf("scan assigned shard: %w", err)
		}
		assignments = append(assignments, buildAssignment(shardID, indexID, shardKey, analyzer, tokenizer, mappingVersion))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate assigned shards: %w", err)
	}

	return assignments, nil
}

// LookupPrimaryShard resolves the shard responsible for the given index and shard key.
func (r *Repository) LookupPrimaryShard(ctx context.Context, indexID, shardKey string) (Info, error) {
	row := r.db.QueryRowContext(ctx, `SELECT id, primary_node FROM shards WHERE index_id = ? AND shard_key = ?`, indexID, shardKey)

	var info Info
	if err := row.Scan(&info.ID, &info.PrimaryNode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Info{}, ErrPrimaryNotFound
		}
		return Info{}, fmt.Errorf("scan shard: %w", err)
	}

	if strings.TrimSpace(info.PrimaryNode) == "" {
		return Info{}, fmt.Errorf("shard %s has no primary assigned", info.ID)
	}

	return info, nil
}

// LookupNode resolves a node by identifier and returns its advertise address.
func (r *Repository) LookupNode(ctx context.Context, nodeID string) (NodeInfo, error) {
	if strings.TrimSpace(nodeID) == "" {
		return NodeInfo{}, fmt.Errorf("node id is required")
	}

	row := r.db.QueryRowContext(ctx, `SELECT id, advertise_addr FROM nodes WHERE id = ?`, nodeID)

	var (
		node          NodeInfo
		advertiseAddr sql.NullString
	)

	if err := row.Scan(&node.ID, &advertiseAddr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeInfo{}, fmt.Errorf("node %s not found", nodeID)
		}
		return NodeInfo{}, fmt.Errorf("scan node: %w", err)
	}

	if advertiseAddr.Valid {
		node.AdvertiseAddr = advertiseAddr.String
	}

	return node, nil
}

// List returns shard metadata filtered by the provided filter.
func (r *Repository) List(ctx context.Context, filter ShardFilter) ([]ShardRecord, error) {
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

	rows, err := r.db.QueryContext(ctx, query, args...)
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

// DB returns the underlying database handle for transactions.
func (r *Repository) DB() *sql.DB {
	return r.db
}

func buildAssignment(shardID, indexID, shardKey, analyzer, tokenizer string, mappingVersion int) searchshards.Assignment {
	return searchshards.Assignment{
		ID:               shardID,
		IndexID:          indexID,
		ShardKey:         shardKey,
		Primary:          true,
		NeedsReplication: false,
		RangeHint:        shardKey,
		Analyzer:         analyzer,
		Tokenizer:        tokenizer,
		MappingVersion:   mappingVersion,
		Fields:           nil,
	}
}

