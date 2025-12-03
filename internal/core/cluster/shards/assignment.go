package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	shards "plastic-engine-core/internal/core/search/shards"
)

const DefaultTimestampFormat = "2006-01-02 15:04:05"

// InsertShard persists a new shard record with pending state.
func InsertShard(ctx context.Context, tx *sql.Tx, shardID, indexID, shardKey, timestamp string) error {
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

// AssignShardsToNodeTx updates shard ownership inside the supplied transaction.
func AssignShardsToNodeTx(ctx context.Context, tx *sql.Tx, nodeID string, limit int) ([]shards.Assignment, error) {
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

	assignments := make([]shards.Assignment, 0)
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
func LoadAssignmentsForNodeTx(ctx context.Context, tx *sql.Tx, nodeID string) ([]shards.Assignment, error) {
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

	assignments := make([]shards.Assignment, 0)
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

func buildAssignment(shardID, indexID, shardKey, analyzer, tokenizer string, mappingVersion int) shards.Assignment {
	return shards.Assignment{
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
