package coordinator

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // SQLite driver without CGO.
)

// OpenMetadataDB opens (or creates) the SQLite database that stores cluster metadata.
// An empty path defaults to a local "coordinator.db" file.
func OpenMetadataDB(path string) (*sql.DB, error) {
	if path == "" {
		path = "coordinator.db"
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	// SQLite works better with a single writer in our use-case.
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable sqlite foreign_keys: %w", err)
	}

	if err := initializeCoordinatorSchema(db); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func initializeCoordinatorSchema(db *sql.DB) error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS indexes (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			shard_strategy TEXT NOT NULL,
			shard_template TEXT,
			shard_config TEXT,
			default_analyzer TEXT NOT NULL,
			default_tokenizer TEXT NOT NULL,
			mapping_version INTEGER NOT NULL DEFAULT 1,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS index_fields (
			index_id TEXT NOT NULL,
			field_name TEXT NOT NULL,
			field_type TEXT NOT NULL,
			analyzer TEXT,
			tokenizer TEXT,
			is_searchable INTEGER NOT NULL DEFAULT 1,
			is_stored INTEGER NOT NULL DEFAULT 0,
			is_required INTEGER NOT NULL DEFAULT 0,
			is_indexed INTEGER NOT NULL DEFAULT 1,
			PRIMARY KEY (index_id, field_name),
			FOREIGN KEY(index_id) REFERENCES indexes(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS nodes (
			id TEXT PRIMARY KEY,
			role TEXT NOT NULL,
			advertise_addr TEXT,
			data_dir TEXT,
			last_heartbeat DATETIME,
			status TEXT NOT NULL DEFAULT 'joining'
		);`,
		`CREATE TABLE IF NOT EXISTS shards (
			id TEXT PRIMARY KEY,
			index_id TEXT NOT NULL,
			shard_key TEXT NOT NULL,
			primary_node TEXT,
			state TEXT NOT NULL DEFAULT 'pending',
			version INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME,
			FOREIGN KEY(index_id) REFERENCES indexes(id) ON DELETE CASCADE,
			FOREIGN KEY(primary_node) REFERENCES nodes(id),
			UNIQUE(index_id, shard_key)
		);`,
		`CREATE TABLE IF NOT EXISTS shard_replicas (
			shard_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			state TEXT NOT NULL DEFAULT 'pending',
			last_sync DATETIME,
			PRIMARY KEY (shard_id, node_id),
			FOREIGN KEY(shard_id) REFERENCES shards(id) ON DELETE CASCADE,
			FOREIGN KEY(node_id) REFERENCES nodes(id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_nodes_last_heartbeat ON nodes(last_heartbeat);`,
		`CREATE INDEX IF NOT EXISTS idx_shards_state ON shards(state);`,
		`CREATE INDEX IF NOT EXISTS idx_shards_index ON shards(index_id);`,
		`CREATE INDEX IF NOT EXISTS idx_shards_primary_node ON shards(primary_node);`,
		`CREATE INDEX IF NOT EXISTS idx_indexes_name ON indexes(name);`,
	}

	for _, stmt := range schema {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("apply schema statement: %w", err)
		}
	}

	if _, err := db.Exec(`ALTER TABLE indexes ADD COLUMN shard_config TEXT`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add shard_config column: %w", err)
		}
	}

	if _, err := db.Exec(`ALTER TABLE index_fields ADD COLUMN is_required INTEGER NOT NULL DEFAULT 0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add index_fields.is_required column: %w", err)
		}
	}

	if _, err := db.Exec(`ALTER TABLE index_fields ADD COLUMN is_indexed INTEGER NOT NULL DEFAULT 1`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add index_fields.is_indexed column: %w", err)
		}
	}

	return nil
}
