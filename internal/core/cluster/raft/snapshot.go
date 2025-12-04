package raft

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"

	"plastic-engine-core/internal/pkg/logger"
)

// FSMSnapshot implements raft.FSMSnapshot for SQLite state.
// It captures the SQLite database state for log compaction.
type FSMSnapshot struct {
	db  *sql.DB
	log logger.Logger
}

// Persist writes the snapshot to the given sink.
// We use SQLite's VACUUM INTO to create a clean copy of the database.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	s.log.Info("persisting raft snapshot")

	// Create a temporary file for the snapshot
	tempDir := os.TempDir()
	tempFile := filepath.Join(tempDir, fmt.Sprintf("raft-snapshot-%s.db", sink.ID()))

	// Use VACUUM INTO to create a clean copy of the database
	// This is atomic and doesn't block readers
	_, err := s.db.Exec(fmt.Sprintf(`VACUUM INTO '%s'`, tempFile))
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("vacuum into snapshot: %w", err)
	}
	defer os.Remove(tempFile)

	// Read the snapshot file and write to sink
	f, err := os.Open(tempFile)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("open snapshot file: %w", err)
	}
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("stat snapshot file: %w", err)
	}

	// Write size header (8 bytes)
	sizeHeader := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeHeader, uint64(stat.Size()))
	if _, err := sink.Write(sizeHeader); err != nil {
		sink.Cancel()
		return fmt.Errorf("write size header: %w", err)
	}

	// Copy database contents
	if _, err := io.Copy(sink, f); err != nil {
		sink.Cancel()
		return fmt.Errorf("copy snapshot data: %w", err)
	}

	s.log.Info("raft snapshot persisted",
		logger.Field{Key: "size", Value: stat.Size()},
	)

	return sink.Close()
}

// Release is called when the snapshot is no longer needed.
func (s *FSMSnapshot) Release() {
	// Nothing to release - temporary file already cleaned up
}

// SQLiteSnapshot handles restoring SQLite from a snapshot.
type SQLiteSnapshot struct{}

// Restore restores the SQLite database from a snapshot reader.
func (s *SQLiteSnapshot) Restore(db *sql.DB, r io.Reader) error {
	// Read size header
	sizeHeader := make([]byte, 8)
	if _, err := io.ReadFull(r, sizeHeader); err != nil {
		return fmt.Errorf("read size header: %w", err)
	}
	size := binary.BigEndian.Uint64(sizeHeader)

	// Create temporary file for the snapshot
	tempDir := os.TempDir()
	tempFile := filepath.Join(tempDir, "raft-restore-temp.db")
	defer os.Remove(tempFile)

	f, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("create restore file: %w", err)
	}

	// Copy exactly 'size' bytes
	if _, err := io.CopyN(f, r, int64(size)); err != nil {
		f.Close()
		return fmt.Errorf("copy restore data: %w", err)
	}
	f.Close()

	// Clear existing tables and restore from snapshot
	// We do this by reading the snapshot DB and copying data

	snapshotDB, err := sql.Open("sqlite", tempFile)
	if err != nil {
		return fmt.Errorf("open snapshot db: %w", err)
	}
	defer snapshotDB.Close()

	// Begin transaction on target DB
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin restore tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Get list of tables from snapshot
	tables, err := getTableNames(snapshotDB)
	if err != nil {
		return fmt.Errorf("get table names: %w", err)
	}

	// Clear and restore each table
	for _, table := range tables {
		// Delete existing data
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s`, table)); err != nil {
			// Table might not exist yet, that's OK
			continue
		}

		// Copy data from snapshot
		if err := copyTable(snapshotDB, tx, table); err != nil {
			return fmt.Errorf("copy table %s: %w", table, err)
		}
	}

	return tx.Commit()
}

func getTableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func copyTable(src *sql.DB, dst *sql.Tx, table string) error {
	// Get column names
	rows, err := src.Query(fmt.Sprintf(`SELECT * FROM %s LIMIT 0`, table))
	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	rows.Close()
	if err != nil {
		return err
	}

	if len(cols) == 0 {
		return nil
	}

	// Build INSERT statement
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf(`INSERT INTO %s VALUES (%s)`,
		table, joinStrings(placeholders, ", "))

	// Copy all rows
	rows, err = src.Query(fmt.Sprintf(`SELECT * FROM %s`, table))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// Create slice of interface{} to hold values
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		if _, err := dst.Exec(insertSQL, values...); err != nil {
			return err
		}
	}

	return rows.Err()
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for _, s := range strs[1:] {
		result += sep + s
	}
	return result
}

