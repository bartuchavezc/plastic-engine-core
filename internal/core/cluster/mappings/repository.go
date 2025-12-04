package mappings

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SQLiteRepository implements Repository using SQLite.
type SQLiteRepository struct {
	db *sql.DB
}

// NewSQLiteRepository creates a new SQLite-backed mapping repository.
func NewSQLiteRepository(db *sql.DB) *SQLiteRepository {
	return &SQLiteRepository{db: db}
}

// GetMapping retrieves the mapping for an index, including all fields.
func (r *SQLiteRepository) GetMapping(ctx context.Context, indexID string) (Mapping, error) {
	// First, get the index's dynamic mode from indexes table
	var dynamic sql.NullString
	var createdAt, updatedAt time.Time
	var mappingVersion int

	err := r.db.QueryRowContext(ctx, `
		SELECT dynamic_mode, mapping_version, created_at, updated_at
		FROM indexes WHERE id = ?
	`, indexID).Scan(&dynamic, &mappingVersion, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Mapping{}, ErrMappingNotFound
		}
		return Mapping{}, fmt.Errorf("get index: %w", err)
	}

	// Get all fields for this index
	rows, err := r.db.QueryContext(ctx, `
		SELECT field_name, field_type, analyzer, tokenizer,
		       is_stored, is_required, is_indexed
		FROM index_fields
		WHERE index_id = ?
		ORDER BY field_name
	`, indexID)
	if err != nil {
		return Mapping{}, fmt.Errorf("query fields: %w", err)
	}
	defer rows.Close()

	var fields []Field
	for rows.Next() {
		var f Field
		var analyzer, tokenizer sql.NullString
		var stored, required, indexed int

		if err := rows.Scan(
			&f.Name,
			(*string)(&f.Type),
			&analyzer,
			&tokenizer,
			&stored,
			&required,
			&indexed,
		); err != nil {
			return Mapping{}, fmt.Errorf("scan field: %w", err)
		}

		f.Analyzer = analyzer.String
		f.Tokenizer = tokenizer.String
		f.Stored = stored == 1
		f.Required = required == 1
		f.Indexed = indexed == 1
		fields = append(fields, f)
	}

	if err := rows.Err(); err != nil {
		return Mapping{}, fmt.Errorf("iterate fields: %w", err)
	}

	dynamicMode := DynamicTrue
	if dynamic.Valid && dynamic.String != "" {
		dynamicMode = DynamicMode(dynamic.String)
	}

	return Mapping{
		IndexID:   indexID,
		Version:   mappingVersion,
		Dynamic:   dynamicMode,
		Fields:    fields,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// SaveMapping creates or updates a complete mapping.
func (r *SQLiteRepository) SaveMapping(ctx context.Context, mapping Mapping) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Update the index's dynamic mode and version
	_, err = tx.ExecContext(ctx, `
		UPDATE indexes
		SET dynamic_mode = ?, mapping_version = mapping_version + 1, updated_at = ?
		WHERE id = ?
	`, string(mapping.Dynamic), time.Now().UTC(), mapping.IndexID)
	if err != nil {
		return fmt.Errorf("update index: %w", err)
	}

	// Delete existing fields and re-insert (for full replacement)
	_, err = tx.ExecContext(ctx, `DELETE FROM index_fields WHERE index_id = ?`, mapping.IndexID)
	if err != nil {
		return fmt.Errorf("delete fields: %w", err)
	}

	for _, f := range mapping.Fields {
		if err := insertField(ctx, tx, mapping.IndexID, f); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// AddFields adds new fields to an existing mapping.
func (r *SQLiteRepository) AddFields(ctx context.Context, indexID string, fields []Field) error {
	if len(fields) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, f := range fields {
		if err := insertField(ctx, tx, indexID, f); err != nil {
			return err
		}
	}

	// Increment mapping version
	_, err = tx.ExecContext(ctx, `
		UPDATE indexes
		SET mapping_version = mapping_version + 1, updated_at = ?
		WHERE id = ?
	`, time.Now().UTC(), indexID)
	if err != nil {
		return fmt.Errorf("update version: %w", err)
	}

	return tx.Commit()
}

// UpdateDynamic changes the dynamic mode for a mapping.
func (r *SQLiteRepository) UpdateDynamic(ctx context.Context, indexID string, mode DynamicMode) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE indexes
		SET dynamic_mode = ?, updated_at = ?
		WHERE id = ?
	`, string(mode), time.Now().UTC(), indexID)
	if err != nil {
		return fmt.Errorf("update dynamic: %w", err)
	}
	return nil
}

func insertField(ctx context.Context, tx *sql.Tx, indexID string, f Field) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO index_fields (
			index_id, field_name, field_type, analyzer, tokenizer,
			is_searchable, is_stored, is_required, is_indexed
		) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
	`,
		indexID,
		f.Name,
		string(f.Type),
		nullableString(f.Analyzer),
		nullableString(f.Tokenizer),
		boolToInt(f.Stored),
		boolToInt(f.Required),
		boolToInt(f.Indexed),
	)
	if err != nil {
		return fmt.Errorf("insert field %s: %w", f.Name, err)
	}
	return nil
}

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

