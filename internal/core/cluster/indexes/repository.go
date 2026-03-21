package indexes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SupportedTokenizers lists all valid tokenizer names that can be used in field mappings.
// These must match the tokenizers implemented in the search node's TokenizerFactory.
var SupportedTokenizers = map[string]bool{
	"":           true, // empty defaults to whitespace
	"whitespace": true,
	"standard":   true,
}

// SupportedAnalyzers lists all valid analyzer names that can be used in field mappings.
// These must match the analyzers implemented in the search node's AnalyzerFactory.
var SupportedAnalyzers = map[string]bool{
	"":         true, // empty defaults to simple
	"simple":   true,
	"standard": true,
}

// IsValidTokenizer checks if a tokenizer name is supported.
func IsValidTokenizer(name string) bool {
	return SupportedTokenizers[strings.ToLower(strings.TrimSpace(name))]
}

// IsValidAnalyzer checks if an analyzer name is supported.
func IsValidAnalyzer(name string) bool {
	return SupportedAnalyzers[strings.ToLower(strings.TrimSpace(name))]
}

// Repository persists and retrieves index definitions.
type Repository struct {
	db *sql.DB
}

// NewRepository constructs a Repository backed by the provided database.
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// CreateIndex stores a new index definition and associated field mappings.
func (r *Repository) CreateIndex(ctx context.Context, req CreateIndexRequest) (CreateIndexResponse, error) {
	NormalizeShardConfig(&req.ShardConfig, req.ShardStrategy)
	req.ShardStrategy = req.ShardConfig.Strategy

	if err := ValidateCreateRequest(req); err != nil {
		return CreateIndexResponse{}, err
	}

	now := time.Now().UTC()
	mappingVersion := req.MappingVersion
	if mappingVersion == 0 {
		mappingVersion = 1
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return CreateIndexResponse{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	encodedConfig, err := encodeShardConfig(req.ShardConfig)
	if err != nil {
		return CreateIndexResponse{}, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO indexes (
			id, name, shard_strategy, shard_template, shard_config,
			default_analyzer, default_tokenizer, mapping_version,
			cooccurrence_config, search_pipeline,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ID,
		req.Name,
		string(req.ShardStrategy),
		req.ShardTemplate,
		encodedConfig,
		req.DefaultAnalyzer,
		req.DefaultTokenizer,
		mappingVersion,
		nullableString(string(req.CooccurrenceConfigRaw)),
		nullableString(string(req.SearchPipelineRaw)),
		now,
		now,
	)
	if err != nil {
		return CreateIndexResponse{}, translateUniqueConstraint(err)
	}

	for _, field := range req.FieldMappings {
		if err := insertFieldMapping(ctx, tx, req.ID, field); err != nil {
			return CreateIndexResponse{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return CreateIndexResponse{}, fmt.Errorf("commit tx: %w", err)
	}

	def := IndexDefinition{
		ID:                    req.ID,
		Name:                  req.Name,
		ShardStrategy:         req.ShardStrategy,
		ShardTemplate:         req.ShardTemplate,
		ShardConfig:           req.ShardConfig,
		DefaultAnalyzer:       req.DefaultAnalyzer,
		DefaultTokenizer:      req.DefaultTokenizer,
		FieldMappings:         append([]FieldMapping(nil), req.FieldMappings...),
		MappingVersion:        mappingVersion,
		CooccurrenceConfigRaw: req.CooccurrenceConfigRaw,
		SearchPipelineRaw:     req.SearchPipelineRaw,
		CreatedAt:             now,
		UpdatedAt:             now,
	}

	return CreateIndexResponse{Definition: def}, nil
}

// GetIndex fetches an index definition by identifier, including field mappings.
func (r *Repository) GetIndex(ctx context.Context, id string) (IndexDefinition, error) {
	const query = `SELECT
		id, name, shard_strategy, shard_template, shard_config,
		default_analyzer, default_tokenizer, mapping_version,
		cooccurrence_config, search_pipeline,
		created_at, updated_at
	FROM indexes WHERE id = ?`

	var (
		def IndexDefinition
		err error
	)

	row := r.db.QueryRowContext(ctx, query, id)
	var rawConfig sql.NullString
	var rawCooccurrence, rawPipeline sql.NullString
	if scanErr := row.Scan(
		&def.ID,
		&def.Name,
		(*string)(&def.ShardStrategy),
		&def.ShardTemplate,
		&rawConfig,
		&def.DefaultAnalyzer,
		&def.DefaultTokenizer,
		&def.MappingVersion,
		&rawCooccurrence,
		&rawPipeline,
		&def.CreatedAt,
		&def.UpdatedAt,
	); scanErr != nil {
		if scanErr == sql.ErrNoRows {
			return IndexDefinition{}, ErrIndexNotFound
		}
		return IndexDefinition{}, fmt.Errorf("scan index: %w", scanErr)
	}

	fields, err := r.loadFieldMappings(ctx, id)
	if err != nil {
		return IndexDefinition{}, err
	}
	def.FieldMappings = fields

	if err := decodeShardConfigInto(&def, rawConfig.String); err != nil {
		return IndexDefinition{}, err
	}
	if rawCooccurrence.Valid && rawCooccurrence.String != "" {
		def.CooccurrenceConfigRaw = json.RawMessage(rawCooccurrence.String)
	}
	if rawPipeline.Valid && rawPipeline.String != "" {
		def.SearchPipelineRaw = json.RawMessage(rawPipeline.String)
	}

	return def, nil
}

// ListIndexes returns every index definition stored in the repository.
func (r *Repository) ListIndexes(ctx context.Context) ([]IndexDefinition, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT
		id, name, shard_strategy, shard_template, shard_config,
		default_analyzer, default_tokenizer, mapping_version,
		cooccurrence_config, search_pipeline,
		created_at, updated_at
		FROM indexes
		ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("query indexes: %w", err)
	}
	defer rows.Close()

	var (
		defs       []IndexDefinition
		rawConfigs = make(map[string]string)
	)
	for rows.Next() {
		var (
			def              IndexDefinition
			shardStrategy    string
			shardConfig      sql.NullString
			rawCooccurrence  sql.NullString
			rawPipeline      sql.NullString
		)
		if err := rows.Scan(
			&def.ID,
			&def.Name,
			&shardStrategy,
			&def.ShardTemplate,
			&shardConfig,
			&def.DefaultAnalyzer,
			&def.DefaultTokenizer,
			&def.MappingVersion,
			&rawCooccurrence,
			&rawPipeline,
			&def.CreatedAt,
			&def.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan index: %w", err)
		}

		def.ShardStrategy = ShardStrategy(shardStrategy)
		rawConfigs[def.ID] = shardConfig.String
		if rawCooccurrence.Valid && rawCooccurrence.String != "" {
			def.CooccurrenceConfigRaw = json.RawMessage(rawCooccurrence.String)
		}
		if rawPipeline.Valid && rawPipeline.String != "" {
			def.SearchPipelineRaw = json.RawMessage(rawPipeline.String)
		}
		defs = append(defs, def)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexes: %w", err)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close index rows: %w", err)
	}

	for i := range defs {
		fields, err := r.loadFieldMappings(ctx, defs[i].ID)
		if err != nil {
			return nil, fmt.Errorf("load field mappings %s: %w", defs[i].ID, err)
		}
		defs[i].FieldMappings = fields
		if err := decodeShardConfigInto(&defs[i], rawConfigs[defs[i].ID]); err != nil {
			return nil, fmt.Errorf("decode shard config %s: %w", defs[i].ID, err)
		}
	}

	return defs, nil
}

// CreateShards stores the provided shard specifications for an index.
func (r *Repository) CreateShards(ctx context.Context, shards []NewShard) ([]ShardRecord, error) {
	if len(shards) == 0 {
		return nil, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin shard tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	records := make([]ShardRecord, 0, len(shards))

	for _, shard := range shards {
		if strings.TrimSpace(shard.IndexID) == "" {
			return nil, fmt.Errorf("shard index id is required")
		}
		if strings.TrimSpace(shard.Key) == "" {
			return nil, fmt.Errorf("shard key is required")
		}

		id := uuid.NewString()
		_, err := tx.ExecContext(
			ctx,
			`INSERT INTO shards (
				id, index_id, shard_key, state, version, created_at, updated_at
			) VALUES (?, ?, ?, ?, 0, ?, ?)`,
			id,
			shard.IndexID,
			shard.Key,
			string(ShardStatePending),
			now,
			now,
		)
		if err != nil {
			return nil, fmt.Errorf("insert shard %s: %w", shard.Key, err)
		}

		records = append(records, ShardRecord{
			ID:           id,
			IndexID:      shard.IndexID,
			Key:          shard.Key,
			State:        ShardStatePending,
			Version:      0,
			CreatedAt:    now,
			LastModified: now,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit shard tx: %w", err)
	}

	return records, nil
}

// AssignPrimaryNodes updates the primary node for each shard, returning updated records.
func (r *Repository) AssignPrimaryNodes(ctx context.Context, assignments map[string]string) ([]ShardRecord, error) {
	if len(assignments) == 0 {
		return nil, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin assign tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	updated := make([]ShardRecord, 0, len(assignments))

	for shardID, nodeID := range assignments {
		if strings.TrimSpace(shardID) == "" {
			return nil, fmt.Errorf("assignment missing shard id")
		}
		res, err := tx.ExecContext(
			ctx,
			`UPDATE shards
			 SET primary_node = ?, state = ?, updated_at = ?
			 WHERE id = ?`,
			nullableString(nodeID),
			string(ShardStateAssigned),
			now,
			shardID,
		)
		if err != nil {
			return nil, fmt.Errorf("assign shard %s: %w", shardID, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("rows affected shard %s: %w", shardID, err)
		}
		if affected == 0 {
			return nil, fmt.Errorf("shard %s not found", shardID)
		}

		record, err := r.loadShard(ctx, tx, shardID)
		if err != nil {
			return nil, err
		}
		updated = append(updated, record)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit assign tx: %w", err)
	}

	return updated, nil
}

func (r *Repository) loadShard(ctx context.Context, tx *sql.Tx, shardID string) (ShardRecord, error) {
	row := tx.QueryRowContext(ctx, `SELECT
		id, index_id, shard_key, primary_node, state, version, created_at, COALESCE(updated_at, created_at)
		FROM shards WHERE id = ?`, shardID)

	var record ShardRecord
	var state string
	if err := row.Scan(
		&record.ID,
		&record.IndexID,
		&record.Key,
		&record.PrimaryNode,
		&state,
		&record.Version,
		&record.CreatedAt,
		&record.LastModified,
	); err != nil {
		if err == sql.ErrNoRows {
			return ShardRecord{}, fmt.Errorf("shard %s not found", shardID)
		}
		return ShardRecord{}, fmt.Errorf("scan shard %s: %w", shardID, err)
	}

	record.State = ShardState(state)
	return record, nil
}

// ListShardsForNode returns shards assigned as primary to the given node.
func (r *Repository) ListShardsForNode(ctx context.Context, nodeID string) ([]ShardRecord, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT
		id, index_id, shard_key, state, version, created_at, COALESCE(updated_at, created_at)
		FROM shards WHERE primary_node = ?
		ORDER BY created_at`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("query shards for node: %w", err)
	}
	defer rows.Close()

	var shards []ShardRecord
	for rows.Next() {
		var record ShardRecord
		var state string
		if err := rows.Scan(
			&record.ID,
			&record.IndexID,
			&record.Key,
			&state,
			&record.Version,
			&record.CreatedAt,
			&record.LastModified,
		); err != nil {
			return nil, fmt.Errorf("scan shard: %w", err)
		}
		record.State = ShardState(state)
		record.PrimaryNode = nodeID
		shards = append(shards, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate shards: %w", err)
	}

	return shards, nil
}

// ValidateCreateRequest validates the fields of a CreateIndexRequest.
// This is exported so it can be used by the Coordinator before creating commands.
func ValidateCreateRequest(req CreateIndexRequest) error {
	switch {
	case strings.TrimSpace(req.ID) == "":
		return NewValidationError("index id is required")
	case strings.TrimSpace(req.Name) == "":
		return NewValidationError("index name is required")
	case strings.TrimSpace(string(req.ShardStrategy)) == "":
		return NewValidationError("shard strategy is required")
	case strings.TrimSpace(req.DefaultAnalyzer) == "":
		return NewValidationError("default analyzer is required")
	case strings.TrimSpace(req.DefaultTokenizer) == "":
		return NewValidationError("default tokenizer is required")
	}

	fieldTypes := make(map[string]FieldType, len(req.FieldMappings))
	for _, field := range req.FieldMappings {
		if strings.TrimSpace(field.Name) == "" {
			return NewValidationError("field name is required")
		}
		if strings.TrimSpace(string(field.Type)) == "" {
			return NewValidationError(fmt.Sprintf("field type is required for field %q", field.Name))
		}
		fieldTypes[field.Name] = field.Type
	}

	switch req.ShardConfig.Strategy {
	case ShardStrategyAutomatic:
		if req.ShardConfig.Automatic == nil {
			return NewValidationError("automatic shard configuration is required")
		}
		if req.ShardConfig.Automatic.ShardCount < 1 {
			return NewValidationError("automatic shard count must be at least 1")
		}
		if field := req.ShardConfig.Automatic.Field; field != "" {
			if _, ok := fieldTypes[field]; !ok {
				return NewValidationError(fmt.Sprintf("automatic shard field %q is not defined in mappings", field))
			}
		}
	case ShardStrategyDate:
		cfg := req.ShardConfig.Date
		if cfg == nil {
			return NewValidationError("date shard configuration is required")
		}
		if cfg.Field == "" {
			return NewValidationError("date shard field is required")
		}
		if ft, ok := fieldTypes[cfg.Field]; !ok {
			return NewValidationError(fmt.Sprintf("date shard field %q is not defined in mappings", cfg.Field))
		} else if ft != FieldTypeDate {
			return NewValidationError(fmt.Sprintf("date shard field %q must be of type date", cfg.Field))
		}
		switch cfg.Granularity {
		case DateGranularityYear, DateGranularityMonth, DateGranularityDay:
		default:
			return NewValidationError(fmt.Sprintf("unsupported date granularity %q", cfg.Granularity))
		}
	case ShardStrategyComputed:
		cfg := req.ShardConfig.Computed
		if cfg == nil || len(cfg.Components) == 0 {
			return NewValidationError("computed shard configuration requires at least one component")
		}
		for _, comp := range cfg.Components {
			if comp.Field == "" {
				return NewValidationError("computed shard component field is required")
			}
			ft, ok := fieldTypes[comp.Field]
			if !ok {
				return NewValidationError(fmt.Sprintf("computed shard field %q is not defined in mappings", comp.Field))
			}
			switch comp.Transform {
			case ComputedTransformExact:
			case ComputedTransformSlug:
				if ft != FieldTypeKeyword && ft != FieldTypeText {
					return NewValidationError(fmt.Sprintf("slug transform requires keyword/text field %q", comp.Field))
				}
			default:
				return NewValidationError(fmt.Sprintf("unsupported computed transform %q", comp.Transform))
			}
		}
	default:
		return NewValidationError(fmt.Sprintf("unsupported shard strategy %q", req.ShardConfig.Strategy))
	}

	// Validate default tokenizer and analyzer
	if !IsValidTokenizer(req.DefaultTokenizer) {
		return NewValidationError(fmt.Sprintf("unsupported default tokenizer %q", req.DefaultTokenizer))
	}
	if !IsValidAnalyzer(req.DefaultAnalyzer) {
		return NewValidationError(fmt.Sprintf("unsupported default analyzer %q", req.DefaultAnalyzer))
	}

	// Per-field analyzer/tokenizer overrides are not supported.
	// All text fields use the index-level DefaultAnalyzer and DefaultTokenizer.
	for _, field := range req.FieldMappings {
		if field.Analyzer != "" {
			return NewValidationError(fmt.Sprintf("per-field analyzer is not supported (field %q); use default_analyzer at index level", field.Name))
		}
		if field.Tokenizer != "" {
			return NewValidationError(fmt.Sprintf("per-field tokenizer is not supported (field %q); use default_tokenizer at index level", field.Name))
		}
	}

	return nil
}

// NormalizeShardConfig applies default values to a shard configuration.
// This should be called before validation to ensure required fields are set.
func NormalizeShardConfig(cfg *ShardConfig, fallback ShardStrategy) {
	if cfg == nil {
		return
	}
	if cfg.Strategy == "" {
		if fallback != "" {
			cfg.Strategy = fallback
		} else {
			cfg.Strategy = ShardStrategyAutomatic
		}
	}

	switch cfg.Strategy {
	case ShardStrategyAutomatic:
		if cfg.Automatic == nil {
			cfg.Automatic = &AutomaticShardConfig{}
		}
		cfg.Automatic.Field = strings.TrimSpace(cfg.Automatic.Field)
		if cfg.Automatic.ShardCount < 1 {
			cfg.Automatic.ShardCount = 1
		}
		cfg.Date = nil
		cfg.Computed = nil
	case ShardStrategyDate:
		if cfg.Date == nil {
			cfg.Date = &DateShardConfig{}
		}
		cfg.Date.Field = strings.TrimSpace(cfg.Date.Field)
		if cfg.Date.Granularity == "" {
			cfg.Date.Granularity = DateGranularityMonth
		}
		cfg.Automatic = nil
		cfg.Computed = nil
	case ShardStrategyComputed:
		if cfg.Computed == nil {
			cfg.Computed = &ComputedShardConfig{}
		}
		for i := range cfg.Computed.Components {
			cfg.Computed.Components[i].Field = strings.TrimSpace(cfg.Computed.Components[i].Field)
			if cfg.Computed.Components[i].Transform == "" {
				cfg.Computed.Components[i].Transform = ComputedTransformExact
			}
		}
		cfg.Automatic = nil
		cfg.Date = nil
	default:
		// leave as-is, validation will reject unsupported strategy.
	}
}

func encodeShardConfig(cfg ShardConfig) (string, error) {
	if cfg.Strategy == "" {
		return "", nil
	}
	bytes, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal shard config: %w", err)
	}
	return string(bytes), nil
}

func decodeShardConfigInto(def *IndexDefinition, raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		cfg := ShardConfig{Strategy: def.ShardStrategy}
		NormalizeShardConfig(&cfg, def.ShardStrategy)
		def.ShardConfig = cfg
		return nil
	}

	var cfg ShardConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return fmt.Errorf("unmarshal shard config: %w", err)
	}
	NormalizeShardConfig(&cfg, cfg.Strategy)
	def.ShardConfig = cfg
	def.ShardStrategy = cfg.Strategy
	return nil
}

func insertFieldMapping(ctx context.Context, tx *sql.Tx, indexID string, field FieldMapping) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO index_fields (
			index_id, field_name, field_type, analyzer, tokenizer,
			is_searchable, is_stored, is_required, is_indexed
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		indexID,
		field.Name,
		string(field.Type),
		nullableString(field.Analyzer),
		nullableString(field.Tokenizer),
		1,
		boolToInt(field.Stored),
		boolToInt(field.Required),
		boolToInt(field.Indexed),
	)
	if err != nil {
		return fmt.Errorf("insert field mapping %s: %w", field.Name, err)
	}
	return nil
}

func (r *Repository) loadFieldMappings(ctx context.Context, indexID string) ([]FieldMapping, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT
		field_name, field_type, analyzer, tokenizer,
		is_searchable, is_stored, is_required, is_indexed
	FROM index_fields WHERE index_id = ? ORDER BY field_name`, indexID)
	if err != nil {
		return nil, fmt.Errorf("query field mappings: %w", err)
	}
	defer rows.Close()

	var mappings []FieldMapping
	for rows.Next() {
		var (
			field    FieldMapping
			ft       string
			an       sql.NullString
			tok      sql.NullString
			dummy    int
			stored   int
			required int
			indexed  int
		)

		if err := rows.Scan(
			&field.Name,
			&ft,
			&an,
			&tok,
			&dummy,
			&stored,
			&required,
			&indexed,
		); err != nil {
			return nil, fmt.Errorf("scan field mapping: %w", err)
		}

		field.Type = FieldType(ft)
		if an.Valid {
			field.Analyzer = an.String
		}
		if tok.Valid {
			field.Tokenizer = tok.String
		}
		field.Stored = stored == 1
		field.Required = required == 1
		field.Indexed = indexed == 1
		mappings = append(mappings, field)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate field mappings: %w", err)
	}

	return mappings, nil
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func translateUniqueConstraint(err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "indexes.id"):
		return ErrIndexIDExists
	case strings.Contains(msg, "indexes.name"):
		return ErrIndexNameExists
	default:
		return fmt.Errorf("insert index: %w", err)
	}
}
