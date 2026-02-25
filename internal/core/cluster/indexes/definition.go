package indexes

import "time"

// ShardStrategy represents how documents will be distributed across shards.
type ShardStrategy string

const (
	// ShardStrategyAutomatic hashes documents into a configurable pool of shards.
	ShardStrategyAutomatic ShardStrategy = "automatic"
	// ShardStrategyDate groups documents by date windows (month/day/year).
	ShardStrategyDate ShardStrategy = "date"
	// ShardStrategyComputed builds composite keys out of specific fields.
	ShardStrategyComputed ShardStrategy = "computed"
)

// FieldType describes the semantics of a field stored in the index.
type FieldType string

const (
	// FieldTypeText stores analysed full-text content.
	FieldTypeText FieldType = "text"
	// FieldTypeKeyword stores exact-match strings.
	FieldTypeKeyword FieldType = "keyword"
	// FieldTypeInteger stores integer values for range/aggregations.
	FieldTypeInteger FieldType = "integer"
	// FieldTypeDate stores temporal values.
	FieldTypeDate FieldType = "date"
)

// FieldMapping controls how a field is indexed and stored.
type FieldMapping struct {
	Name      string
	Type      FieldType
	Analyzer  string
	Tokenizer string
	Stored    bool
	Required  bool
	Indexed   bool
}

// ShardConfig describes how shard keys should be derived.
type ShardConfig struct {
	Strategy  ShardStrategy
	Automatic *AutomaticShardConfig
	Date      *DateShardConfig
	Computed  *ComputedShardConfig
}

// AutomaticShardConfig holds configuration for the automatic strategy.
type AutomaticShardConfig struct {
	ShardCount int
	Field      string
}

// DateShardConfig defines the field and granularity for date-based sharding.
type DateShardConfig struct {
	Field       string
	Granularity DateGranularity
}

// DateGranularity describes the temporal bucket used by the date strategy.
type DateGranularity string

const (
	// DateGranularityYear groups documents by year.
	DateGranularityYear DateGranularity = "year"
	// DateGranularityMonth groups documents by month.
	DateGranularityMonth DateGranularity = "month"
	// DateGranularityDay groups documents by day.
	DateGranularityDay DateGranularity = "day"
)

// ComputedShardConfig combines multiple document fields to compose the shard key.
type ComputedShardConfig struct {
	Components []ComputedComponent
}

// ComputedComponent indicates how a particular field participates in the shard key.
type ComputedComponent struct {
	Field     string
	Transform ComputedTransform
}

// ComputedTransform determines how the field value is normalized.
type ComputedTransform string

const (
	// ComputedTransformExact uses the raw value (lowercased and trimmed).
	ComputedTransformExact ComputedTransform = "exact"
	// ComputedTransformSlug applies slug transformation replacing separators with dashes.
	ComputedTransformSlug ComputedTransform = "slug"
)

// NgramConfig defines the parameters for edge n-gram generation.
type NgramConfig struct {
	Enabled   bool `json:"enabled"`
	MinLength int  `json:"min_length"`
	MaxLength int  `json:"max_length"`
}

// DefaultNgramConfig returns the default n-gram configuration.
func DefaultNgramConfig() NgramConfig {
	return NgramConfig{
		Enabled:   true,
		MinLength: 5,
		MaxLength: 10,
	}
}

// Validate ensures the n-gram configuration is valid.
func (c NgramConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.MinLength < 1 {
		return NewValidationError("ngram min_length must be at least 1")
	}
	if c.MaxLength < c.MinLength {
		return NewValidationError("ngram max_length must be >= min_length")
	}
	if c.MaxLength > 50 {
		return NewValidationError("ngram max_length cannot exceed 50")
	}
	return nil
}

// IndexDefinition captures the full set of configuration for an index.
type IndexDefinition struct {
	ID               string
	Name             string
	ShardStrategy    ShardStrategy
	ShardTemplate    string
	ShardConfig      ShardConfig
	DefaultAnalyzer  string
	DefaultTokenizer string
	FieldMappings    []FieldMapping
	MappingVersion   int
	NgramConfig      NgramConfig
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// CreateIndexRequest encapsulates the required information to register a new index.
type CreateIndexRequest struct {
	ID               string
	Name             string
	ShardStrategy    ShardStrategy
	ShardTemplate    string
	ShardConfig      ShardConfig
	DefaultAnalyzer  string
	DefaultTokenizer string
	FieldMappings    []FieldMapping
	MappingVersion   int
	NgramConfig      *NgramConfig // nil means use default
	InitialShardKeys []string
}

// CreateIndexResponse returns the stored index definition.
type CreateIndexResponse struct {
	Definition IndexDefinition
}
