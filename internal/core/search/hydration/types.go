package hydration

import (
	"context"
	"time"
)

// Document represents a hydrated document from external storage.
type Document struct {
	ID       string         `json:"id"`
	Source   map[string]any `json:"_source"`
	Metadata DocumentMeta   `json:"_metadata,omitempty"`
	Found    bool           `json:"found"`
}

// DocumentMeta contains metadata about the stored document.
type DocumentMeta struct {
	StoredAt    time.Time `json:"stored_at,omitempty"`
	Size        int64     `json:"size,omitempty"`
	ContentType string    `json:"content_type,omitempty"`
}

// Connector defines how to fetch documents from external storage.
// Implementations can be HTTP, MongoDB, S3, Redis, Internal, etc.
type Connector interface {
	// Name returns the connector type identifier.
	Name() string

	// Fetch retrieves a single document by key.
	// The key is extracted from the indexed document using KeyField.
	Fetch(ctx context.Context, key string) (Document, error)

	// FetchBatch retrieves multiple documents efficiently.
	// Returns a map of key -> Document. Missing docs have Found=false.
	FetchBatch(ctx context.Context, keys []string) (map[string]Document, error)

	// Store saves a document (only for connectors that support it).
	// Returns ErrStoreNotSupported if the connector is read-only.
	Store(ctx context.Context, key string, source map[string]any) error

	// Delete removes a document (only for connectors that support it).
	Delete(ctx context.Context, key string) error

	// Close releases any resources held by the connector.
	Close() error
}

// Config defines hydration settings for an index.
type Config struct {
	Enabled   bool              `json:"enabled"`
	Connector string            `json:"connector"`  // "internal", "http", "mongodb", "s3", etc.
	KeyField  string            `json:"key_field"`  // Field in indexed doc to use as lookup key
	Settings  map[string]string `json:"settings"`   // Connector-specific configuration
}

// DefaultConfig returns a disabled hydration config.
func DefaultConfig() Config {
	return Config{
		Enabled:   false,
		Connector: "",
		KeyField:  "_id",
		Settings:  nil,
	}
}

// InternalConfig returns config for the internal connector.
func InternalConfig() Config {
	return Config{
		Enabled:   true,
		Connector: "internal",
		KeyField:  "_id",
		Settings:  nil,
	}
}

// Validate checks if the config is valid.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Connector == "" {
		return ErrConnectorRequired
	}
	if c.KeyField == "" {
		return ErrKeyFieldRequired
	}
	return nil
}

// Error types for hydration.
type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ErrConnectorRequired  Error = "hydration connector is required when enabled"
	ErrKeyFieldRequired   Error = "hydration key_field is required"
	ErrConnectorNotFound  Error = "hydration connector not found"
	ErrDocumentNotFound   Error = "document not found"
	ErrStoreNotSupported  Error = "store operation not supported by this connector"
	ErrDeleteNotSupported Error = "delete operation not supported by this connector"
	ErrFetchFailed        Error = "failed to fetch document"
)

