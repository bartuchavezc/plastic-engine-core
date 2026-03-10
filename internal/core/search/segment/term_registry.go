package segment

import (
	"context"
	"time"
)

// TermRegistry is the interface for the term registry.
// The FSTTermRegistry is the production implementation.
type TermRegistry interface {
	// Get retrieves a term ID if it exists.
	Get(ctx context.Context, field, term string) (string, bool)

	// GetOrCreate retrieves an existing term or creates a new one.
	GetOrCreate(ctx context.Context, field, term string) (string, error)

	// CreateAlias creates a new term that points to an existing term_id.
	CreateAlias(field, newTerm, existingTermID string) error

	// GetDF returns the global document frequency for a term.
	GetDF(termID string) int64

	// IncrementDF atomically increments the document frequency.
	IncrementDF(termID string, delta int64)

	// GetTotalDocs returns the total number of indexed documents.
	GetTotalDocs() int64

	// IncrementTotalDocs increments the total document count.
	IncrementTotalDocs(delta int64)

	// GetTermsWithPrefix returns all term IDs for terms that match a prefix.
	GetTermsWithPrefix(ctx context.Context, field, prefix string) []string

	// GetTermCount returns the total number of terms in the registry.
	GetTermCount() int64

	// Sync forces a sync to disk.
	Sync() error

	// Close closes the registry.
	Close() error
}

// TermInfo stores metadata about a term.
type TermInfo struct {
	TermID    string    `json:"term_id"`
	CreatedAt time.Time `json:"created_at"`
	IsAlias   bool      `json:"is_alias,omitempty"`
	AliasOf   string    `json:"alias_of,omitempty"` // Original term if this is an alias
}

// Compile-time check that PebbleTermRegistry implements TermRegistry
var _ TermRegistry = (*PebbleTermRegistry)(nil)
