package segment

import (
	"time"
)

// Posting stores the posting data for a term in a document.
type Posting struct {
	DocID     string `json:"doc_id"`
	TF        int    `json:"tf"`                  // Term frequency
	Positions []int  `json:"positions,omitempty"` // Token positions
}

// TermDictEntry represents an entry in the term dictionary.
type TermDictEntry struct {
	TermID       string `json:"term_id"`
	PostingOffset int64  `json:"offset"`  // Offset in posting list section
	DF           int64  `json:"df"`       // Document frequency in this segment
	PostingCount int    `json:"count"`    // Number of postings
}

// SegmentMeta contains metadata about a segment.
type SegmentMeta struct {
	ID         string    `json:"id"`
	Version    int       `json:"version"`
	DocCount   int       `json:"doc_count"`
	TermCount  int       `json:"term_count"`
	CreatedAt  time.Time `json:"created_at"`
	MinDocID   string    `json:"min_doc_id,omitempty"`
	MaxDocID   string    `json:"max_doc_id,omitempty"`
	SizeBytes  int64     `json:"size_bytes"`
	Level      int       `json:"level"` // Tier level for merge policy
}

// Hit represents a search result from a segment.
type Hit struct {
	DocID     string
	TermID    string
	TF        int
	Positions []int
	SegmentID string
}

// Segment is the interface for both memory and disk segments.
type Segment interface {
	// ID returns the segment identifier.
	ID() string

	// Meta returns segment metadata.
	Meta() SegmentMeta

	// Search finds all postings for a term.
	Search(termID string) []Hit

	// GetLocalDF returns the document frequency for a term in this segment.
	GetLocalDF(termID string) int64

	// DocCount returns the number of documents in this segment.
	DocCount() int

	// Close releases resources.
	Close() error
}

// SegmentWriter writes a new segment to disk.
type SegmentWriter interface {
	// AddTerm adds a term with its postings to the segment.
	AddTerm(termID string, postings []Posting, df int64) error

	// SetMeta sets the segment metadata.
	SetMeta(meta SegmentMeta)

	// Finalize completes the segment and returns the path.
	Finalize() (string, error)

	// Abort discards the segment being written.
	Abort() error
}

// PostingList is an ordered list of postings for a term.
type PostingList struct {
	TermID   string
	Postings []Posting
}

// SegmentIterator iterates over terms in a segment.
type SegmentIterator interface {
	// Next advances to the next term. Returns false when exhausted.
	Next() bool

	// Term returns the current term ID.
	Term() string

	// Postings returns the postings for the current term.
	Postings() []Posting

	// DF returns the document frequency for the current term.
	DF() int64

	// Close releases iterator resources.
	Close() error
}

// MergeCandidate represents segments that should be merged.
type MergeCandidate struct {
	Segments []*DiskSegment
	Level    int
}

// Config holds configuration for segment operations.
type Config struct {
	// FlushThreshold is the number of documents before flushing to disk.
	FlushThreshold int

	// FlushThresholdBytes is the estimated memory size before flushing to disk.
	// This acts as a safety net for documents with many fields/terms.
	// Default: 32MB. Set to 0 to disable byte-based flushing.
	FlushThresholdBytes int64

	// FlushInterval is how often the periodic flush check runs.
	// Default: 10s.
	FlushInterval time.Duration

	// MaxSegmentsPerLevel is the maximum segments before triggering merge.
	MaxSegmentsPerLevel int

	// LevelSizeMultiplier is how much larger each level is.
	LevelSizeMultiplier int

	// MergeWorkers is the number of dedicated merge goroutines.
	// Default: 2.
	MergeWorkers int

	// MergeInterval is how often the merge scheduler checks for work.
	// Default: 30s.
	MergeInterval time.Duration

	// DataDir is the directory for segment files.
	DataDir string

	// FSTRebuildThreshold is the number of pending terms before auto-rebuilding FST.
	// Default: 1000
	// Deprecated: Use TermRegistryMergeConfig instead.
	FSTRebuildThreshold int

	// TermRegistryMergeConfig configures the Cache+FST term registry.
	// If nil, DefaultMergeConfig() is used.
	TermRegistryMergeConfig *MergeConfig

	// TermRegistry is an optional external term registry to use.
	// If provided, the Manager will use this registry instead of creating its own.
	// This allows sharing a single registry across multiple shards (recommended).
	TermRegistry TermRegistry
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		FlushThreshold:      5000,              // 5k docs before flush
		FlushThresholdBytes: 16 * 1024 * 1024,   // 16MB estimated memory (keep MemSegment small on 2GB containers)
		FlushInterval:       10 * time.Second,   // Check every 10s
		MaxSegmentsPerLevel: 5,                  // Keep low for search performance
		LevelSizeMultiplier: 10,
		MergeWorkers:        2,                  // 2 dedicated merge goroutines
		MergeInterval:       30 * time.Second,   // Check every 30s
		DataDir:             "segments",
		FSTRebuildThreshold: 1000,
	}
}
