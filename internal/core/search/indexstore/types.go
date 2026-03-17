package indexstore

import (
	"strconv"

	pebbledb "github.com/cockroachdb/pebble"
)

// Hit represents a search result.
type Hit struct {
	DocID     string
	TermID    string
	TF        int
	DocLen    int // unique terms in field (0 = unknown/pre-migration)
	Positions []int
	SegmentID string
}

// TermEntry represents a term with its metadata for listing/search operations.
type TermEntry struct {
	Field  string `json:"field"`
	Term   string `json:"term"`
	TermID string `json:"term_id"`
	DF     int64  `json:"df,omitempty"`
}

// Config holds configuration for the segment manager.
type Config struct {
	// DataDir is the directory for all data files.
	DataDir string

	// TermRegistry is an optional external term registry to use.
	// If provided, the Manager will use this registry instead of creating its own.
	TermRegistry TermRegistry

	// PostingStoreConfig configures the PebblePostingStore.
	PostingStoreConfig PostingStoreConfig

	// CooccurrenceConfig configures the co-occurrence accumulator.
	CooccurrenceConfig CooccurrenceConfig

	// Cache is an optional shared Pebble block cache for all Pebble instances.
	Cache *pebbledb.Cache
}

// DamerauLevenshteinDistance computes the optimal string alignment distance
// (restricted Damerau-Levenshtein) between two strings.
// It supports insertions, deletions, substitutions, and adjacent transpositions.
func DamerauLevenshteinDistance(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	// Quick length-difference check: if lengths differ by more than maxDistance
	// the caller will check, we still compute the full distance.
	pprev := make([]int, lb+1) // row i-2
	prev := make([]int, lb+1)  // row i-1
	curr := make([]int, lb+1)  // row i

	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			// Transposition: swap adjacent characters
			if i > 1 && j > 1 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				trans := pprev[j-2] + cost
				if trans < m {
					m = trans
				}
			}
			curr[j] = m
		}
		pprev, prev, curr = prev, curr, pprev
	}
	return prev[lb]
}

// FuzzyPrefixes generates prefix candidates for efficient fuzzy candidate scanning.
// For a query term, it produces short prefixes that cover likely edit positions.
func FuzzyPrefixes(query string, maxDistance int) []string {
	runes := []rune(query)
	n := len(runes)
	if n < 2 {
		return []string{string(runes)}
	}

	set := make(map[string]struct{})

	// Original prefixes of decreasing length
	for plen := min(n, 4); plen >= max(2, 4-maxDistance); plen-- {
		set[string(runes[:plen])] = struct{}{}
	}

	// Transposition of first 2 chars (covers typos at position 0-1)
	transposed := []rune{runes[1], runes[0]}
	if n >= 3 {
		transposed = append(transposed, runes[2])
	}
	set[string(transposed)] = struct{}{}

	// Skip first char (covers deletion/substitution at position 0)
	if n >= 3 {
		set[string(runes[1:min(n, 4)])] = struct{}{}
	}

	prefixes := make([]string, 0, len(set))
	for p := range set {
		prefixes = append(prefixes, p)
	}
	return prefixes
}

// RegistryStats holds a snapshot of registry statistics.
type RegistryStats struct {
	FSTTermCount   int64 `json:"fst_term_count"`
	NewTermCount   int64 `json:"new_term_count"`
	TotalTermCount int64 `json:"total_term_count"`
	TotalDocs      int64 `json:"total_docs"`
	IsMerging      bool  `json:"is_merging"`
}

// formatTermID converts a counter to a term ID string.
func formatTermID(counter uint64) string {
	return "t" + strconv.FormatUint(counter, 10)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		DataDir:            "segments",
		CooccurrenceConfig: DefaultCooccurrenceConfig(),
	}
}
