package pebble

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Key prefixes for different data structures in the shard.
const (
	PrefixTermRegistry = "term:"
	PrefixInverted     = "inv:"
	PrefixNgram        = "ngram:"
	PrefixForward      = "fwd:"
	PrefixMeta         = "meta:"
)

// Metadata key suffixes.
const (
	MetaDocCount   = "doc_count"
	MetaAvgDocLen  = "avg_doc_len"
	MetaFieldDocs  = "field_docs"
	MetaTermDF     = "term_df"
)

// --------------------------------------------------------------------------
// Term Registry keys
// --------------------------------------------------------------------------

// TermRegistryKey builds a key for the term registry: term:{field}:{term}
func TermRegistryKey(field, term string) string {
	return PrefixTermRegistry + field + ":" + term
}

// TermRegistryPrefix returns the prefix for all terms in a field: term:{field}:
func TermRegistryPrefix(field string) string {
	return PrefixTermRegistry + field + ":"
}

// ParseTermRegistryKey extracts field and term from a term registry key.
// Returns empty strings and false if the key is malformed.
func ParseTermRegistryKey(key string) (field, term string, ok bool) {
	if !strings.HasPrefix(key, PrefixTermRegistry) {
		return "", "", false
	}
	rest := key[len(PrefixTermRegistry):]
	idx := strings.Index(rest, ":")
	if idx < 0 {
		return "", "", false
	}
	return rest[:idx], rest[idx+1:], true
}

// --------------------------------------------------------------------------
// Term ID generation
// --------------------------------------------------------------------------

// GenerateTermID creates a deterministic, short ID from field+term using SHA256.
// Format: t_{16 hex chars} (64 bits of entropy)
func GenerateTermID(field, term string) string {
	input := field + "\x00" + term // null separator prevents collisions
	hash := sha256.Sum256([]byte(input))
	return "t_" + hex.EncodeToString(hash[:8])
}

// --------------------------------------------------------------------------
// Inverted Index keys (by term_id)
// --------------------------------------------------------------------------

// InvertedKey builds a key for an inverted index entry: inv:{term_id}:{docID}
func InvertedKey(termID, docID string) string {
	return PrefixInverted + termID + ":" + docID
}

// InvertedPrefix returns the prefix for all postings of a term: inv:{term_id}:
func InvertedPrefix(termID string) string {
	return PrefixInverted + termID + ":"
}

// ParseInvertedKey extracts termID and docID from an inverted index key.
// Returns empty strings and false if the key is malformed.
func ParseInvertedKey(key string) (termID, docID string, ok bool) {
	if !strings.HasPrefix(key, PrefixInverted) {
		return "", "", false
	}
	rest := key[len(PrefixInverted):]
	idx := strings.Index(rest, ":")
	if idx < 0 {
		return "", "", false
	}
	return rest[:idx], rest[idx+1:], true
}

// --------------------------------------------------------------------------
// N-gram keys (for prefix search)
// --------------------------------------------------------------------------

// NgramKey builds a key for an n-gram entry: ngram:{field}:{prefix}:{term_id}
func NgramKey(field, prefix, termID string) string {
	return PrefixNgram + field + ":" + prefix + ":" + termID
}

// NgramPrefix returns the prefix for scanning n-grams: ngram:{field}:{prefix}:
func NgramPrefix(field, prefix string) string {
	return PrefixNgram + field + ":" + prefix + ":"
}

// ParseNgramKey extracts field, prefix, and termID from an n-gram key.
// Returns empty strings and false if the key is malformed.
func ParseNgramKey(key string) (field, prefix, termID string, ok bool) {
	if !strings.HasPrefix(key, PrefixNgram) {
		return "", "", "", false
	}
	rest := key[len(PrefixNgram):]
	
	// Find first colon (after field)
	idx1 := strings.Index(rest, ":")
	if idx1 < 0 {
		return "", "", "", false
	}
	field = rest[:idx1]
	rest = rest[idx1+1:]
	
	// Find second colon (after prefix)
	idx2 := strings.Index(rest, ":")
	if idx2 < 0 {
		return "", "", "", false
	}
	prefix = rest[:idx2]
	termID = rest[idx2+1:]
	
	return field, prefix, termID, true
}

// --------------------------------------------------------------------------
// Forward Index keys
// --------------------------------------------------------------------------

// ForwardKey builds a key for the forward index: fwd:{docID}
func ForwardKey(docID string) string {
	return PrefixForward + docID
}

// ParseForwardKey extracts docID from a forward index key.
func ParseForwardKey(key string) (docID string, ok bool) {
	if !strings.HasPrefix(key, PrefixForward) {
		return "", false
	}
	return key[len(PrefixForward):], true
}

// --------------------------------------------------------------------------
// Metadata keys
// --------------------------------------------------------------------------

// DocCountKey returns the key for total document count: meta:doc_count
func DocCountKey() string {
	return PrefixMeta + MetaDocCount
}

// AvgDocLenKey returns the key for average document length per field.
// Format: meta:avg_doc_len:{field}
func AvgDocLenKey(field string) string {
	return PrefixMeta + MetaAvgDocLen + ":" + field
}

// FieldDocsKey returns the key for document count per field.
// Format: meta:field_docs:{field}
func FieldDocsKey(field string) string {
	return PrefixMeta + MetaFieldDocs + ":" + field
}

// TotalTermLenKey returns the key for sum of term lengths per field.
// Used to compute avg_doc_len: total_term_len / field_docs
func TotalTermLenKey(field string) string {
	return PrefixMeta + "total_term_len:" + field
}
