package document

import (
	"unicode/utf8"
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
		MinLength: 2,
		MaxLength: 10,
	}
}

// NgramConfigDisabled returns a config with n-grams disabled.
func NgramConfigDisabled() NgramConfig {
	return NgramConfig{
		Enabled:   false,
		MinLength: 0,
		MaxLength: 0,
	}
}

// Validate ensures the configuration is valid.
func (c NgramConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.MinLength < 1 {
		return ErrNgramMinLengthInvalid
	}
	if c.MaxLength < c.MinLength {
		return ErrNgramMaxLengthInvalid
	}
	if c.MaxLength > 50 {
		return ErrNgramMaxLengthTooLarge
	}
	return nil
}

// GenerateEdgeNgrams creates prefix n-grams for a term.
// For "hello" with min=2, max=5: ["he", "hel", "hell", "hello"]
// Uses rune-aware slicing to handle UTF-8 correctly.
func GenerateEdgeNgrams(term string, cfg NgramConfig) []string {
	if !cfg.Enabled {
		return nil
	}

	runes := []rune(term)
	runeCount := len(runes)

	if runeCount < cfg.MinLength {
		return nil
	}

	maxLen := cfg.MaxLength
	if runeCount < maxLen {
		maxLen = runeCount
	}

	ngrams := make([]string, 0, maxLen-cfg.MinLength+1)
	for n := cfg.MinLength; n <= maxLen; n++ {
		ngrams = append(ngrams, string(runes[:n]))
	}

	return ngrams
}

// GenerateEdgeNgramsBytes is like GenerateEdgeNgrams but returns byte counts.
// Useful for generating keys where we need the exact byte prefix.
func GenerateEdgeNgramsBytes(term string, cfg NgramConfig) []string {
	if !cfg.Enabled {
		return nil
	}

	// For ASCII-only strings, this is equivalent to GenerateEdgeNgrams
	if isASCII(term) {
		return generateASCIINgrams(term, cfg)
	}

	// For UTF-8, use rune-aware slicing
	return GenerateEdgeNgrams(term, cfg)
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func generateASCIINgrams(term string, cfg NgramConfig) []string {
	length := len(term)
	if length < cfg.MinLength {
		return nil
	}

	maxLen := cfg.MaxLength
	if length < maxLen {
		maxLen = length
	}

	ngrams := make([]string, 0, maxLen-cfg.MinLength+1)
	for n := cfg.MinLength; n <= maxLen; n++ {
		ngrams = append(ngrams, term[:n])
	}

	return ngrams
}

// Error types for ngram validation.
type NgramError string

func (e NgramError) Error() string {
	return string(e)
}

const (
	ErrNgramMinLengthInvalid  NgramError = "ngram min_length must be at least 1"
	ErrNgramMaxLengthInvalid  NgramError = "ngram max_length must be >= min_length"
	ErrNgramMaxLengthTooLarge NgramError = "ngram max_length cannot exceed 50"
)

