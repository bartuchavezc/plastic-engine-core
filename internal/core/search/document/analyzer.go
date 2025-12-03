package document

import (
	"fmt"
	"strings"
)

// Analyzer applies normalization steps over tokens.
type Analyzer interface {
	Analyze(tokens []Token) []Token
}

// AnalyzerFactory resolves analyzers by name.
type AnalyzerFactory struct{}

// NewAnalyzer builds an analyzer by identifier.
func (f AnalyzerFactory) NewAnalyzer(name string) (Analyzer, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "simple":
		return SimpleAnalyzer{}, nil
	default:
		return nil, fmt.Errorf("unsupported analyzer %q", name)
	}
}

// SimpleAnalyzer lowercases tokens; placeholder for future normalization.
type SimpleAnalyzer struct{}

func (SimpleAnalyzer) Analyze(tokens []Token) []Token {
	for i := range tokens {
		tokens[i].Term = strings.ToLower(tokens[i].Term)
	}
	return tokens
}
