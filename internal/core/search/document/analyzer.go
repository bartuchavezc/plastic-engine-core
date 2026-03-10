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
// Supported: "standard" (lowercase + stop words + stemming), "simple" (lowercase only).
// Names may include a language suffix: "standard.spanish", "standard.english".
// Empty name defaults to "standard".
func (f AnalyzerFactory) NewAnalyzer(name string) (Analyzer, error) {
	name = strings.ToLower(strings.TrimSpace(name))

	// Check for language suffix: "standard.spanish" → base="standard", lang="spanish"
	base, lang, _ := strings.Cut(name, ".")

	switch base {
	case "", "standard":
		return NewStandardAnalyzer(lang), nil
	case "simple":
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
