package query

import (
	"plastic-engine-core/internal/core/search/document"
)

// StandardQueryAnalyzer applies the same tokenization + analysis pipeline
// used at index time to query text. This ensures that stemmed/normalized
// terms in the index match the query terms.
type StandardQueryAnalyzer struct {
	tokenizer document.Tokenizer
	analyzer  document.Analyzer
}

// NewStandardQueryAnalyzer creates a query analyzer matching the index-time pipeline.
// language is passed to the StandardAnalyzer for stemming/stop-words (e.g. "english", "spanish").
func NewStandardQueryAnalyzer(language string) *StandardQueryAnalyzer {
	return &StandardQueryAnalyzer{
		tokenizer: document.StandardTokenizer{},
		analyzer:  document.NewStandardAnalyzer(language),
	}
}

// AnalyzeQuery tokenizes and analyzes query text, returning stemmed/normalized terms.
func (a *StandardQueryAnalyzer) AnalyzeQuery(text string) []string {
	tokens := a.tokenizer.Tokenize(text)
	if len(tokens) == 0 {
		return nil
	}

	tokens = a.analyzer.Analyze(tokens)

	terms := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if t.Term != "" {
			terms = append(terms, t.Term)
		}
	}
	return terms
}
