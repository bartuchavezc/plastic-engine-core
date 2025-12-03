package indexer_test

import (
	"testing"

	"plastic-engine-core/internal/core/search/indexer"
)

func TestSimpleAnalyzerLowercasesTokens(t *testing.T) {
	t.Parallel()

	analyzer := indexer.SimpleAnalyzer{}
	tokens := []indexer.Token{
		{Term: "HELLO"},
		{Term: "WORLD"},
	}

	result := analyzer.Analyze(tokens)

	if result[0].Term != "hello" || result[1].Term != "world" {
		t.Fatalf("expected lowercase tokens, got %+v", result)
	}
}
