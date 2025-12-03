package document_test

import (
	"testing"

	"plastic-engine-core/internal/core/search/document"
)

func TestSimpleAnalyzerLowercasesTokens(t *testing.T) {
	t.Parallel()

	analyzer := document.SimpleAnalyzer{}
	tokens := []document.Token{
		{Term: "HELLO"},
		{Term: "WORLD"},
	}

	result := analyzer.Analyze(tokens)

	if result[0].Term != "hello" || result[1].Term != "world" {
		t.Fatalf("expected lowercase tokens, got %+v", result)
	}
}
