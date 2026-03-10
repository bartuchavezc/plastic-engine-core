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

func TestStandardAnalyzerStemming(t *testing.T) {
	t.Parallel()

	analyzer := document.NewStandardAnalyzer("english")

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"Moisturizing  stems to moistur", "Moisturizing ", "moistur"},
		{"organization stems to organ", "organization", "organ"},
		{"shelves stems to shelv", "shelves", "shelv"},
		// Short stems are now accepted — the snowball stemmer is trusted
		{"cats stems to cat", "cats", "cat"},
		{"dogs stems to dog", "dogs", "dog"},
		{"running stems to run", "running", "run"},
		{"books stems to book", "books", "book"},
		{"faces stems to face", "faces", "face"},
		// Terms that stem to themselves
		{"run stays run", "run", "run"},
		{"box stays box", "box", "box"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := []document.Token{{Term: tt.input}}
			result := analyzer.Analyze(tokens)
			if len(result) == 0 {
				t.Fatalf("expected 1 token, got 0")
			}
			if result[0].Term != tt.want {
				t.Errorf("Analyze(%q) = %q, want %q", tt.input, result[0].Term, tt.want)
			}
		})
	}
}

func TestStandardAnalyzerSplitNonAlphaNum(t *testing.T) {
	t.Parallel()

	analyzer := document.NewStandardAnalyzer("english")

	tests := []struct {
		name    string
		input   string
		wantLen int
		want    []string // expected terms after analysis
	}{
		// Split on non-alphanumeric boundaries
		{"face&body splits and stems", "face&body", 2, []string{"face", "bodi"}},
		{"hello-world splits", "hello-world", 2, []string{"hello", "world"}},
		{"urns,cats,dogs splits and stems", "urns,cats,dogs", 3, []string{"urn", "cat", "dog"}},
		// Single-char parts are filtered
		{"a.b both parts too short", "a.b", 0, nil},
		// Trailing punct stripped by TrimFunc, no split needed
		{"mois( trimmed to mois", "mois(", 1, []string{"moi"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := []document.Token{{Term: tt.input}}
			result := analyzer.Analyze(tokens)
			if len(result) != tt.wantLen {
				terms := make([]string, len(result))
				for i, r := range result {
					terms[i] = r.Term
				}
				t.Fatalf("expected %d tokens, got %d: %v", tt.wantLen, len(result), terms)
			}
			for i, want := range tt.want {
				if result[i].Term != want {
					t.Errorf("token[%d] = %q, want %q", i, result[i].Term, want)
				}
			}
		})
	}
}

func TestStandardAnalyzerQueryConsistency(t *testing.T) {
	t.Parallel()

	analyzer := document.NewStandardAnalyzer("english")

	// Index-time and query-time should produce the same tokens
	inputs := []string{"moisturizer", "organization", "shelves", "books", "running"}

	for _, input := range inputs {
		indexTokens := analyzer.Analyze([]document.Token{{Term: input}})
		queryTokens := analyzer.Analyze([]document.Token{{Term: input}})

		if len(indexTokens) != len(queryTokens) {
			t.Fatalf("token count mismatch for %q: index=%d query=%d", input, len(indexTokens), len(queryTokens))
		}
		if len(indexTokens) > 0 && indexTokens[0].Term != queryTokens[0].Term {
			t.Errorf("token mismatch for %q: index=%q query=%q", input, indexTokens[0].Term, queryTokens[0].Term)
		}
	}
}

func TestStandardAnalyzerStopWords(t *testing.T) {
	t.Parallel()

	analyzer := document.NewStandardAnalyzer("english")

	stopWords := []string{"the", "is", "at", "for", "and", "or", "but", "in", "with"}
	for _, sw := range stopWords {
		tokens := []document.Token{{Term: sw}}
		result := analyzer.Analyze(tokens)
		if len(result) != 0 {
			t.Errorf("stop word %q should be filtered, got %+v", sw, result)
		}
	}
}
