package document_test

import (
	"testing"

	"plastic-engine-core/internal/core/search/document"
)

func TestStandardTokenizerSentenceBoundary(t *testing.T) {
	t.Parallel()

	tok := document.StandardTokenizer{}

	tests := []struct {
		name   string
		input  string
		// expected: map[sentenceID] → list of terms in that sentence
		expect map[int][]string
	}{
		{
			name:  "two sentences",
			input: "Hello world. How are you?",
			expect: map[int][]string{
				0: {"hello", "world"},
				1: {"how", "are", "you"},
			},
		},
		{
			name:  "decimal does not split",
			input: "Version 3.14 is out.",
			// UAX#29 treats "3.14" as a single numeric token (type 1).
			// The final "." is a separate type-0 segment that IS a sentence boundary,
			// but since there's no word after it, sentenceID stays 0 for all tokens.
			expect: map[int][]string{
				0: {"version", "3.14", "is", "out"},
			},
		},
		{
			name:  "exclamation and question marks",
			input: "Stop! What is that? Amazing.",
			expect: map[int][]string{
				0: {"stop"},
				1: {"what", "is", "that"},
				2: {"amazing"},
			},
		},
		{
			name:  "semicolon as boundary",
			input: "first clause; second clause",
			expect: map[int][]string{
				0: {"first", "clause"},
				1: {"second", "clause"},
			},
		},
		{
			name:  "single sentence no punct",
			input: "just some words here",
			expect: map[int][]string{
				0: {"just", "some", "words", "here"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := tok.Tokenize(tt.input)

			// Group tokens by sentenceID
			got := make(map[int][]string)
			for _, tk := range tokens {
				got[tk.SentenceID] = append(got[tk.SentenceID], tk.Term)
			}

			if len(got) != len(tt.expect) {
				t.Fatalf("got %d sentences, want %d\ngot: %v", len(got), len(tt.expect), got)
			}

			for sid, wantTerms := range tt.expect {
				gotTerms := got[sid]
				if len(gotTerms) != len(wantTerms) {
					t.Errorf("sentence %d: got %v, want %v", sid, gotTerms, wantTerms)
					continue
				}
				for i, w := range wantTerms {
					if gotTerms[i] != w {
						t.Errorf("sentence %d token %d: got %q, want %q", sid, i, gotTerms[i], w)
					}
				}
			}
		})
	}
}

func TestStandardTokenizerSentenceIDPreservedByAnalyzer(t *testing.T) {
	t.Parallel()

	tok := document.StandardTokenizer{}
	analyzer := document.NewStandardAnalyzer("english")

	// Use words that won't be removed as stop words in both sentences
	tokens := tok.Tokenize("Quick brown fox jumps. Lazy dog runs fast.")
	analyzed := analyzer.Analyze(tokens)

	// Verify at least one token in sentence 0 and one in sentence 1
	hasSent0, hasSent1 := false, false
	for _, tk := range analyzed {
		if tk.SentenceID == 0 {
			hasSent0 = true
		}
		if tk.SentenceID == 1 {
			hasSent1 = true
		}
		if tk.SentenceID < 0 || tk.SentenceID > 1 {
			t.Errorf("token %q has unexpected sentenceID=%d", tk.Term, tk.SentenceID)
		}
	}
	if !hasSent0 || !hasSent1 {
		t.Errorf("expected tokens in both sentences, got sent0=%v sent1=%v, tokens=%+v",
			hasSent0, hasSent1, analyzed)
	}
}
