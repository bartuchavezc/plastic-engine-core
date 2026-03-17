package indexstore

import (
	"math"
	"testing"
)

// TestCooccurrenceCrossSentenceNoPair verifies that terms in different sentences
// do NOT produce a co-occurrence pair, even if within the positional window.
func TestCooccurrenceCrossSentenceNoPair(t *testing.T) {
	acc := newCooccurrenceAccumulator(CooccurrenceConfig{
		WindowSize:    10,
		MinPairCount:  1,
		MaxBufferSize: 10000,
	})
	defer acc.Close()

	terms := []TermPosting{
		{Term: "alpha", TF: 1, Positions: []int{1}, SentenceIDs: []int{0}},
		{Term: "beta", TF: 1, Positions: []int{2}, SentenceIDs: []int{1}},
	}

	acc.ExtractFromDocument("body", terms)
	pairs := acc.Drain()

	if len(pairs) != 0 {
		t.Errorf("expected 0 pairs across sentences, got %d: %+v", len(pairs), pairs)
	}
}

// TestCooccurrenceSameSentencePair verifies that terms in the same sentence
// DO produce a co-occurrence pair.
func TestCooccurrenceSameSentencePair(t *testing.T) {
	acc := newCooccurrenceAccumulator(CooccurrenceConfig{
		WindowSize:    10,
		MinPairCount:  1,
		MaxBufferSize: 10000,
	})
	defer acc.Close()

	terms := []TermPosting{
		{Term: "alpha", TF: 1, Positions: []int{1}, SentenceIDs: []int{0}},
		{Term: "beta", TF: 1, Positions: []int{2}, SentenceIDs: []int{0}},
	}

	acc.ExtractFromDocument("body", terms)
	pairs := acc.Drain()

	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair in same sentence, got %d: %+v", len(pairs), pairs)
	}
	if pairs[0].Count != 1 {
		t.Errorf("expected count=1, got %d", pairs[0].Count)
	}
}

// TestCooccurrencePositionalDecay verifies the weighted sum uses logarithmic decay.
func TestCooccurrencePositionalDecay(t *testing.T) {
	tests := []struct {
		name    string
		dist    int
		wantW   float64
		wantTol float64
	}{
		{"adjacent dist=1", 1, 1.0, 0.001},           // 1/log2(2) = 1.0
		{"dist=3", 3, 1.0 / math.Log2(4), 0.001},     // ~0.5
		{"dist=5", 5, 1.0 / math.Log2(6), 0.001},     // ~0.387
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := newCooccurrenceAccumulator(CooccurrenceConfig{
				WindowSize:    10,
				MinPairCount:  1,
				MaxBufferSize: 10000,
			})
			defer acc.Close()

			terms := []TermPosting{
				{Term: "foo", TF: 1, Positions: []int{1}, SentenceIDs: []int{0}},
				{Term: "bar", TF: 1, Positions: []int{1 + tt.dist}, SentenceIDs: []int{0}},
			}

			acc.ExtractFromDocument("body", terms)
			pairs := acc.Drain()

			if len(pairs) != 1 {
				t.Fatalf("expected 1 pair, got %d", len(pairs))
			}

			if math.Abs(pairs[0].WeightedSum-tt.wantW) > tt.wantTol {
				t.Errorf("WeightedSum=%.4f, want %.4f (±%.4f)", pairs[0].WeightedSum, tt.wantW, tt.wantTol)
			}
		})
	}
}

// TestCooccurrenceWindowCapWithinSentence verifies that pairs beyond WindowSize
// within the same sentence are NOT emitted.
func TestCooccurrenceWindowCapWithinSentence(t *testing.T) {
	acc := newCooccurrenceAccumulator(CooccurrenceConfig{
		WindowSize:    3,
		MinPairCount:  1,
		MaxBufferSize: 10000,
	})
	defer acc.Close()

	terms := []TermPosting{
		{Term: "near", TF: 1, Positions: []int{1}, SentenceIDs: []int{0}},
		{Term: "far", TF: 1, Positions: []int{5}, SentenceIDs: []int{0}},
	}

	acc.ExtractFromDocument("body", terms)
	pairs := acc.Drain()

	if len(pairs) != 0 {
		t.Errorf("expected 0 pairs (beyond window), got %d: %+v", len(pairs), pairs)
	}
}

// TestCooccurrenceFallbackNoSentenceIDs verifies backward compatibility when
// SentenceIDs is empty — all tokens default to sentence 0.
func TestCooccurrenceFallbackNoSentenceIDs(t *testing.T) {
	acc := newCooccurrenceAccumulator(CooccurrenceConfig{
		WindowSize:    5,
		MinPairCount:  1,
		MaxBufferSize: 10000,
	})
	defer acc.Close()

	// No SentenceIDs — should default to sentenceID=0 for all
	terms := []TermPosting{
		{Term: "hello", TF: 1, Positions: []int{1}},
		{Term: "world", TF: 1, Positions: []int{2}},
	}

	acc.ExtractFromDocument("body", terms)
	pairs := acc.Drain()

	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair with fallback sentenceID=0, got %d", len(pairs))
	}
}
