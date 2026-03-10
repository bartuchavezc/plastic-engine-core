package segment_test

import (
	"context"
	"testing"

	"plastic-engine-core/internal/core/search/segment"
)

func TestDamerauLevenshteinDistance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "", 3},
		{"", "abc", 3},
		{"abc", "abc", 0},
		{"abc", "abd", 1},            // substitution
		{"abc", "abcd", 1},           // insertion
		{"abcd", "abc", 1},           // deletion
		{"abc", "bac", 1},            // transposition (adjacent swap)
		{"heaslight", "headlight", 1}, // real typo: s→d substitution
		{"headlihgt", "headlight", 1}, // transposition: g↔h
		{"moisturizer", "moisturizer", 0},
		{"cat", "cats", 1},
		{"kitten", "sitting", 3},
	}

	for _, tt := range tests {
		t.Run(tt.a+"→"+tt.b, func(t *testing.T) {
			got := segment.DamerauLevenshteinDistance(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("DamerauLevenshteinDistance(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestFuzzyPrefixes(t *testing.T) {
	t.Parallel()

	prefixes := segment.FuzzyPrefixes("heaslight", 1)
	if len(prefixes) == 0 {
		t.Fatal("expected at least one prefix")
	}

	// Should include "hea" (first 3 chars) which would match "headlight"
	found := false
	for _, p := range prefixes {
		if p == "hea" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected prefix 'hea' in %v", prefixes)
	}
}

func TestManagerListTermsByFuzzy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := segment.DefaultConfig()
	cfg.DataDir = dir
	cfg.CooccurrenceConfig.Disabled = true

	mgr, err := segment.NewManager(cfg)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Index documents with various terms
	docs := []segment.DocumentBatch{
		{DocID: "doc-1", FieldTerms: map[string][]segment.TermPosting{
			"title": {{Term: "headlight", TF: 1, Positions: []int{0}}},
		}},
		{DocID: "doc-2", FieldTerms: map[string][]segment.TermPosting{
			"title": {{Term: "headlight", TF: 1, Positions: []int{0}}},
		}},
		{DocID: "doc-3", FieldTerms: map[string][]segment.TermPosting{
			"title": {{Term: "headline", TF: 1, Positions: []int{0}}},
		}},
		{DocID: "doc-4", FieldTerms: map[string][]segment.TermPosting{
			"title": {{Term: "heavyduty", TF: 1, Positions: []int{0}}},
		}},
	}
	if err := mgr.IndexDocumentBatch(ctx, docs); err != nil {
		t.Fatalf("IndexDocumentBatch: %v", err)
	}

	// Fuzzy search for "heaslight" (typo for "headlight", DL distance 1)
	entries, err := mgr.ListTermsByFuzzy(ctx, "title", "heaslight", 1, 5)
	if err != nil {
		t.Fatalf("ListTermsByFuzzy: %v", err)
	}

	if len(entries) == 0 {
		t.Fatal("expected at least one fuzzy match for 'heaslight'")
	}

	// Best match should be "headlight" (highest DF = 2 docs)
	if entries[0].Term != "headlight" {
		t.Errorf("expected best match 'headlight', got %q", entries[0].Term)
	}

	// "headline" has DL distance 3 from "heaslight" — should NOT match at distance 1
	for _, e := range entries {
		if e.Term == "headline" {
			t.Error("'headline' should not match 'heaslight' at distance 1")
		}
	}

	// Fuzzy search for "headlihgt" (transposition typo for "headlight", DL distance 1)
	entries2, err := mgr.ListTermsByFuzzy(ctx, "title", "headlihgt", 1, 5)
	if err != nil {
		t.Fatalf("ListTermsByFuzzy transposition: %v", err)
	}
	if len(entries2) == 0 {
		t.Fatal("expected fuzzy match for transposition typo 'headlihgt'")
	}
	if entries2[0].Term != "headlight" {
		t.Errorf("expected 'headlight' for transposition typo, got %q", entries2[0].Term)
	}
}
