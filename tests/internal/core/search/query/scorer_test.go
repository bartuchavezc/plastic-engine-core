package query_test

import (
	"math"
	"testing"

	"plastic-engine-core/internal/core/search/query"
)

func TestBM25ScorerIDF(t *testing.T) {
	t.Parallel()

	scorer := query.NewBM25Scorer()

	tests := []struct {
		name      string
		totalDocs int64
		df        int64
		wantMin   float64 // IDF should be at least this
		wantMax   float64 // IDF should be at most this
	}{
		{
			name:      "rare term",
			totalDocs: 10000,
			df:        10,
			wantMin:   5.0,
			wantMax:   8.0,
		},
		{
			name:      "common term",
			totalDocs: 10000,
			df:        5000,
			wantMin:   0.0,
			wantMax:   1.0,
		},
		{
			name:      "very rare term",
			totalDocs: 10000,
			df:        1,
			wantMin:   8.0,
			wantMax:   12.0,
		},
		{
			name:      "zero df",
			totalDocs: 10000,
			df:        0,
			wantMin:   0.0,
			wantMax:   0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idf := scorer.IDF(tt.totalDocs, tt.df)
			if idf < tt.wantMin || idf > tt.wantMax {
				t.Errorf("IDF(%d, %d) = %f, want in range [%f, %f]",
					tt.totalDocs, tt.df, idf, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestBM25ScorerScore(t *testing.T) {
	t.Parallel()

	scorer := query.NewBM25Scorer()

	ctx := query.NewScoringContext().
		WithTotalDocs(10000).
		WithAvgDocLen(100).
		WithTermDF("t_rare", 10).
		WithTermDF("t_common", 5000)

	// Rare term should score higher than common term
	rareMatch := query.TermMatch{
		TermID: "t_rare",
		TF:     3,
		DocLen: 100,
		Boost:  1.0,
	}

	commonMatch := query.TermMatch{
		TermID: "t_common",
		TF:     3,
		DocLen: 100,
		Boost:  1.0,
	}

	rareScore := scorer.Score(ctx, rareMatch)
	commonScore := scorer.Score(ctx, commonMatch)

	if rareScore <= commonScore {
		t.Errorf("rare term score (%f) should be > common term score (%f)",
			rareScore, commonScore)
	}
}

func TestBM25ScorerTFSaturation(t *testing.T) {
	t.Parallel()

	scorer := query.NewBM25Scorer()

	ctx := query.NewScoringContext().
		WithTotalDocs(10000).
		WithAvgDocLen(100).
		WithTermDF("t_test", 100)

	// Higher TF should score higher, but with saturation
	match1 := query.TermMatch{TermID: "t_test", TF: 1, DocLen: 100, Boost: 1.0}
	match2 := query.TermMatch{TermID: "t_test", TF: 5, DocLen: 100, Boost: 1.0}
	match3 := query.TermMatch{TermID: "t_test", TF: 100, DocLen: 100, Boost: 1.0}

	score1 := scorer.Score(ctx, match1)
	score2 := scorer.Score(ctx, match2)
	score3 := scorer.Score(ctx, match3)

	if score1 >= score2 || score2 >= score3 {
		t.Errorf("scores should increase with TF: %f < %f < %f", score1, score2, score3)
	}

	// But the increase should saturate (diminishing returns)
	diff12 := score2 - score1
	diff23 := score3 - score2

	if diff23 >= diff12 {
		t.Errorf("TF saturation expected: diff(1->5)=%f should be > diff(5->100)=%f",
			diff12, diff23)
	}
}

func TestBM25ScorerLengthNormalization(t *testing.T) {
	t.Parallel()

	scorer := query.NewBM25Scorer()

	ctx := query.NewScoringContext().
		WithTotalDocs(10000).
		WithAvgDocLen(100).
		WithTermDF("t_test", 100)

	// Same TF, but shorter doc should score higher
	shortDoc := query.TermMatch{TermID: "t_test", TF: 3, DocLen: 50, Boost: 1.0}
	avgDoc := query.TermMatch{TermID: "t_test", TF: 3, DocLen: 100, Boost: 1.0}
	longDoc := query.TermMatch{TermID: "t_test", TF: 3, DocLen: 200, Boost: 1.0}

	shortScore := scorer.Score(ctx, shortDoc)
	avgScore := scorer.Score(ctx, avgDoc)
	longScore := scorer.Score(ctx, longDoc)

	if shortScore <= avgScore || avgScore <= longScore {
		t.Errorf("shorter docs should score higher: short=%f > avg=%f > long=%f",
			shortScore, avgScore, longScore)
	}
}

func TestBM25ScorerBoost(t *testing.T) {
	t.Parallel()

	scorer := query.NewBM25Scorer()

	ctx := query.NewScoringContext().
		WithTotalDocs(10000).
		WithAvgDocLen(100).
		WithTermDF("t_test", 100)

	noBoost := query.TermMatch{TermID: "t_test", TF: 3, DocLen: 100, Boost: 1.0}
	boosted := query.TermMatch{TermID: "t_test", TF: 3, DocLen: 100, Boost: 2.0}

	noBoostScore := scorer.Score(ctx, noBoost)
	boostedScore := scorer.Score(ctx, boosted)

	expectedBoosted := noBoostScore * 2.0
	if math.Abs(boostedScore-expectedBoosted) > 0.001 {
		t.Errorf("boosted score = %f, want %f (2x of %f)",
			boostedScore, expectedBoosted, noBoostScore)
	}
}

func TestBM25ScorerMultiTerm(t *testing.T) {
	t.Parallel()

	scorer := query.NewBM25Scorer()

	ctx := query.NewScoringContext().
		WithTotalDocs(10000).
		WithAvgDocLen(100).
		WithTermDF("t_hello", 100).
		WithTermDF("t_world", 200)

	matches := []query.TermMatch{
		{TermID: "t_hello", TF: 2, DocLen: 100, Boost: 1.0},
		{TermID: "t_world", TF: 1, DocLen: 100, Boost: 1.0},
	}

	multiScore := scorer.ScoreMultiTerm(ctx, matches)

	// Should equal sum of individual scores
	score1 := scorer.Score(ctx, matches[0])
	score2 := scorer.Score(ctx, matches[1])

	expectedSum := score1 + score2
	if math.Abs(multiScore-expectedSum) > 0.001 {
		t.Errorf("multi-term score = %f, want sum %f", multiScore, expectedSum)
	}
}

func TestBM25ScorerMissingTerm(t *testing.T) {
	t.Parallel()

	scorer := query.NewBM25Scorer()

	ctx := query.NewScoringContext().
		WithTotalDocs(10000).
		WithAvgDocLen(100)
	// Note: no TermDF added

	match := query.TermMatch{
		TermID: "t_missing",
		TF:     3,
		DocLen: 100,
		Boost:  1.0,
	}

	score := scorer.Score(ctx, match)
	if score != 0 {
		t.Errorf("missing term should score 0, got %f", score)
	}
}

func TestDefaultBM25Config(t *testing.T) {
	t.Parallel()

	cfg := query.DefaultBM25Config()

	// Standard BM25 defaults
	if cfg.K1 != 1.2 {
		t.Errorf("default K1 = %f, want 1.2", cfg.K1)
	}
	if cfg.B != 0.75 {
		t.Errorf("default B = %f, want 0.75", cfg.B)
	}
}

func TestBM25ScorerWithConfig(t *testing.T) {
	t.Parallel()

	cfg := query.BM25Config{K1: 2.0, B: 0.5}
	scorer := query.NewBM25ScorerWithConfig(cfg)

	ctx := query.NewScoringContext().
		WithTotalDocs(10000).
		WithAvgDocLen(100).
		WithTermDF("t_test", 100)

	match := query.TermMatch{TermID: "t_test", TF: 3, DocLen: 100, Boost: 1.0}
	score := scorer.Score(ctx, match)

	// Just verify it produces a non-zero score with custom config
	if score <= 0 {
		t.Errorf("expected positive score with custom config, got %f", score)
	}
}

