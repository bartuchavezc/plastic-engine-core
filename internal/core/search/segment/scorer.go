package segment

import (
	"math"
	"sort"
)

// BM25Scorer calculates BM25 scores for search results.
type BM25Scorer struct {
	// K1 controls term frequency saturation. Default 1.2
	K1 float64

	// B controls length normalization. Default 0.75
	B float64
}

// NewBM25Scorer creates a new scorer with default parameters.
func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{
		K1: 1.2,
		B:  0.75,
	}
}

// ScoringContext provides context for scoring calculations.
type ScoringContext struct {
	TotalDocs int64              // Total documents in the index
	AvgDocLen float64            // Average document length
	TermDF    map[string]int64   // term_id -> document frequency
	DocLens   map[string]int     // doc_id -> document length
}

// NewScoringContext creates an empty scoring context.
func NewScoringContext() ScoringContext {
	return ScoringContext{
		TermDF:  make(map[string]int64),
		DocLens: make(map[string]int),
	}
}

// ScoredHit is a hit with its BM25 score.
type ScoredHit struct {
	Hit
	Score float64
}

// Score calculates the BM25 score for a single term match.
func (s *BM25Scorer) Score(ctx ScoringContext, termID string, tf int, docLen int) float64 {
	// Get document frequency
	df := ctx.TermDF[termID]
	if df <= 0 {
		df = 1
	}

	N := float64(ctx.TotalDocs)
	if N <= 0 {
		N = 1
	}

	avgDocLen := ctx.AvgDocLen
	if avgDocLen <= 0 {
		avgDocLen = 100
	}

	// IDF: log((N - df + 0.5) / (df + 0.5) + 1)
	idf := math.Log((N-float64(df)+0.5)/(float64(df)+0.5) + 1)

	// TF component: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
	tfFloat := float64(tf)
	docLenFloat := float64(docLen)

	numerator := tfFloat * (s.K1 + 1)
	denominator := tfFloat + s.K1*(1-s.B+s.B*docLenFloat/avgDocLen)

	if denominator <= 0 {
		denominator = 1
	}

	return idf * (numerator / denominator)
}

// ScoreHits calculates BM25 scores for a set of hits.
func (s *BM25Scorer) ScoreHits(ctx ScoringContext, hits []Hit) []ScoredHit {
	scored := make([]ScoredHit, len(hits))

	for i, hit := range hits {
		docLen := ctx.DocLens[hit.DocID]
		if docLen <= 0 {
			docLen = 100 // Default
		}

		score := s.Score(ctx, hit.TermID, hit.TF, docLen)

		scored[i] = ScoredHit{
			Hit:   hit,
			Score: score,
		}
	}

	return scored
}

// ScoreMultiTermHits scores and aggregates hits from multiple terms.
func (s *BM25Scorer) ScoreMultiTermHits(ctx ScoringContext, hitsPerTerm map[string][]Hit) []ScoredHit {
	// Aggregate scores per document
	docScores := make(map[string]float64)
	docHits := make(map[string]Hit)

	for termID, hits := range hitsPerTerm {
		for _, hit := range hits {
			docLen := ctx.DocLens[hit.DocID]
			if docLen <= 0 {
				docLen = 100
			}

			score := s.Score(ctx, termID, hit.TF, docLen)
			docScores[hit.DocID] += score

			// Keep the first hit for this doc (for metadata)
			if _, ok := docHits[hit.DocID]; !ok {
				docHits[hit.DocID] = hit
			}
		}
	}

	// Convert to scored hits
	result := make([]ScoredHit, 0, len(docScores))
	for docID, score := range docScores {
		hit := docHits[docID]
		result = append(result, ScoredHit{
			Hit:   hit,
			Score: score,
		})
	}

	return result
}

// RankHits sorts hits by score descending.
func RankHits(hits []ScoredHit) {
	sort.Slice(hits, func(i, j int) bool {
		return hits[i].Score > hits[j].Score
	})
}

// TopK returns the top K hits.
func TopK(hits []ScoredHit, k int) []ScoredHit {
	if len(hits) <= k {
		return hits
	}
	return hits[:k]
}

// MergeAndRankHits merges hits from multiple segments and ranks them.
func MergeAndRankHits(scorer *BM25Scorer, ctx ScoringContext, segmentHits [][]Hit) []ScoredHit {
	// Aggregate all hits
	allHits := make([]Hit, 0)
	for _, hits := range segmentHits {
		allHits = append(allHits, hits...)
	}

	// Score all hits
	scored := scorer.ScoreHits(ctx, allHits)

	// Aggregate by document (same doc might appear in multiple segments)
	docScores := make(map[string]ScoredHit)
	for _, sh := range scored {
		existing, ok := docScores[sh.DocID]
		if !ok || sh.Score > existing.Score {
			docScores[sh.DocID] = sh
		}
	}

	// Convert to slice
	result := make([]ScoredHit, 0, len(docScores))
	for _, sh := range docScores {
		result = append(result, sh)
	}

	// Rank
	RankHits(result)

	return result
}
