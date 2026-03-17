package query

import (
	"math"
)

// BM25Config holds the tuning parameters for BM25 scoring.
type BM25Config struct {
	K1 float64 // Term frequency saturation (default: 1.2)
	B  float64 // Length normalization (default: 0.75)
}

// DefaultBM25Config returns the standard BM25 parameters.
func DefaultBM25Config() BM25Config {
	return BM25Config{
		K1: 1.2,
		B:  0.75,
	}
}

// BM25Scorer calculates BM25 relevance scores.
type BM25Scorer struct {
	K1 float64
	B  float64
}

// NewBM25Scorer creates a scorer with default parameters.
func NewBM25Scorer() *BM25Scorer {
	cfg := DefaultBM25Config()
	return &BM25Scorer{
		K1: cfg.K1,
		B:  cfg.B,
	}
}

// NewBM25ScorerWithConfig creates a scorer with custom parameters.
func NewBM25ScorerWithConfig(cfg BM25Config) *BM25Scorer {
	return &BM25Scorer{
		K1: cfg.K1,
		B:  cfg.B,
	}
}

// ScoringContext holds the corpus statistics needed for BM25 scoring.
type ScoringContext struct {
	TotalDocs int64              // Total number of documents (N)
	AvgDocLen float64            // Average document length
	TermDF    map[string]int64   // term_id -> document frequency
}

// TermMatch represents a term hit in a document.
type TermMatch struct {
	TermID    string
	TF        int     // Term frequency in document
	DocLen    int     // Document length (number of terms)
	Boost     float64 // Query-time boost multiplier
}

// IDF calculates the Inverse Document Frequency for a term.
// Uses the BM25 IDF formula: log(1 + (N - df + 0.5) / (df + 0.5))
func (s *BM25Scorer) IDF(totalDocs, df int64) float64 {
	if df <= 0 {
		return 0
	}
	numerator := float64(totalDocs) - float64(df) + 0.5
	denominator := float64(df) + 0.5
	return math.Log(1 + numerator/denominator)
}

// Score calculates the BM25 score for a single term match.
func (s *BM25Scorer) Score(ctx ScoringContext, match TermMatch) float64 {
	df, ok := ctx.TermDF[match.TermID]
	if !ok || df <= 0 {
		return 0
	}

	idf := s.IDF(ctx.TotalDocs, df)

	// TF normalization with length adjustment
	tfNorm := s.tfNorm(match.TF, match.DocLen, ctx.AvgDocLen)

	score := idf * tfNorm

	// Apply boost if set
	if match.Boost > 0 {
		score *= match.Boost
	}

	return score
}

// ScoreMultiTerm calculates the combined BM25 score for multiple term matches.
// The scores are summed (standard BM25 behavior for multi-term queries).
func (s *BM25Scorer) ScoreMultiTerm(ctx ScoringContext, matches []TermMatch) float64 {
	var total float64
	for _, match := range matches {
		total += s.Score(ctx, match)
	}
	return total
}

// tfNorm calculates the normalized term frequency component.
// Formula: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (docLen / avgdl)))
func (s *BM25Scorer) tfNorm(tf, docLen int, avgDocLen float64) float64 {
	if avgDocLen <= 0 {
		avgDocLen = 1 // Prevent division by zero
	}

	tfFloat := float64(tf)
	docLenNorm := float64(docLen) / avgDocLen

	numerator := tfFloat * (s.K1 + 1)
	denominator := tfFloat + s.K1*(1-s.B+s.B*docLenNorm)

	if denominator <= 0 {
		return 0
	}

	return numerator / denominator
}

// --------------------------------------------------------------------------
// Coordinate Matching + Position Ordering
// --------------------------------------------------------------------------

// DefaultCoordWeight controls the exponent for coordinate matching.
// Higher values penalize partial matches more aggressively.
const DefaultCoordWeight = 2.0

// CoordFactor returns a penalty/boost based on how many query terms matched.
// For single-term queries it returns 1.0 (no effect).
// For multi-term queries: ratio^DefaultCoordWeight, so a doc matching 2/4 terms
// gets 0.25 while 4/4 gets 1.0.
func CoordFactor(matchedTerms, totalTerms int) float64 {
	if totalTerms <= 1 {
		return 1.0
	}
	ratio := float64(matchedTerms) / float64(totalTerms)
	return math.Pow(ratio, DefaultCoordWeight)
}

// OrderingBonusFactor controls the maximum bonus for terms appearing in query order.
const OrderingBonusFactor = 0.5

// OrderingBoost calculates a proximity-aware bonus for query terms appearing in
// document order. For each consecutive pair, 1/gap gives full credit only to
// adjacent terms (gap=1→1.0, gap=2→0.5, gap=5→0.2). Out-of-order pairs score 0.
func OrderingBoost(queryTermPositions []int) float64 {
	if len(queryTermPositions) <= 1 {
		return 1.0
	}
	var pairScore float64
	totalPairs := len(queryTermPositions) - 1
	for i := 1; i < len(queryTermPositions); i++ {
		if queryTermPositions[i] > queryTermPositions[i-1] {
			gap := queryTermPositions[i] - queryTermPositions[i-1]
			pairScore += 1.0 / float64(gap)
		}
	}
	ratio := pairScore / float64(totalPairs)
	return 1.0 + OrderingBonusFactor*ratio
}

// --------------------------------------------------------------------------
// Utility functions for building scoring context
// --------------------------------------------------------------------------

// NewScoringContext creates an empty scoring context.
func NewScoringContext() ScoringContext {
	return ScoringContext{
		TermDF: make(map[string]int64),
	}
}

// WithTotalDocs sets the total document count.
func (c ScoringContext) WithTotalDocs(n int64) ScoringContext {
	c.TotalDocs = n
	return c
}

// WithAvgDocLen sets the average document length.
func (c ScoringContext) WithAvgDocLen(avg float64) ScoringContext {
	c.AvgDocLen = avg
	return c
}

// WithTermDF adds a term's document frequency.
func (c ScoringContext) WithTermDF(termID string, df int64) ScoringContext {
	c.TermDF[termID] = df
	return c
}

