package query

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"plastic-engine-core/internal/core/search/segment"
)

// SegmentManager defines the interface for segment-based search.
type SegmentManager interface {
	Search(ctx context.Context, field, term string) ([]segment.Hit, error)
	SearchByTermID(termID string) ([]segment.Hit, error)
	GetDF(termID string) int64
	GetTotalDocs() int64
	Registry() segment.TermRegistry
}

// SegmentExecutor executes queries against a segment-based shard.
type SegmentExecutor struct {
	shardID    string
	segmentMgr SegmentManager
	scorer     *BM25Scorer
}

// NewSegmentExecutor creates a query executor for a segment-based shard.
func NewSegmentExecutor(shardID string, segmentMgr SegmentManager) *SegmentExecutor {
	return &SegmentExecutor{
		shardID:    shardID,
		segmentMgr: segmentMgr,
		scorer:     NewBM25Scorer(),
	}
}

// Execute runs the query and returns matching hits.
func (e *SegmentExecutor) Execute(ctx context.Context, req Request) ([]Hit, int64, error) {
	// Load scoring context
	scoringCtx := e.loadScoringContext()

	// Execute main query
	hits, err := e.executeClause(ctx, req.Query, scoringCtx)
	if err != nil {
		return nil, 0, fmt.Errorf("execute query: %w", err)
	}

	// Apply filters
	for _, filter := range req.Filters {
		hits, err = e.applyFilter(ctx, hits, filter, scoringCtx)
		if err != nil {
			return nil, 0, fmt.Errorf("apply filter: %w", err)
		}
	}

	total := int64(len(hits))

	// Sort by score descending
	sort.Slice(hits, func(i, j int) bool {
		return hits[i].Score > hits[j].Score
	})

	// Apply limit
	if req.Limit > 0 && len(hits) > req.Limit {
		hits = hits[:req.Limit]
	}

	return hits, total, nil
}

func (e *SegmentExecutor) executeClause(ctx context.Context, clause Clause, scoringCtx ScoringContext) ([]Hit, error) {
	switch {
	case clause.Term != nil:
		return e.executeTerm(ctx, *clause.Term, scoringCtx)
	case clause.Match != nil:
		return e.executeMatch(ctx, *clause.Match, scoringCtx)
	case clause.Prefix != nil:
		return e.executePrefix(ctx, *clause.Prefix, scoringCtx)
	case clause.Range != nil:
		return e.executeRange(ctx, *clause.Range)
	default:
		return nil, fmt.Errorf("unsupported clause type")
	}
}

func (e *SegmentExecutor) executeTerm(ctx context.Context, q TermQuery, scoringCtx ScoringContext) ([]Hit, error) {
	termValue := fmt.Sprintf("%v", q.Value)
	termValue = strings.ToLower(termValue) // Normalize

	// Search using segment manager
	segmentHits, err := e.segmentMgr.Search(ctx, q.Field, termValue)
	if err != nil {
		return nil, fmt.Errorf("search term: %w", err)
	}

	if len(segmentHits) == 0 {
		return nil, nil
	}

	// Get term ID for scoring
	registry := e.segmentMgr.Registry()
	termID, found := registry.Get(ctx, q.Field, termValue)
	if !found {
		return nil, nil
	}

	// Update scoring context with DF
	df := e.segmentMgr.GetDF(termID)
	scoringCtx.TermDF[termID] = df

	// Convert segment hits to query hits with scoring
	return e.convertAndScoreHits(segmentHits, termID, 1.0, scoringCtx)
}

func (e *SegmentExecutor) executeMatch(ctx context.Context, q MatchQuery, scoringCtx ScoringContext) ([]Hit, error) {
	// Tokenize the query value
	tokens := tokenizeQuery(q.Value)
	if len(tokens) == 0 {
		return nil, nil
	}

	registry := e.segmentMgr.Registry()
	docScores := make(map[string]float64)

	for _, token := range tokens {
		segmentHits, err := e.segmentMgr.Search(ctx, q.Field, token)
		if err != nil {
			return nil, fmt.Errorf("search term %s: %w", token, err)
		}

		if len(segmentHits) == 0 {
			continue
		}

		// Get term ID for scoring
		termID, found := registry.Get(ctx, q.Field, token)
		if !found {
			continue
		}

		// Update scoring context with DF
		df := e.segmentMgr.GetDF(termID)
		scoringCtx.TermDF[termID] = df

		// Score the hits
		hits, err := e.convertAndScoreHits(segmentHits, termID, q.Boost, scoringCtx)
		if err != nil {
			return nil, err
		}

		for _, hit := range hits {
			docScores[hit.DocID] += hit.Score
		}
	}

	// Convert to hits
	hits := make([]Hit, 0, len(docScores))
	for docID, score := range docScores {
		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   score,
		})
	}

	return hits, nil
}

func (e *SegmentExecutor) executePrefix(ctx context.Context, q PrefixQuery, scoringCtx ScoringContext) ([]Hit, error) {
	registry := e.segmentMgr.Registry()

	// Get all term IDs that match the prefix
	termIDs := registry.GetTermsWithPrefix(ctx, q.Field, q.Value)
	if len(termIDs) == 0 {
		return nil, nil
	}

	docScores := make(map[string]float64)

	for _, termID := range termIDs {
		// Search by term ID
		segmentHits, err := e.segmentMgr.SearchByTermID(termID)
		if err != nil {
			return nil, fmt.Errorf("search term id %s: %w", termID, err)
		}

		if len(segmentHits) == 0 {
			continue
		}

		// Update scoring context with DF
		df := e.segmentMgr.GetDF(termID)
		scoringCtx.TermDF[termID] = df

		// Score the hits
		hits, err := e.convertAndScoreHits(segmentHits, termID, q.Boost, scoringCtx)
		if err != nil {
			return nil, err
		}

		for _, hit := range hits {
			docScores[hit.DocID] += hit.Score
		}
	}

	// Convert to hits
	hits := make([]Hit, 0, len(docScores))
	for docID, score := range docScores {
		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   score,
		})
	}

	return hits, nil
}

func (e *SegmentExecutor) executeRange(_ context.Context, q RangeQuery) ([]Hit, error) {
	_ = q
	return nil, fmt.Errorf("range queries not yet implemented")
}

func (e *SegmentExecutor) convertAndScoreHits(segmentHits []segment.Hit, termID string, boost float64, scoringCtx ScoringContext) ([]Hit, error) {
	hits := make([]Hit, 0, len(segmentHits))

	for _, sh := range segmentHits {
		// Use TF from segment hit, estimate doc length
		docLen := 100 // Default; could be improved by storing doc lengths

		match := TermMatch{
			TermID: termID,
			TF:     sh.TF,
			DocLen: docLen,
			Boost:  boost,
		}
		score := e.scorer.Score(scoringCtx, match)

		hits = append(hits, Hit{
			DocID:   sh.DocID,
			ShardID: e.shardID,
			Score:   score,
		})
	}

	return hits, nil
}

func (e *SegmentExecutor) applyFilter(ctx context.Context, hits []Hit, filter Clause, scoringCtx ScoringContext) ([]Hit, error) {
	if filter.Term == nil {
		return hits, nil // Other filter types not yet implemented
	}

	termValue := fmt.Sprintf("%v", filter.Term.Value)
	termValue = strings.ToLower(termValue)

	// Get docs matching the filter term
	filterHits, err := e.segmentMgr.Search(ctx, filter.Term.Field, termValue)
	if err != nil {
		return nil, err
	}

	// Build set of matching doc IDs
	filterDocs := make(map[string]struct{})
	for _, h := range filterHits {
		filterDocs[h.DocID] = struct{}{}
	}

	// Filter hits
	var filtered []Hit
	for _, hit := range hits {
		if _, ok := filterDocs[hit.DocID]; ok {
			filtered = append(filtered, hit)
		}
	}

	return filtered, nil
}

func (e *SegmentExecutor) loadScoringContext() ScoringContext {
	ctx := NewScoringContext()
	ctx.TotalDocs = e.segmentMgr.GetTotalDocs()
	ctx.AvgDocLen = 100 // Default average
	return ctx
}
