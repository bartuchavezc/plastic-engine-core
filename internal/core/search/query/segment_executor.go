package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"plastic-engine-core/internal/core/search/knowledge"
	"plastic-engine-core/internal/core/search/segment"
)

// SegmentManager defines the interface for segment-based search.
type SegmentManager interface {
	Search(ctx context.Context, field, term string) ([]segment.Hit, error)
	SearchByTermID(termID string) ([]segment.Hit, error)
	GetDF(termID string) int64
	GetTotalDocs() int64
	GetTermsWithPrefix(ctx context.Context, field, prefix string) []string
	ListTermsByFuzzy(ctx context.Context, field, query string, maxDistance, limit int) ([]segment.TermEntry, error)
	TermMatrix() *knowledge.AdjacencyMatrix
}

// HybridExpansionInfo captures metadata about graph-based query expansion.
type HybridExpansionInfo struct {
	OriginalTokens []string           `json:"original_tokens"`
	ExpandedTerms  map[string]float64 `json:"expanded_terms"`
	ExpansionHops  int                `json:"expansion_hops"`
	TotalExpanded  int                `json:"total_expanded"`
}

// QueryAnalyzer normalizes query text using the same pipeline as indexing.
// Implementations should apply lowercase, stop-word removal, and stemming
// to ensure query terms match indexed terms.
type QueryAnalyzer interface {
	// AnalyzeQuery takes raw query text and returns analyzed terms.
	AnalyzeQuery(text string) []string
}

// SegmentExecutor executes queries against a segment-based shard.
type SegmentExecutor struct {
	shardID        string
	segmentMgr     SegmentManager
	queryAnalyzer  QueryAnalyzer
	scorer         *BM25Scorer
	hybridMetadata *HybridExpansionInfo
}

// NewSegmentExecutor creates a query executor for a segment-based shard.
func NewSegmentExecutor(shardID string, segmentMgr SegmentManager, analyzer QueryAnalyzer) *SegmentExecutor {
	return &SegmentExecutor{
		shardID:       shardID,
		segmentMgr:    segmentMgr,
		queryAnalyzer: analyzer,
		scorer:        NewBM25Scorer(),
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
	case clause.Hybrid != nil:
		return e.executeHybrid(ctx, *clause.Hybrid, scoringCtx)
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

	// Deterministic term key — no registry lookup needed.
	termID := q.Field + "\x00" + termValue

	// DF = number of docs containing the term (already known from search results)
	scoringCtx.TermDF[termID] = int64(len(segmentHits))

	// Convert segment hits to query hits with scoring
	return e.convertAndScoreHits(segmentHits, termID, 1.0, scoringCtx)
}

func (e *SegmentExecutor) executeMatch(ctx context.Context, q MatchQuery, scoringCtx ScoringContext) ([]Hit, error) {
	// Analyze query with the same pipeline used at index time
	var tokens []string
	if e.queryAnalyzer != nil {
		tokens = e.queryAnalyzer.AnalyzeQuery(q.Value)
	} else {
		tokens = tokenizeQuery(q.Value)
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	docScores := make(map[string]float64)

	for _, token := range tokens {
		segmentHits, err := e.segmentMgr.Search(ctx, q.Field, token)
		if err != nil {
			return nil, fmt.Errorf("search term %s: %w", token, err)
		}

		// Fuzzy fallback: if no exact hits and fuzziness enabled, try correction
		// Distance scales with token length for better recall on longer terms
		if len(segmentHits) == 0 && q.Fuzziness > 0 && len(token) >= 3 {
			dist := autoFuzzyDistance(len(token))
			if corrected, correctedHits, fErr := e.fuzzyFallback(ctx, q.Field, token, dist); fErr == nil && len(correctedHits) > 0 {
				token = corrected
				segmentHits = correctedHits
			}
		}

		if len(segmentHits) == 0 {
			continue
		}

		// Deterministic term key — no registry lookup needed.
		termID := q.Field + "\x00" + token

		// DF derived from search results
		scoringCtx.TermDF[termID] = int64(len(segmentHits))

		// Score directly into docScores — no intermediate []Hit allocation
		e.scoreHitsInto(docScores, segmentHits, termID, q.Boost, scoringCtx)
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
	// Get all term IDs that match the prefix by scanning segments directly.
	termIDs := e.segmentMgr.GetTermsWithPrefix(ctx, q.Field, q.Value)
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

		// DF derived from search results
		scoringCtx.TermDF[termID] = int64(len(segmentHits))

		// Score directly into docScores — no intermediate []Hit allocation
		e.scoreHitsInto(docScores, segmentHits, termID, q.Boost, scoringCtx)
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

func (e *SegmentExecutor) executeHybrid(ctx context.Context, q HybridQuery, scoringCtx ScoringContext) ([]Hit, error) {
	// Phase 1: BM25 on original tokens (full boost)
	var tokens []string
	if e.queryAnalyzer != nil {
		tokens = e.queryAnalyzer.AnalyzeQuery(q.Value)
	} else {
		tokens = tokenizeQuery(q.Value)
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	docScores := make(map[string]float64)

	for i, token := range tokens {
		segmentHits, err := e.segmentMgr.Search(ctx, q.Field, token)
		if err != nil {
			return nil, fmt.Errorf("hybrid search term %s: %w", token, err)
		}

		// Fuzzy fallback for zero-hit tokens
		// Distance scales with token length for better recall on longer terms
		if len(segmentHits) == 0 && q.Fuzziness > 0 && len(token) >= 3 {
			dist := autoFuzzyDistance(len(token))
			if corrected, correctedHits, fErr := e.fuzzyFallback(ctx, q.Field, token, dist); fErr == nil && len(correctedHits) > 0 {
				token = corrected
				tokens[i] = corrected // update for Phase 2 graph expansion
				segmentHits = correctedHits
			}
		}

		if len(segmentHits) == 0 {
			continue
		}

		termID := q.Field + "\x00" + token
		scoringCtx.TermDF[termID] = int64(len(segmentHits))

		// Score directly into docScores — no intermediate []Hit allocation
		e.scoreHitsInto(docScores, segmentHits, termID, q.Boost, scoringCtx)
	}

	// Phase 2: Graph expansion via SpreadActivation with energy repartition
	expandedTerms := make(map[string]float64)
	termMatrix := e.segmentMgr.TermMatrix()

	if termMatrix != nil {
		originalSet := make(map[string]struct{}, len(tokens))
		for _, t := range tokens {
			originalSet[q.Field+"\x00"+t] = struct{}{}
		}

		// Energy repartition: divide entry energy equally among tokens
		entryEnergy := 1.0 / float64(len(tokens))

		// SUM merge: bridges naturally accumulate energy from multiple paths
		allExpanded := make(map[string]float64)
		for _, token := range tokens {
			termID := q.Field + "\x00" + token
			spread := termMatrix.SpreadActivation(
				termID, entryEnergy, q.Hops, q.Decay,
				q.MaxFanOut, q.Epsilon, q.EnergyThreshold,
			)
			for id, w := range spread {
				if _, orig := originalSet[id]; orig {
					continue
				}
				allExpanded[id] += w // SUM, not MAX — bridges boosted naturally
			}
		}

		// Top 10 by accumulated energy
		const maxExpansions = 10
		selected := topNExpansions(allExpanded, maxExpansions)

		// Parallel search for expanded terms (semaphore of 4)
		var mu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, 4)

		for termID, weight := range selected {
			expandedTerms[termID] = weight
			wg.Add(1)
			sem <- struct{}{}
			go func(id string, w float64) {
				defer wg.Done()
				defer func() { <-sem }()

				// B1: DF pre-filter — skip terms that are too common
				if df := e.segmentMgr.GetDF(id); df > q.MaxDF {
					return
				}

				segmentHits, err := e.segmentMgr.SearchByTermID(id)
				if err != nil || len(segmentHits) == 0 {
					return
				}

				// Local scoring context — avoids concurrent map read/write on shared scoringCtx.TermDF
				localCtx := ScoringContext{
					TotalDocs: scoringCtx.TotalDocs,
					AvgDocLen: scoringCtx.AvgDocLen,
					TermDF:    map[string]int64{id: int64(len(segmentHits))},
				}

				// Apply ExpansionCap to limit expanded term influence
				boost := q.Boost * w * q.ExpansionCap

				// Score directly into shared docScores under mutex
				mu.Lock()
				e.scoreHitsInto(docScores, segmentHits, id, boost, localCtx)
				mu.Unlock()
			}(termID, weight)
		}
		wg.Wait()
	}

	// Phase 3: Build result
	hits := make([]Hit, 0, len(docScores))
	for docID, score := range docScores {
		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   score,
		})
	}

	e.hybridMetadata = &HybridExpansionInfo{
		OriginalTokens: tokens,
		ExpandedTerms:  expandedTerms,
		ExpansionHops:  q.Hops,
		TotalExpanded:  len(expandedTerms),
	}

	return hits, nil
}

// scoreHitsInto scores segment hits and accumulates directly into a docScores map,
// eliminating the intermediate []Hit allocation that convertAndScoreHits creates.
func (e *SegmentExecutor) scoreHitsInto(
	docScores map[string]float64,
	segmentHits []segment.Hit,
	termID string,
	boost float64,
	scoringCtx ScoringContext,
) {
	for _, sh := range segmentHits {
		match := TermMatch{
			TermID: termID,
			TF:     sh.TF,
			DocLen: 100,
			Boost:  boost,
		}
		score := e.scorer.Score(scoringCtx, match)
		docScores[sh.DocID] += score
	}
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

type weightedTerm struct {
	id     string
	weight float64
}

// topNExpansions returns the top N expanded terms by weight.
func topNExpansions(all map[string]float64, n int) map[string]float64 {
	if len(all) <= n {
		return all
	}
	sorted := make([]weightedTerm, 0, len(all))
	for id, w := range all {
		sorted = append(sorted, weightedTerm{id, w})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].weight > sorted[j].weight
	})
	result := make(map[string]float64, n)
	for i := 0; i < n; i++ {
		result[sorted[i].id] = sorted[i].weight
	}
	return result
}

// autoFuzzyDistance computes the max edit distance based on token length.
// Short tokens get less tolerance; longer tokens allow more corrections.
func autoFuzzyDistance(tokenLen int) int {
	switch {
	case tokenLen <= 2:
		return 0
	case tokenLen <= 5:
		return 1
	case tokenLen <= 8:
		return 2
	default:
		return 3
	}
}

// fuzzyFallback tries to find the best fuzzy match for a token that returned 0 hits.
// Returns the corrected term, its hits, and any error.
func (e *SegmentExecutor) fuzzyFallback(ctx context.Context, field, token string, maxDistance int) (string, []segment.Hit, error) {
	candidates, err := e.segmentMgr.ListTermsByFuzzy(ctx, field, token, maxDistance, 5)
	if err != nil || len(candidates) == 0 {
		return "", nil, err
	}

	// Candidates are sorted by DF descending — pick the best one
	best := candidates[0]
	hits, err := e.segmentMgr.Search(ctx, field, best.Term)
	if err != nil {
		return "", nil, err
	}
	return best.Term, hits, nil
}
