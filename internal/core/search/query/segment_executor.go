package query

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"plastic-engine-core/internal/core/search/knowledge"
	"plastic-engine-core/internal/core/search/indexstore"
)

// SegmentManager defines the interface for segment-based search.
type SegmentManager interface {
	Search(ctx context.Context, field, term string) ([]indexstore.Hit, error)
	SearchWithPositions(ctx context.Context, field, term string) ([]indexstore.Hit, error)
	SearchByTermID(termID string) ([]indexstore.Hit, error)
	GetDF(termID string) int64
	GetTotalDocs() int64
	GetAvgDocLen(field string) float64
	GetTermsWithPrefix(ctx context.Context, field, prefix string) []string
	ListTermsByFuzzy(ctx context.Context, field, query string, maxDistance, limit int) ([]indexstore.TermEntry, error)
	GetPositionsForDoc(field, term, docID string) ([]int, error)
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
	pipeline       indexstore.SearchPipeline
	hybridMetadata *HybridExpansionInfo
}

// NewSegmentExecutor creates a query executor for a segment-based shard with default pipeline.
func NewSegmentExecutor(shardID string, segmentMgr SegmentManager, analyzer QueryAnalyzer) *SegmentExecutor {
	pipeline := indexstore.DefaultSearchPipeline()
	return &SegmentExecutor{
		shardID:       shardID,
		segmentMgr:    segmentMgr,
		queryAnalyzer: analyzer,
		scorer:        NewBM25ScorerFromPipeline(pipeline.Scoring),
		pipeline:      pipeline,
	}
}

// NewSegmentExecutorWithPipeline creates a query executor with a resolved pipeline.
func NewSegmentExecutorWithPipeline(shardID string, segmentMgr SegmentManager, analyzer QueryAnalyzer, pipeline indexstore.SearchPipeline) *SegmentExecutor {
	return &SegmentExecutor{
		shardID:       shardID,
		segmentMgr:    segmentMgr,
		queryAnalyzer: analyzer,
		scorer:        NewBM25ScorerFromPipeline(pipeline.Scoring),
		pipeline:      pipeline,
	}
}

// resolveGraphConfig merges pipeline graph defaults with per-query HybridQuery overrides.
func (e *SegmentExecutor) resolveGraphConfig(q HybridQuery) indexstore.GraphTraversalConfig {
	cfg := e.pipeline.Graph
	if q.Hops > 0 {
		cfg.Hops = q.Hops
	}
	if q.Decay > 0 {
		cfg.Decay = q.Decay
	}
	if q.MaxFanOut > 0 {
		cfg.MaxFanOut = q.MaxFanOut
	}
	if q.Epsilon > 0 {
		cfg.Epsilon = q.Epsilon
	}
	if q.EnergyThreshold > 0 {
		cfg.EnergyThreshold = q.EnergyThreshold
	}
	if q.ExpansionCap > 0 {
		cfg.ExpansionCap = q.ExpansionCap
	}
	if q.MaxDF > 0 {
		cfg.MaxDF = q.MaxDF
	}
	return cfg
}

// Execute runs the query and returns matching hits.
func (e *SegmentExecutor) Execute(ctx context.Context, req Request) ([]Hit, int64, error) {
	// Load scoring context with field from the query clause
	field := extractField(req.Query)
	scoringCtx := e.loadScoringContext(field)

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
	case clause.Bool != nil:
		return e.executeBool(ctx, *clause.Bool, scoringCtx)
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

// topKForOrdering is the number of top hits to apply position ordering boost via point lookups.
const topKForOrdering = 50

func (e *SegmentExecutor) executeMatch(ctx context.Context, q MatchQuery, scoringCtx ScoringContext) ([]Hit, error) {
	tokens := e.queryAnalyzer.AnalyzeQuery(q.Value)
	if len(tokens) == 0 {
		return nil, nil
	}

	docScores := make(map[string]float64)
	docTermCounts := make(map[string]int)
	tokensWithHits := 0

	// Pass 1: Search() (no positions) → BM25 + coord factor
	for i, token := range tokens {
		segmentHits, err := e.segmentMgr.Search(ctx, q.Field, token)
		if err != nil {
			return nil, fmt.Errorf("search term %s: %w", token, err)
		}

		// Fuzzy fallback: if no exact hits and fuzziness enabled, try correction
		if len(segmentHits) == 0 && q.Fuzziness > 0 && len(token) >= 3 {
			dist := autoFuzzyDistance(len(token))
			if corrected, correctedHits, fErr := e.fuzzyFallback(ctx, q.Field, token, dist); fErr == nil && len(correctedHits) > 0 {
				tokens[i] = corrected
				token = corrected
				segmentHits = correctedHits
			}
		}

		if len(segmentHits) == 0 {
			continue
		}
		tokensWithHits++

		termID := q.Field + "\x00" + token
		scoringCtx.TermDF[termID] = int64(len(segmentHits))
		e.scoreHitsInto(docScores, segmentHits, termID, q.Boost, scoringCtx)

		for _, sh := range segmentHits {
			docTermCounts[sh.DocID]++
		}
	}

	// AND operator: remove docs that don't match all tokens that had hits
	if q.Operator == "and" && tokensWithHits > 1 {
		for docID := range docScores {
			if docTermCounts[docID] < tokensWithHits {
				delete(docScores, docID)
			}
		}
	}

	// Build hits with coord factor (no ordering yet)
	hits := make([]Hit, 0, len(docScores))
	for docID, score := range docScores {
		coord := CoordFactor(docTermCounts[docID], len(tokens), e.pipeline.Scoring.CoordWeight)
		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   score * coord,
		})
	}

	// Pass 2: Ordering boost via point lookups for top-K only
	if len(tokens) > 1 && len(hits) > 0 {
		sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
		topN := len(hits)
		if topN > topKForOrdering {
			topN = topKForOrdering
		}
		e.applyOrderingBoost(hits[:topN], q.Field, tokens)
	}

	return hits, nil
}

// applyOrderingBoost fetches positions via point lookups for top-K docs and applies ordering boost.
func (e *SegmentExecutor) applyOrderingBoost(hits []Hit, field string, tokens []string) {
	for i := range hits {
		positions := make([]int, 0, len(tokens))
		for _, token := range tokens {
			pos, err := e.segmentMgr.GetPositionsForDoc(field, token, hits[i].DocID)
			if err == nil && len(pos) > 0 {
				positions = append(positions, pos[0]) // first position (sorted asc in Pebble value)
			}
		}
		hits[i].Score *= OrderingBoost(positions, e.pipeline.Scoring.OrderingFactor)
	}
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

// conceptGroup holds per-concept scores for Boolean concept grouping (operator="and").
type conceptGroup struct {
	docScores map[string]float64
}

// rrfK is the standard Reciprocal Rank Fusion constant (same as Elasticsearch default).
const rrfK = 60

func (e *SegmentExecutor) executeHybrid(ctx context.Context, q HybridQuery, scoringCtx ScoringContext) ([]Hit, error) {
	tokens := e.queryAnalyzer.AnalyzeQuery(q.Value)
	if len(tokens) == 0 {
		return nil, nil
	}

	useConceptGroups := q.Operator == "and"

	// Shared state for flat OR mode; per-concept state for AND mode
	var flatScores map[string]float64
	var concepts []conceptGroup

	if useConceptGroups {
		concepts = make([]conceptGroup, len(tokens))
		for i := range concepts {
			concepts[i].docScores = make(map[string]float64)
		}
	} else {
		flatScores = make(map[string]float64)
	}

	// Tracking for coord (both modes)
	docTermCounts := make(map[string]int)
	tokensWithHits := 0

	// Phase 1: BM25 on original tokens (Search — no positions)
	for tokenIdx, token := range tokens {
		segmentHits, err := e.segmentMgr.Search(ctx, q.Field, token)
		if err != nil {
			return nil, fmt.Errorf("hybrid search term %s: %w", token, err)
		}

		if len(segmentHits) == 0 && q.Fuzziness > 0 && len(token) >= 3 {
			dist := autoFuzzyDistance(len(token))
			if corrected, correctedHits, fErr := e.fuzzyFallback(ctx, q.Field, token, dist); fErr == nil && len(correctedHits) > 0 {
				token = corrected
				tokens[tokenIdx] = corrected
				segmentHits = correctedHits
			}
		}

		if len(segmentHits) == 0 {
			continue
		}
		tokensWithHits++

		termID := q.Field + "\x00" + token
		scoringCtx.TermDF[termID] = int64(len(segmentHits))

		if useConceptGroups {
			e.scoreHitsInto(concepts[tokenIdx].docScores, segmentHits, termID, q.Boost, scoringCtx)
		} else {
			e.scoreHitsInto(flatScores, segmentHits, termID, q.Boost, scoringCtx)
		}

		for _, sh := range segmentHits {
			docTermCounts[sh.DocID]++
		}
	}

	// Phase 2: Graph expansion (sequential — no goroutines)
	graphCfg := e.resolveGraphConfig(q)
	expandedTerms := make(map[string]float64)
	termMatrix := e.segmentMgr.TermMatrix()

	// For RRF (flat OR mode): collect expansion scores into a separate map
	var expansionScores map[string]float64
	if !useConceptGroups {
		expansionScores = make(map[string]float64)
	}

	if termMatrix != nil {
		originalSet := make(map[string]struct{}, len(tokens))
		for _, t := range tokens {
			originalSet[q.Field+"\x00"+t] = struct{}{}
		}

		entryEnergy := 1.0 / float64(len(tokens))

		if useConceptGroups {
			// AND mode: keep additive fusion within concept groups (RRF not applicable)
			for tokenIdx, token := range tokens {
				termID := q.Field + "\x00" + token
				spread := termMatrix.SpreadActivation(
					termID, entryEnergy, graphCfg.Hops, graphCfg.Decay,
					graphCfg.MaxFanOut, graphCfg.Epsilon, graphCfg.EnergyThreshold,
				)
				filtered := make(map[string]float64)
				for id, w := range spread {
					if _, orig := originalSet[id]; !orig {
						filtered[id] = w
					}
				}
				selected := topNExpansions(filtered, 5)
				for termID, weight := range selected {
					expandedTerms[termID] = weight
					if df := e.segmentMgr.GetDF(termID); df > graphCfg.MaxDF {
						continue
					}
					segmentHits, err := e.segmentMgr.SearchByTermID(termID)
					if err != nil || len(segmentHits) == 0 {
						continue
					}
					localCtx := ScoringContext{
						TotalDocs: scoringCtx.TotalDocs,
						AvgDocLen: scoringCtx.AvgDocLen,
						TermDF:    map[string]int64{termID: int64(len(segmentHits))},
					}
					boost := q.Boost * weight * graphCfg.ExpansionCap
					e.scoreHitsInto(concepts[tokenIdx].docScores, segmentHits, termID, boost, localCtx)
				}
			}
		} else {
			// Flat OR: expansion scores go to separate map for RRF
			allExpanded := make(map[string]float64)
			for _, token := range tokens {
				termID := q.Field + "\x00" + token
				spread := termMatrix.SpreadActivation(
					termID, entryEnergy, graphCfg.Hops, graphCfg.Decay,
					graphCfg.MaxFanOut, graphCfg.Epsilon, graphCfg.EnergyThreshold,
				)
				for id, w := range spread {
					if _, orig := originalSet[id]; orig {
						continue
					}
					allExpanded[id] += w
				}
			}

			maxExp := graphCfg.MaxExpansions
			if maxExp <= 0 {
				maxExp = 10
			}
			selected := topNExpansions(allExpanded, maxExp)

			// Score expanded terms into separate map (not into flatScores)
			for termID, weight := range selected {
				expandedTerms[termID] = weight
				if df := e.segmentMgr.GetDF(termID); df > graphCfg.MaxDF {
					continue
				}
				segmentHits, err := e.segmentMgr.SearchByTermID(termID)
				if err != nil || len(segmentHits) == 0 {
					continue
				}
				localCtx := ScoringContext{
					TotalDocs: scoringCtx.TotalDocs,
					AvgDocLen: scoringCtx.AvgDocLen,
					TermDF:    map[string]int64{termID: int64(len(segmentHits))},
				}
				boost := q.Boost * weight * graphCfg.ExpansionCap
				e.scoreHitsInto(expansionScores, segmentHits, termID, boost, localCtx)
			}
		}
	}

	// Phase 3: Build result
	var hits []Hit

	if useConceptGroups {
		// AND mode: intersect concept groups (unchanged — additive fusion)
		activeGroups := 0
		smallestIdx := -1
		smallestSize := int(^uint(0) >> 1) // max int
		for i, cg := range concepts {
			if len(cg.docScores) == 0 {
				continue
			}
			activeGroups++
			if len(cg.docScores) < smallestSize {
				smallestSize = len(cg.docScores)
				smallestIdx = i
			}
		}

		if activeGroups > 0 && smallestIdx >= 0 {
			hits = make([]Hit, 0, smallestSize)
			for docID := range concepts[smallestIdx].docScores {
				totalScore := 0.0
				inAll := true
				for _, cg := range concepts {
					if len(cg.docScores) == 0 {
						continue
					}
					s, ok := cg.docScores[docID]
					if !ok {
						inAll = false
						break
					}
					totalScore += s
				}
				if !inAll {
					continue
				}
				coord := CoordFactor(docTermCounts[docID], len(tokens), e.pipeline.Scoring.CoordWeight)
				hits = append(hits, Hit{
					DocID:   docID,
					ShardID: e.shardID,
					Score:   totalScore * coord,
				})
			}

			// Fallback: if intersection is empty, fall back to union with coord penalty
			if len(hits) == 0 && activeGroups > 1 {
				merged := make(map[string]float64)
				for _, cg := range concepts {
					for docID, score := range cg.docScores {
						merged[docID] += score
					}
				}
				hits = make([]Hit, 0, len(merged))
				for docID, score := range merged {
					coord := CoordFactor(docTermCounts[docID], len(tokens), e.pipeline.Scoring.CoordWeight)
					hits = append(hits, Hit{
						DocID:   docID,
						ShardID: e.shardID,
						Score:   score * coord,
					})
				}
			}
		}

		// AND mode: ordering boost after building hits
		if len(tokens) > 1 && len(hits) > 0 {
			sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
			topN := len(hits)
			if topN > topKForOrdering {
				topN = topKForOrdering
			}
			e.applyOrderingBoost(hits[:topN], q.Field, tokens)
		}
	} else {
		// ── Flat OR: RRF fusion of lexical + expansion rankings ──

		// Build lexical hits with coord factor
		lexicalHits := make([]Hit, 0, len(flatScores))
		for docID, score := range flatScores {
			coord := CoordFactor(docTermCounts[docID], len(tokens), e.pipeline.Scoring.CoordWeight)
			lexicalHits = append(lexicalHits, Hit{
				DocID:   docID,
				ShardID: e.shardID,
				Score:   score * coord,
			})
		}

		// Ordering boost on top-K lexical hits (before RRF ranking)
		if len(tokens) > 1 && len(lexicalHits) > 0 {
			sort.Slice(lexicalHits, func(i, j int) bool { return lexicalHits[i].Score > lexicalHits[j].Score })
			topN := len(lexicalHits)
			if topN > topKForOrdering {
				topN = topKForOrdering
			}
			e.applyOrderingBoost(lexicalHits[:topN], q.Field, tokens)
		}

		// RRF merge if we have expansion scores; otherwise pure lexical
		if len(expansionScores) > 0 {
			hits = e.rrfMerge(lexicalHits, expansionScores)
		} else {
			hits = lexicalHits
		}
	}

	e.hybridMetadata = &HybridExpansionInfo{
		OriginalTokens: tokens,
		ExpandedTerms:  expandedTerms,
		ExpansionHops:  graphCfg.Hops,
		TotalExpanded:  len(expandedTerms),
	}

	return hits, nil
}

// rrfMerge combines lexical hits and expansion scores using Reciprocal Rank Fusion.
// Lexical hits are already scored (BM25 + coord + ordering); expansion scores are raw BM25.
// Each list is ranked independently, then combined: score = 1/(k+rank_lex) + blend/(k+rank_exp).
func (e *SegmentExecutor) rrfMerge(lexicalHits []Hit, expansionScores map[string]float64) []Hit {
	blend := e.pipeline.Scoring.ExpansionBlend

	// Sort lexical hits by score desc (may already be sorted, but ensure after ordering boost)
	sort.Slice(lexicalHits, func(i, j int) bool { return lexicalHits[i].Score > lexicalHits[j].Score })

	// Build lexical rank map (1-indexed)
	lexicalRank := make(map[string]int, len(lexicalHits))
	for i, h := range lexicalHits {
		lexicalRank[h.DocID] = i + 1
	}

	// Build expansion rank (1-indexed, sorted by score desc)
	type docScore struct {
		docID string
		score float64
	}
	expSlice := make([]docScore, 0, len(expansionScores))
	for docID, score := range expansionScores {
		expSlice = append(expSlice, docScore{docID, score})
	}
	sort.Slice(expSlice, func(i, j int) bool { return expSlice[i].score > expSlice[j].score })

	expansionRank := make(map[string]int, len(expSlice))
	for i, ds := range expSlice {
		expansionRank[ds.docID] = i + 1
	}

	// Collect all unique doc IDs from both rankings
	allDocs := make(map[string]struct{}, len(lexicalRank)+len(expansionRank))
	for docID := range lexicalRank {
		allDocs[docID] = struct{}{}
	}
	for docID := range expansionRank {
		allDocs[docID] = struct{}{}
	}

	kFloat := float64(rrfK)
	hits := make([]Hit, 0, len(allDocs))
	for docID := range allDocs {
		var rrfScore float64
		if rank, ok := lexicalRank[docID]; ok {
			rrfScore += 1.0 / (kFloat + float64(rank))
		}
		if rank, ok := expansionRank[docID]; ok {
			rrfScore += blend / (kFloat + float64(rank))
		}
		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   rrfScore,
		})
	}

	return hits
}

func (e *SegmentExecutor) executeBool(ctx context.Context, q BoolQuery, scoringCtx ScoringContext) ([]Hit, error) {
	docScores := make(map[string]float64)

	// Execute "must" clauses — all must match, scores summed.
	var mustSets []map[string]struct{}
	for _, clause := range q.Must {
		field := extractField(clause)
		cCtx := e.loadScoringContext(field)
		hits, err := e.executeClause(ctx, clause, cCtx)
		if err != nil {
			return nil, fmt.Errorf("bool.must: %w", err)
		}
		set := make(map[string]struct{}, len(hits))
		for _, h := range hits {
			docScores[h.DocID] += h.Score
			set[h.DocID] = struct{}{}
		}
		mustSets = append(mustSets, set)
	}

	// Execute "should" clauses — scores summed (OR semantics).
	for _, clause := range q.Should {
		field := extractField(clause)
		cCtx := e.loadScoringContext(field)
		hits, err := e.executeClause(ctx, clause, cCtx)
		if err != nil {
			return nil, fmt.Errorf("bool.should: %w", err)
		}
		for _, h := range hits {
			docScores[h.DocID] += h.Score
		}
	}

	// Execute "must_not" clauses — collect excluded doc IDs.
	excluded := make(map[string]struct{})
	for _, clause := range q.MustNot {
		field := extractField(clause)
		cCtx := e.loadScoringContext(field)
		hits, err := e.executeClause(ctx, clause, cCtx)
		if err != nil {
			return nil, fmt.Errorf("bool.must_not: %w", err)
		}
		for _, h := range hits {
			excluded[h.DocID] = struct{}{}
		}
	}

	// Build result: apply must intersection + must_not exclusion.
	hits := make([]Hit, 0, len(docScores))
	for docID, score := range docScores {
		if _, ex := excluded[docID]; ex {
			continue
		}
		// Check must intersection: doc must be in ALL must sets.
		inAll := true
		for _, set := range mustSets {
			if _, ok := set[docID]; !ok {
				inAll = false
				break
			}
		}
		if !inAll {
			continue
		}
		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   score,
		})
	}

	return hits, nil
}

// scoreHitsInto scores segment hits and accumulates directly into a docScores map,
// eliminating the intermediate []Hit allocation that convertAndScoreHits creates.
func (e *SegmentExecutor) scoreHitsInto(
	docScores map[string]float64,
	segmentHits []indexstore.Hit,
	termID string,
	boost float64,
	scoringCtx ScoringContext,
) {
	for _, sh := range segmentHits {
		docLen := sh.DocLen
		if docLen <= 0 {
			docLen = 1 // fallback pre-migration
		}
		match := TermMatch{
			TermID: termID,
			TF:     sh.TF,
			DocLen: docLen,
			Boost:  boost,
		}
		score := e.scorer.Score(scoringCtx, match)
		docScores[sh.DocID] += score
	}
}

func (e *SegmentExecutor) convertAndScoreHits(segmentHits []indexstore.Hit, termID string, boost float64, scoringCtx ScoringContext) ([]Hit, error) {
	hits := make([]Hit, 0, len(segmentHits))

	for _, sh := range segmentHits {
		docLen := sh.DocLen
		if docLen <= 0 {
			docLen = 1 // fallback pre-migration
		}
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

func (e *SegmentExecutor) loadScoringContext(field string) ScoringContext {
	ctx := NewScoringContext()
	ctx.TotalDocs = e.segmentMgr.GetTotalDocs()
	ctx.AvgDocLen = e.segmentMgr.GetAvgDocLen(field)
	if ctx.AvgDocLen <= 0 {
		ctx.AvgDocLen = 1
	}
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

// extractField returns the field name from a query clause.
func extractField(c Clause) string {
	switch {
	case c.Term != nil:
		return c.Term.Field
	case c.Match != nil:
		return c.Match.Field
	case c.Prefix != nil:
		return c.Prefix.Field
	case c.Hybrid != nil:
		return c.Hybrid.Field
	case c.Bool != nil:
		// Return field from first sub-clause for scoring context defaults.
		for _, sub := range c.Bool.Must {
			if f := extractField(sub); f != "" {
				return f
			}
		}
		for _, sub := range c.Bool.Should {
			if f := extractField(sub); f != "" {
				return f
			}
		}
		return ""
	default:
		return ""
	}
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
func (e *SegmentExecutor) fuzzyFallback(ctx context.Context, field, token string, maxDistance int) (string, []indexstore.Hit, error) {
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
