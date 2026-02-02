package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"plastic-engine-core/internal/adapters/storage/pebble"
	"plastic-engine-core/internal/core/search/document"
)

// Hit represents a matched document with its score.
type Hit struct {
	DocID   string  `json:"doc_id"`
	ShardID string  `json:"shard_id"`
	Score   float64 `json:"score"`
}

// ShardStore defines the storage operations required by the executor.
type ShardStore interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	GetInt64(key string) (int64, error)
	GetFloat64(key string) (float64, error)
	PrefixScan(prefix string) ([]pebble.KeyValue, error)
	PrefixScanKeys(prefix string) ([]string, error)
	// MergeInt64 atomically adds delta to the int64 at key (required by TermRegistry).
	MergeInt64(key string, delta int64) error
}

// Executor executes queries against a single shard.
type Executor struct {
	shardID      string
	store        ShardStore
	termRegistry *document.TermRegistry
	scorer       *BM25Scorer
}

// NewExecutor creates a query executor for a shard.
func NewExecutor(shardID string, store ShardStore) *Executor {
	return &Executor{
		shardID:      shardID,
		store:        store,
		termRegistry: document.NewTermRegistry(store),
		scorer:       NewBM25Scorer(),
	}
}

// Execute runs the query and returns matching hits.
func (e *Executor) Execute(ctx context.Context, req Request) ([]Hit, int64, error) {
	// Load scoring context
	scoringCtx, err := e.loadScoringContext(ctx, req)
	if err != nil {
		return nil, 0, fmt.Errorf("load scoring context: %w", err)
	}

	// Execute main query
	hits, err := e.executeClause(ctx, req.Query, scoringCtx)
	if err != nil {
		return nil, 0, fmt.Errorf("execute query: %w", err)
	}

	// Apply filters
	for _, filter := range req.Filters {
		hits, err = e.applyFilter(ctx, hits, filter)
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

func (e *Executor) executeClause(ctx context.Context, clause Clause, scoringCtx ScoringContext) ([]Hit, error) {
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

func (e *Executor) executeTerm(ctx context.Context, q TermQuery, scoringCtx ScoringContext) ([]Hit, error) {
	termValue := fmt.Sprintf("%v", q.Value)
	termValue = strings.ToLower(termValue) // Normalize

	// Get term from registry
	entry, found, err := e.termRegistry.Get(ctx, q.Field, termValue)
	if err != nil {
		return nil, fmt.Errorf("get term: %w", err)
	}
	if !found {
		return nil, nil // No matches
	}

	// Get DF for this term (stored separately)
	df, err := e.termRegistry.GetDF(ctx, q.Field, termValue)
	if err != nil {
		return nil, fmt.Errorf("get term df: %w", err)
	}

	// Update scoring context with this term's DF
	scoringCtx.TermDF[entry.TermID] = df

	// Scan postings
	return e.scanPostings(ctx, entry.TermID, q.Field, 1.0, scoringCtx)
}

func (e *Executor) executeMatch(ctx context.Context, q MatchQuery, scoringCtx ScoringContext) ([]Hit, error) {
	// Tokenize the query value
	tokens := tokenizeQuery(q.Value)
	if len(tokens) == 0 {
		return nil, nil
	}

	// Collect term IDs and their DFs
	termIDs := make([]string, 0, len(tokens))
	for _, token := range tokens {
		entry, found, err := e.termRegistry.Get(ctx, q.Field, token)
		if err != nil {
			return nil, fmt.Errorf("get term %s: %w", token, err)
		}
		if found {
			termIDs = append(termIDs, entry.TermID)
			// Get DF for this term (stored separately)
			df, err := e.termRegistry.GetDF(ctx, q.Field, token)
			if err != nil {
				return nil, fmt.Errorf("get term df %s: %w", token, err)
			}
			scoringCtx.TermDF[entry.TermID] = df
		}
	}

	if len(termIDs) == 0 {
		return nil, nil
	}

	// Aggregate hits across all terms
	docScores := make(map[string]float64)
	for _, termID := range termIDs {
		hits, err := e.scanPostings(ctx, termID, q.Field, q.Boost, scoringCtx)
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

func (e *Executor) executePrefix(ctx context.Context, q PrefixQuery, scoringCtx ScoringContext) ([]Hit, error) {
	// Scan n-gram index for matching term IDs
	prefix := pebble.NgramPrefix(q.Field, q.Value)
	keys, err := e.store.PrefixScanKeys(prefix)
	if err != nil {
		return nil, fmt.Errorf("scan ngrams: %w", err)
	}

	// Extract unique term IDs
	termIDSet := make(map[string]struct{})
	for _, key := range keys {
		_, _, termID, ok := pebble.ParseNgramKey(key)
		if ok {
			termIDSet[termID] = struct{}{}
		}
	}

	if len(termIDSet) == 0 {
		return nil, nil
	}

	// Aggregate hits across all matching terms
	docScores := make(map[string]float64)
	for termID := range termIDSet {
		// We need to get the DF for this term ID
		// For now, we'll scan the postings and count
		hits, err := e.scanPostingsWithDF(ctx, termID, q.Field, q.Boost, scoringCtx)
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

func (e *Executor) executeRange(_ context.Context, q RangeQuery) ([]Hit, error) {
	// Range queries would require scanning the forward index
	// This is a placeholder for future implementation
	_ = q
	return nil, fmt.Errorf("range queries not yet implemented")
}

func (e *Executor) scanPostings(ctx context.Context, termID, field string, boost float64, scoringCtx ScoringContext) ([]Hit, error) {
	prefix := pebble.InvertedPrefix(termID)
	kvs, err := e.store.PrefixScan(prefix)
	if err != nil {
		return nil, fmt.Errorf("scan postings: %w", err)
	}

	hits := make([]Hit, 0, len(kvs))
	for _, kv := range kvs {
		_, docID, ok := pebble.ParseInvertedKey(kv.Key)
		if !ok {
			continue
		}

		var posting document.Posting
		if err := json.Unmarshal([]byte(kv.Value), &posting); err != nil {
			continue // Skip malformed entries
		}

		// Get document length from forward index
		docLen := e.getDocumentLength(ctx, docID, field)

		// Calculate score
		match := TermMatch{
			TermID: termID,
			TF:     posting.TF,
			DocLen: docLen,
			Boost:  boost,
		}
		score := e.scorer.Score(scoringCtx, match)

		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   score,
		})
	}

	return hits, nil
}

func (e *Executor) scanPostingsWithDF(ctx context.Context, termID, field string, boost float64, scoringCtx ScoringContext) ([]Hit, error) {
	prefix := pebble.InvertedPrefix(termID)
	kvs, err := e.store.PrefixScan(prefix)
	if err != nil {
		return nil, fmt.Errorf("scan postings: %w", err)
	}

	// Use posting count as DF approximation
	df := int64(len(kvs))
	scoringCtx.TermDF[termID] = df

	hits := make([]Hit, 0, len(kvs))
	for _, kv := range kvs {
		_, docID, ok := pebble.ParseInvertedKey(kv.Key)
		if !ok {
			continue
		}

		var posting document.Posting
		if err := json.Unmarshal([]byte(kv.Value), &posting); err != nil {
			continue
		}

		docLen := e.getDocumentLength(ctx, docID, field)

		match := TermMatch{
			TermID: termID,
			TF:     posting.TF,
			DocLen: docLen,
			Boost:  boost,
		}
		score := e.scorer.Score(scoringCtx, match)

		hits = append(hits, Hit{
			DocID:   docID,
			ShardID: e.shardID,
			Score:   score,
		})
	}

	return hits, nil
}

func (e *Executor) getDocumentLength(_ context.Context, docID, field string) int {
	key := pebble.ForwardKey(docID)
	raw, err := e.store.Get(key)
	if err != nil {
		return 1 // Default to 1 to avoid division issues
	}

	var fwd struct {
		Fields map[string]struct {
			Tokens []struct{} `json:"tokens"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(raw), &fwd); err != nil {
		return 1
	}

	if fieldData, ok := fwd.Fields[field]; ok {
		return len(fieldData.Tokens)
	}
	return 1
}

func (e *Executor) applyFilter(_ context.Context, hits []Hit, filter Clause) ([]Hit, error) {
	// Filters are applied as post-filtering
	// For now, we'll support term filters only
	if filter.Term == nil {
		return hits, nil // Other filter types not yet implemented
	}

	termValue := fmt.Sprintf("%v", filter.Term.Value)
	termValue = strings.ToLower(termValue)

	var filtered []Hit
	for _, hit := range hits {
		// Check if document has the term in the specified field
		if e.documentHasTerm(hit.DocID, filter.Term.Field, termValue) {
			filtered = append(filtered, hit)
		}
	}

	return filtered, nil
}

func (e *Executor) documentHasTerm(docID, field, term string) bool {
	key := pebble.ForwardKey(docID)
	raw, err := e.store.Get(key)
	if err != nil {
		return false
	}

	var fwd struct {
		Fields map[string]struct {
			Tokens []struct {
				Term string `json:"term"`
			} `json:"tokens"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(raw), &fwd); err != nil {
		return false
	}

	if fieldData, ok := fwd.Fields[field]; ok {
		for _, token := range fieldData.Tokens {
			if strings.ToLower(token.Term) == term {
				return true
			}
		}
	}
	return false
}

func (e *Executor) loadScoringContext(_ context.Context, _ Request) (ScoringContext, error) {
	ctx := NewScoringContext()

	// Load total document count
	docCount, err := e.store.GetInt64(pebble.DocCountKey())
	if err != nil && !pebble.IsNotFound(err) {
		return ctx, fmt.Errorf("get doc count: %w", err)
	}
	ctx.TotalDocs = docCount

	// For avg doc len, we'll use a default if not available
	// This could be improved by specifying the field in the context
	ctx.AvgDocLen = 100 // Default average

	return ctx, nil
}

// tokenizeQuery splits and normalizes query text into tokens.
func tokenizeQuery(text string) []string {
	text = strings.ToLower(text)
	words := strings.Fields(text)

	tokens := make([]string, 0, len(words))
	for _, word := range words {
		// Remove common punctuation
		word = strings.Trim(word, ".,!?;:\"'()[]{}") 
		if word != "" {
			tokens = append(tokens, word)
		}
	}
	return tokens
}

