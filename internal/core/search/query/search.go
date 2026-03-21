package query

import (
	"context"
	"sync"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/core/search/indexstore"
	"plastic-engine-core/internal/pkg/logger"
)

// Hit represents a matched document with its score.
type Hit struct {
	DocID   string  `json:"doc_id"`
	ShardID string  `json:"shard_id"`
	Score   float64 `json:"score"`
}

// ShardResult encapsulates the result from a single shard query.
type ShardResult struct {
	ShardID  string
	Hits     []Hit
	Total    int64
	Error    error
	Metadata map[string]any
}

// Response is the aggregated search response.
type Response struct {
	Hits     []Hit          `json:"hits"`
	Total    int64          `json:"total"`
	Cursor   string         `json:"cursor,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// SegmentManagerWithPipeline bundles a segment manager with the index's search pipeline.
type SegmentManagerWithPipeline struct {
	Manager  SegmentManager
	Pipeline indexstore.SearchPipeline
}

// SegmentManagerGetter provides access to segment managers (with pipeline) by shard ID.
type SegmentManagerGetter func(shardID string) (SegmentManagerWithPipeline, bool)

// SearchService coordinates search across multiple shards.
// It assumes shard routing has been resolved externally.
type SearchService struct {
	getSegmentManager SegmentManagerGetter
	queryAnalyzer     QueryAnalyzer // Shared query analyzer (tokenize+stem+stopwords)
	log               logger.Logger
}

// NewSearchService creates a new search service using segment-based storage.
// analyzer may be nil — in that case queries use basic whitespace tokenization.
func NewSearchService(getter SegmentManagerGetter, analyzer QueryAnalyzer, log logger.Logger) *SearchService {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &SearchService{
		getSegmentManager: getter,
		queryAnalyzer:     analyzer,
		log:               log,
	}
}

// Search executes the query across all relevant shards and returns aggregated results.
// It assumes shard routing has been resolved and shard_ids are provided in the request.
func (s *SearchService) Search(ctx context.Context, req Request) (Response, error) {
	shardIDs := req.ShardIDs
	if len(shardIDs) == 0 {
		return Response{Hits: []Hit{}, Total: 0}, nil
	}

	start := time.Now()

	// Execute on all shards in parallel
	results := s.executeParallel(ctx, shardIDs, req)

	// Merge results
	response := s.mergeResults(results, req.Limit)

	// Record metrics
	if mp := metrics.Global(); mp != nil {
		queryType := detectQueryType(req.Query)
		mp.RecordSearchQueryMetrics(ctx, queryType, len(shardIDs), response.Total, time.Since(start))
	}

	return response, nil
}

// detectQueryType returns the type of query for metrics labeling.
func detectQueryType(clause Clause) string {
	switch {
	case clause.Term != nil:
		return "term"
	case clause.Match != nil:
		return "match"
	case clause.Prefix != nil:
		return "prefix"
	case clause.Range != nil:
		return "range"
	case clause.Hybrid != nil:
		return "hybrid"
	default:
		return "unknown"
	}
}

// SearchStream returns a channel of shard results for streaming.
func (s *SearchService) SearchStream(ctx context.Context, req Request) <-chan ShardResult {
	shardIDs := req.ShardIDs
	resultCh := make(chan ShardResult, len(shardIDs))

	go func() {
		defer close(resultCh)

		var wg sync.WaitGroup
		for _, shardID := range shardIDs {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				result := s.executeOnShard(ctx, id, req)
				select {
				case resultCh <- result:
				case <-ctx.Done():
				}
			}(shardID)
		}
		wg.Wait()
	}()

	return resultCh
}

func (s *SearchService) executeParallel(ctx context.Context, shardIDs []string, req Request) []ShardResult {
	results := make([]ShardResult, len(shardIDs))
	var wg sync.WaitGroup

	for i, shardID := range shardIDs {
		wg.Add(1)
		go func(idx int, id string) {
			defer wg.Done()
			results[idx] = s.executeOnShard(ctx, id, req)
		}(i, shardID)
	}

	wg.Wait()
	return results
}

func (s *SearchService) executeOnShard(ctx context.Context, shardID string, req Request) ShardResult {
	smp, ok := s.getSegmentManager(shardID)
	if !ok {
		return ShardResult{
			ShardID: shardID,
			Error:   ErrShardNotFound,
		}
	}

	// Resolve pipeline: global default → index default → request override
	pipeline := indexstore.DefaultSearchPipeline()
	pipeline = pipeline.Merge(smp.Pipeline)
	if req.Pipeline != nil {
		pipeline = pipeline.Merge(*req.Pipeline)
	}

	executor := NewSegmentExecutorWithPipeline(shardID, smp.Manager, s.queryAnalyzer, pipeline)
	hits, total, err := executor.Execute(ctx, req)
	result := ShardResult{
		ShardID: shardID,
		Hits:    hits,
		Total:   total,
		Error:   err,
	}
	if executor.hybridMetadata != nil {
		result.Metadata = map[string]any{"hybrid_expansion": executor.hybridMetadata}
	}
	return result
}

func (s *SearchService) mergeResults(results []ShardResult, limit int) Response {
	merger := NewMerger()
	return merger.Merge(results, limit)
}

// ErrShardNotFound indicates the shard could not be found.
var ErrShardNotFound = &ValidationError{
	Field:   "shard",
	Message: "shard not found",
}
