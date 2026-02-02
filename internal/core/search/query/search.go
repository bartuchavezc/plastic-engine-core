package query

import (
	"context"
	"sync"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/pkg/logger"
)

// ShardResult encapsulates the result from a single shard query.
type ShardResult struct {
	ShardID string
	Hits    []Hit
	Total   int64
	Error   error
}

// Response is the aggregated search response.
type Response struct {
	Hits   []Hit  `json:"hits"`
	Total  int64  `json:"total"`
	Cursor string `json:"cursor,omitempty"`
}

// ShardStoreGetter provides access to shard stores by ID (legacy Pebble-based).
type ShardStoreGetter func(shardID string) (ShardStore, bool)

// SegmentManagerGetter provides access to segment managers by shard ID.
type SegmentManagerGetter func(shardID string) (SegmentManager, bool)

// SearchService coordinates search across multiple shards.
// It assumes shard routing has been resolved externally.
type SearchService struct {
	getShardStore     ShardStoreGetter    // Legacy Pebble-based (will be nil after migration)
	getSegmentManager SegmentManagerGetter // Segment-based
	log               logger.Logger
}

// NewSearchService creates a new search service (legacy Pebble-based).
func NewSearchService(getter ShardStoreGetter, log logger.Logger) *SearchService {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &SearchService{
		getShardStore: getter,
		log:           log,
	}
}

// NewSegmentSearchService creates a new search service using segment-based storage.
func NewSegmentSearchService(getter SegmentManagerGetter, log logger.Logger) *SearchService {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &SearchService{
		getSegmentManager: getter,
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
	// Try segment-based storage first
	if s.getSegmentManager != nil {
		segMgr, ok := s.getSegmentManager(shardID)
		if ok {
			executor := NewSegmentExecutor(shardID, segMgr)
			hits, total, err := executor.Execute(ctx, req)
			return ShardResult{
				ShardID: shardID,
				Hits:    hits,
				Total:   total,
				Error:   err,
			}
		}
	}

	// Fall back to legacy Pebble-based storage
	if s.getShardStore != nil {
		store, ok := s.getShardStore(shardID)
		if ok {
			executor := NewExecutor(shardID, store)
			hits, total, err := executor.Execute(ctx, req)
			return ShardResult{
				ShardID: shardID,
				Hits:    hits,
				Total:   total,
				Error:   err,
			}
		}
	}

	return ShardResult{
		ShardID: shardID,
		Error:   ErrShardNotFound,
	}
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
