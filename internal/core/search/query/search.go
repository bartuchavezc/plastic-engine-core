package query

import (
	"context"
	"sync"

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

// ShardProvider provides access to shards for query execution.
type ShardProvider interface {
	// GetShardsByIndex returns all shard IDs for the given index.
	GetShardsByIndex(indexID string) []string
	// GetShardStore returns the store for a specific shard.
	GetShardStore(shardID string) (ShardStore, bool)
}

// SearchService coordinates search across multiple shards.
type SearchService struct {
	shardProvider ShardProvider
	log           logger.Logger
}

// NewSearchService creates a new search service.
func NewSearchService(provider ShardProvider, log logger.Logger) *SearchService {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &SearchService{
		shardProvider: provider,
		log:           log,
	}
}

// Search executes the query across all relevant shards and returns aggregated results.
func (s *SearchService) Search(ctx context.Context, req Request) (Response, error) {
	// Get shards for this index
	shardIDs := s.shardProvider.GetShardsByIndex(req.IndexID)
	if len(shardIDs) == 0 {
		return Response{Hits: []Hit{}, Total: 0}, nil
	}

	// Execute on all shards in parallel
	results := s.executeParallel(ctx, shardIDs, req)

	// Merge results
	return s.mergeResults(results, req.Limit), nil
}

// SearchStream returns a channel of shard results for streaming.
func (s *SearchService) SearchStream(ctx context.Context, req Request) <-chan ShardResult {
	shardIDs := s.shardProvider.GetShardsByIndex(req.IndexID)
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
	store, ok := s.shardProvider.GetShardStore(shardID)
	if !ok {
		return ShardResult{
			ShardID: shardID,
			Error:   ErrShardNotFound,
		}
	}

	executor := NewExecutor(shardID, store)
	hits, total, err := executor.Execute(ctx, req)

	return ShardResult{
		ShardID: shardID,
		Hits:    hits,
		Total:   total,
		Error:   err,
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
