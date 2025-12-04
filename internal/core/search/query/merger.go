package query

import (
	"container/heap"
	"context"
)

// Merger combines results from multiple shards.
type Merger struct{}

// NewMerger creates a new result merger.
func NewMerger() *Merger {
	return &Merger{}
}

// Merge combines shard results into a single response, sorted by score descending.
func (m *Merger) Merge(results []ShardResult, limit int) Response {
	// Count total and collect all hits
	var totalHits int64
	allHits := make([]Hit, 0)

	for _, result := range results {
		if result.Error != nil {
			continue // Skip errored shards
		}
		totalHits += result.Total
		allHits = append(allHits, result.Hits...)
	}

	if len(allHits) == 0 {
		return Response{
			Hits:  []Hit{},
			Total: totalHits,
		}
	}

	// Use a min-heap to get top-K efficiently
	topK := m.selectTopK(allHits, limit)

	return Response{
		Hits:  topK,
		Total: totalHits,
	}
}

// MergeStreaming merges results from a channel for streaming use cases.
func (m *Merger) MergeStreaming(ctx context.Context, in <-chan ShardResult, limit int) <-chan Hit {
	out := make(chan Hit)

	go func() {
		defer close(out)

		// Collect all results first (for proper scoring merge)
		var results []ShardResult
		for {
			select {
			case result, ok := <-in:
				if !ok {
					// Input channel closed, process results
					response := m.Merge(results, limit)
					for _, hit := range response.Hits {
						select {
						case out <- hit:
						case <-ctx.Done():
							return
						}
					}
					return
				}
				results = append(results, result)
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

// selectTopK returns the top K hits by score using a min-heap.
func (m *Merger) selectTopK(hits []Hit, k int) []Hit {
	if k <= 0 || len(hits) == 0 {
		return []Hit{}
	}

	if len(hits) <= k {
		// Just sort all if we need them all - use max-heap
		h := &hitHeap{}
		for _, hit := range hits {
			heap.Push(h, hit)
		}

		// Max-heap: Pop returns largest first, so fill result from start
		result := make([]Hit, len(hits))
		for i := 0; i < len(hits); i++ {
			result[i] = heap.Pop(h).(Hit)
		}
		return result
	}

	// Use min-heap to track top K
	h := &hitMinHeap{}
	heap.Init(h)

	for _, hit := range hits {
		if h.Len() < k {
			heap.Push(h, hit)
		} else if hit.Score > (*h)[0].Score {
			heap.Pop(h)
			heap.Push(h, hit)
		}
	}

	// Min-heap: Pop returns smallest first, so fill result from end
	result := make([]Hit, h.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(Hit)
	}

	return result
}

// hitHeap is a max-heap of hits by score (for extracting in order).
type hitHeap []Hit

func (h hitHeap) Len() int           { return len(h) }
func (h hitHeap) Less(i, j int) bool { return h[i].Score > h[j].Score } // Max heap
func (h hitHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *hitHeap) Push(x any) {
	*h = append(*h, x.(Hit))
}

func (h *hitHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// hitMinHeap is a min-heap of hits by score (for top-K selection).
type hitMinHeap []Hit

func (h hitMinHeap) Len() int           { return len(h) }
func (h hitMinHeap) Less(i, j int) bool { return h[i].Score < h[j].Score } // Min heap
func (h hitMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *hitMinHeap) Push(x any) {
	*h = append(*h, x.(Hit))
}

func (h *hitMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

