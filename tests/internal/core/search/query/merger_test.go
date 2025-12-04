package query_test

import (
	"context"
	"testing"

	"plastic-engine-core/internal/core/search/query"
)

func TestMergerMergeEmpty(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()

	response := merger.Merge(nil, 10)
	if len(response.Hits) != 0 {
		t.Errorf("expected empty hits, got %d", len(response.Hits))
	}
	if response.Total != 0 {
		t.Errorf("expected total=0, got %d", response.Total)
	}
}

func TestMergerMergeSingleShard(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()

	results := []query.ShardResult{
		{
			ShardID: "shard-1",
			Hits: []query.Hit{
				{DocID: "doc-1", ShardID: "shard-1", Score: 4.5},
				{DocID: "doc-2", ShardID: "shard-1", Score: 3.2},
				{DocID: "doc-3", ShardID: "shard-1", Score: 2.1},
			},
			Total: 3,
		},
	}

	response := merger.Merge(results, 10)

	if len(response.Hits) != 3 {
		t.Errorf("expected 3 hits, got %d", len(response.Hits))
	}
	if response.Total != 3 {
		t.Errorf("expected total=3, got %d", response.Total)
	}

	// Should be sorted by score descending
	if response.Hits[0].Score < response.Hits[1].Score {
		t.Error("hits not sorted by score descending")
	}
}

func TestMergerMergeMultipleShards(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()

	results := []query.ShardResult{
		{
			ShardID: "shard-1",
			Hits: []query.Hit{
				{DocID: "doc-1", ShardID: "shard-1", Score: 4.5},
				{DocID: "doc-3", ShardID: "shard-1", Score: 2.1},
			},
			Total: 2,
		},
		{
			ShardID: "shard-2",
			Hits: []query.Hit{
				{DocID: "doc-2", ShardID: "shard-2", Score: 3.8},
				{DocID: "doc-4", ShardID: "shard-2", Score: 1.5},
			},
			Total: 2,
		},
	}

	response := merger.Merge(results, 10)

	if len(response.Hits) != 4 {
		t.Errorf("expected 4 hits, got %d", len(response.Hits))
	}
	if response.Total != 4 {
		t.Errorf("expected total=4, got %d", response.Total)
	}

	// Verify sorted by score
	for i := 1; i < len(response.Hits); i++ {
		if response.Hits[i-1].Score < response.Hits[i].Score {
			t.Errorf("hits not sorted: %f < %f at positions %d, %d",
				response.Hits[i-1].Score, response.Hits[i].Score, i-1, i)
		}
	}

	// First hit should be doc-1 with highest score
	if response.Hits[0].DocID != "doc-1" {
		t.Errorf("first hit should be doc-1, got %s", response.Hits[0].DocID)
	}
}

func TestMergerMergeWithLimit(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()

	results := []query.ShardResult{
		{
			ShardID: "shard-1",
			Hits: []query.Hit{
				{DocID: "doc-1", ShardID: "shard-1", Score: 5.0},
				{DocID: "doc-2", ShardID: "shard-1", Score: 4.0},
				{DocID: "doc-3", ShardID: "shard-1", Score: 3.0},
			},
			Total: 3,
		},
		{
			ShardID: "shard-2",
			Hits: []query.Hit{
				{DocID: "doc-4", ShardID: "shard-2", Score: 4.5},
				{DocID: "doc-5", ShardID: "shard-2", Score: 2.5},
			},
			Total: 2,
		},
	}

	response := merger.Merge(results, 3)

	if len(response.Hits) != 3 {
		t.Errorf("expected 3 hits (limit), got %d", len(response.Hits))
	}
	if response.Total != 5 {
		t.Errorf("expected total=5 (all hits), got %d", response.Total)
	}

	// Top 3 should be: doc-1 (5.0), doc-4 (4.5), doc-2 (4.0)
	expectedDocs := []string{"doc-1", "doc-4", "doc-2"}
	for i, expected := range expectedDocs {
		if response.Hits[i].DocID != expected {
			t.Errorf("position %d: expected %s, got %s",
				i, expected, response.Hits[i].DocID)
		}
	}
}

func TestMergerMergeSkipsErrors(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()

	results := []query.ShardResult{
		{
			ShardID: "shard-1",
			Hits: []query.Hit{
				{DocID: "doc-1", ShardID: "shard-1", Score: 4.5},
			},
			Total: 1,
		},
		{
			ShardID: "shard-2",
			Error:   query.ErrShardNotFound,
		},
		{
			ShardID: "shard-3",
			Hits: []query.Hit{
				{DocID: "doc-2", ShardID: "shard-3", Score: 3.0},
			},
			Total: 1,
		},
	}

	response := merger.Merge(results, 10)

	// Should skip errored shard
	if len(response.Hits) != 2 {
		t.Errorf("expected 2 hits (skipping error), got %d", len(response.Hits))
	}
	if response.Total != 2 {
		t.Errorf("expected total=2, got %d", response.Total)
	}
}

func TestMergerMergeStreaming(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()
	ctx := context.Background()

	// Create input channel
	in := make(chan query.ShardResult, 2)
	in <- query.ShardResult{
		ShardID: "shard-1",
		Hits: []query.Hit{
			{DocID: "doc-1", ShardID: "shard-1", Score: 4.5},
		},
		Total: 1,
	}
	in <- query.ShardResult{
		ShardID: "shard-2",
		Hits: []query.Hit{
			{DocID: "doc-2", ShardID: "shard-2", Score: 3.0},
		},
		Total: 1,
	}
	close(in)

	out := merger.MergeStreaming(ctx, in, 10)

	var hits []query.Hit
	for hit := range out {
		hits = append(hits, hit)
	}

	if len(hits) != 2 {
		t.Errorf("expected 2 hits from stream, got %d", len(hits))
	}
}

func TestMergerMergeStreamingWithCancel(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()
	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan query.ShardResult)

	out := merger.MergeStreaming(ctx, in, 10)

	// Cancel before sending anything
	cancel()

	// Should not block indefinitely
	for range out {
		// drain
	}
}

func TestMergerTopKSelection(t *testing.T) {
	t.Parallel()

	merger := query.NewMerger()

	// Create many hits to test top-K heap
	var hits []query.Hit
	for i := 0; i < 100; i++ {
		hits = append(hits, query.Hit{
			DocID:   "doc-" + string(rune('A'+i%26)) + string(rune('0'+i/26)),
			ShardID: "shard-1",
			Score:   float64(i), // Scores 0-99
		})
	}

	results := []query.ShardResult{
		{ShardID: "shard-1", Hits: hits, Total: 100},
	}

	response := merger.Merge(results, 5)

	if len(response.Hits) != 5 {
		t.Errorf("expected 5 hits (top-K), got %d", len(response.Hits))
	}

	// Top 5 should have scores 99, 98, 97, 96, 95
	expectedScores := []float64{99, 98, 97, 96, 95}
	for i, expected := range expectedScores {
		if response.Hits[i].Score != expected {
			t.Errorf("position %d: expected score %f, got %f",
				i, expected, response.Hits[i].Score)
		}
	}
}

