package sharding_test

import (
	"encoding/json"
	"testing"
	"time"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/index/sharding"
)

func TestComputeShardKeyAutomatic(t *testing.T) {
	t.Parallel()

	def := coreindex.IndexDefinition{
		ID:            "idx-auto",
		ShardStrategy: coreindex.ShardStrategyAutomatic,
		ShardConfig: coreindex.ShardConfig{
			Strategy: coreindex.ShardStrategyAutomatic,
			Automatic: &coreindex.AutomaticShardConfig{
				ShardCount: 4,
			},
		},
	}

	key, err := sharding.ComputeShardKey(def, "doc-123", nil, nil)
	if err != nil {
		t.Fatalf("ComputeShardKey: %v", err)
	}
	if key == "" {
		t.Fatalf("expected shard key, got empty")
	}
}

func TestComputeShardKeyDate(t *testing.T) {
	t.Parallel()

	def := coreindex.IndexDefinition{
		ID:            "idx-date",
		ShardStrategy: coreindex.ShardStrategyDate,
		ShardConfig: coreindex.ShardConfig{
			Strategy: coreindex.ShardStrategyDate,
			Date: &coreindex.DateShardConfig{
				Field:       "created_at",
				Granularity: coreindex.DateGranularityMonth,
			},
		},
	}

	payload := map[string]string{
		"created_at": "2025-11-24T15:04:05Z",
	}
	body, _ := json.Marshal(payload)

	key, err := sharding.ComputeShardKey(def, "doc-1", body, nil)
	if err != nil {
		t.Fatalf("ComputeShardKey: %v", err)
	}
	if key != "2025-11" {
		t.Fatalf("key = %q, want %q", key, "2025-11")
	}
}

func TestComputeShardKeyComputed(t *testing.T) {
	t.Parallel()

	def := coreindex.IndexDefinition{
		ID:            "idx-computed",
		ShardStrategy: coreindex.ShardStrategyComputed,
		ShardConfig: coreindex.ShardConfig{
			Strategy: coreindex.ShardStrategyComputed,
			Computed: &coreindex.ComputedShardConfig{
				Components: []coreindex.ComputedComponent{
					{Field: "region", Transform: coreindex.ComputedTransformSlug},
					{Field: "env", Transform: coreindex.ComputedTransformExact},
				},
			},
		},
	}

	payload := map[string]string{
		"region": "US East",
		"env":    "prod",
	}
	body, _ := json.Marshal(payload)

	key, err := sharding.ComputeShardKey(def, "doc-1", body, nil)
	if err != nil {
		t.Fatalf("ComputeShardKey: %v", err)
	}
	if key != "us-east-prod" {
		t.Fatalf("key = %q, want %q", key, "us-east-prod")
	}
}

func TestComputeShardKeyDateAcceptsEpochMillis(t *testing.T) {
	t.Parallel()

	def := coreindex.IndexDefinition{
		ID:            "idx-date-epoch",
		ShardStrategy: coreindex.ShardStrategyDate,
		ShardConfig: coreindex.ShardConfig{
			Strategy: coreindex.ShardStrategyDate,
			Date: &coreindex.DateShardConfig{
				Field:       "created_at",
				Granularity: coreindex.DateGranularityDay,
			},
		},
	}

	ts := time.Date(2025, 11, 24, 0, 0, 0, 0, time.UTC).UnixMilli()
	payload := map[string]any{
		"created_at": ts,
	}
	body, _ := json.Marshal(payload)

	key, err := sharding.ComputeShardKey(def, "doc-1", body, nil)
	if err != nil {
		t.Fatalf("ComputeShardKey: %v", err)
	}
	if key != "2025-11-24" {
		t.Fatalf("key = %q, want %q", key, "2025-11-24")
	}
}
