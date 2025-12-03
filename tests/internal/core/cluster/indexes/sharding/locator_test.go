package sharding_test

import (
	"encoding/json"
	"testing"
	"time"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/indexes/sharding"
)

func TestComputeShardKeyAutomatic(t *testing.T) {
	t.Parallel()

	def := indexes.IndexDefinition{
		ID:            "idx-auto",
		ShardStrategy: indexes.ShardStrategyAutomatic,
		ShardConfig: indexes.ShardConfig{
			Strategy: indexes.ShardStrategyAutomatic,
			Automatic: &indexes.AutomaticShardConfig{
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

	def := indexes.IndexDefinition{
		ID:            "idx-date",
		ShardStrategy: indexes.ShardStrategyDate,
		ShardConfig: indexes.ShardConfig{
			Strategy: indexes.ShardStrategyDate,
			Date: &indexes.DateShardConfig{
				Field:       "created_at",
				Granularity: indexes.DateGranularityMonth,
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

	def := indexes.IndexDefinition{
		ID:            "idx-computed",
		ShardStrategy: indexes.ShardStrategyComputed,
		ShardConfig: indexes.ShardConfig{
			Strategy: indexes.ShardStrategyComputed,
			Computed: &indexes.ComputedShardConfig{
				Components: []indexes.ComputedComponent{
					{Field: "region", Transform: indexes.ComputedTransformSlug},
					{Field: "env", Transform: indexes.ComputedTransformExact},
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

	def := indexes.IndexDefinition{
		ID:            "idx-date-epoch",
		ShardStrategy: indexes.ShardStrategyDate,
		ShardConfig: indexes.ShardConfig{
			Strategy: indexes.ShardStrategyDate,
			Date: &indexes.DateShardConfig{
				Field:       "created_at",
				Granularity: indexes.DateGranularityDay,
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
