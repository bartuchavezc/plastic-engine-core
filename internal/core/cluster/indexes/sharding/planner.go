package sharding

import (
	"context"
	"fmt"
	"strings"
	"time"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
)

// Planner decides which shards must be materialised for an index definition.
type Planner interface {
	PlanInitialShards(ctx context.Context, def indexes.IndexDefinition, seeds []string) ([]ShardSpec, error)
}

// ShardSpec captures the required information to persist a shard.
type ShardSpec struct {
	Key string
}

// PlannerFactory resolves a planner for a given shard strategy.
type PlannerFactory struct {
	planners map[indexes.ShardStrategy]Planner
}

// NewFactory registers the supported planners.
func NewFactory() *PlannerFactory {
	return &PlannerFactory{
		planners: map[indexes.ShardStrategy]Planner{
			indexes.ShardStrategyAutomatic: automaticPlanner{},
			indexes.ShardStrategyDate:      datePlanner{},
			indexes.ShardStrategyComputed:  computedPlanner{},
		},
	}
}

// Get returns the planner associated with the strategy.
func (f *PlannerFactory) Get(strategy indexes.ShardStrategy) (Planner, error) {
	planner, ok := f.planners[strategy]
	if !ok {
		return nil, fmt.Errorf("unsupported shard strategy %q", strategy)
	}
	return planner, nil
}

type automaticPlanner struct{}

func (automaticPlanner) PlanInitialShards(_ context.Context, def indexes.IndexDefinition, _ []string) ([]ShardSpec, error) {
	count := 1
	if cfg := def.ShardConfig.Automatic; cfg != nil && cfg.ShardCount > 1 {
		count = cfg.ShardCount
	}

	specs := make([]ShardSpec, 0, count)
	if count <= 1 {
		specs = append(specs, ShardSpec{Key: "default"})
		return specs, nil
	}

	for i := 0; i < count; i++ {
		specs = append(specs, ShardSpec{Key: fmt.Sprintf("h%02d", i)})
	}
	return specs, nil
}

type datePlanner struct{}

func (datePlanner) PlanInitialShards(_ context.Context, def indexes.IndexDefinition, seeds []string) ([]ShardSpec, error) {
	if def.ShardConfig.Date == nil || def.ShardConfig.Date.Field == "" {
		return nil, fmt.Errorf("date sharding requires configuration")
	}

	if len(seeds) == 0 {
		now := time.Now().UTC()
		key := formatDateKey(now, def.ShardConfig.Date.Granularity)
		return []ShardSpec{{Key: key}}, nil
	}

	unique := uniqueStrings(seeds)
	specs := make([]ShardSpec, 0, len(unique))
	for _, seed := range unique {
		specs = append(specs, ShardSpec{Key: sanitizeKey(seed)})
	}
	return specs, nil
}

type computedPlanner struct{}

func (computedPlanner) PlanInitialShards(_ context.Context, _ indexes.IndexDefinition, seeds []string) ([]ShardSpec, error) {
	if len(seeds) == 0 {
		// TODO(bypasscash): derive computed shard seeds from mapping catalogues or allow on-demand creation.
		return []ShardSpec{{Key: "default"}}, nil
	}

	unique := uniqueStrings(seeds)
	specs := make([]ShardSpec, 0, len(unique))
	for _, seed := range unique {
		specs = append(specs, ShardSpec{Key: sanitizeKey(seed)})
	}
	return specs, nil
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))

	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}

	return result
}

func sanitizeKey(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, " ", "-")
	value = strings.ReplaceAll(value, "/", "-")
	return value
}

func formatDateKey(t time.Time, granularity indexes.DateGranularity) string {
	switch granularity {
	case indexes.DateGranularityYear:
		return fmt.Sprintf("%04d", t.Year())
	case indexes.DateGranularityDay:
		return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
	default:
		return fmt.Sprintf("%04d-%02d", t.Year(), t.Month())
	}
}
