package sharding

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
)

// RoutingMetadata captures optional overrides for legacy strategies.
type RoutingMetadata map[string]string

// ComputeShardKey resolves the shard key for an incoming document based on the index definition.
func ComputeShardKey(def indexes.IndexDefinition, documentID string, payload json.RawMessage, routing RoutingMetadata) (string, error) {
	switch def.ShardStrategy {
	case indexes.ShardStrategyAutomatic:
		return computeAutomaticKey(def, documentID, payload, routing)
	case indexes.ShardStrategyDate:
		return computeDateKey(def, payload, routing)
	case indexes.ShardStrategyComputed:
		return computeComputedKey(def, payload, routing)
	default:
		return "", fmt.Errorf("unsupported shard strategy %q", def.ShardStrategy)
	}
}

func computeAutomaticKey(def indexes.IndexDefinition, documentID string, payload json.RawMessage, routing RoutingMetadata) (string, error) {
	cfg := def.ShardConfig.Automatic
	if cfg == nil {
		return "default", nil
	}

	shardCount := cfg.ShardCount
	if shardCount <= 1 {
		return "default", nil
	}

	seedValue := strings.TrimSpace(documentID)
	if cfg.Field != "" {
		if value, ok, err := extractTopLevelString(payload, cfg.Field); err == nil && ok && value != "" {
			seedValue = value
		}
		if seedValue == "" && routing != nil {
			seedValue = strings.TrimSpace(routing[cfg.Field])
		}
	}

	if seedValue == "" {
		return "", fmt.Errorf("automatic sharding requires non-empty seed")
	}

	hash := fnv.New32a()
	if _, err := hash.Write([]byte(seedValue)); err != nil {
		return "", fmt.Errorf("hash automatic shard seed: %w", err)
	}
	slot := int(hash.Sum32() % uint32(shardCount))
	return fmt.Sprintf("h%02d", slot), nil
}

func computeDateKey(def indexes.IndexDefinition, payload json.RawMessage, routing RoutingMetadata) (string, error) {
	cfg := def.ShardConfig.Date
	if cfg == nil || cfg.Field == "" {
		return "", fmt.Errorf("date sharding requires configuration")
	}

	value, ok, err := extractTopLevelString(payload, cfg.Field)
	if err != nil {
		return "", err
	}
	if !ok && routing != nil {
		value = routing[cfg.Field]
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("date shard field %q missing in document", cfg.Field)
	}

	timestamp, err := parseDateValue(value)
	if err != nil {
		return "", fmt.Errorf("parse date shard field %q: %w", cfg.Field, err)
	}

	return formatDateKey(timestamp.UTC(), cfg.Granularity), nil
}

func computeComputedKey(def indexes.IndexDefinition, payload json.RawMessage, routing RoutingMetadata) (string, error) {
	cfg := def.ShardConfig.Computed
	if cfg == nil || len(cfg.Components) == 0 {
		return "", fmt.Errorf("computed sharding requires at least one component")
	}

	parts := make([]string, 0, len(cfg.Components))
	for _, component := range cfg.Components {
		value, ok, err := extractTopLevelString(payload, component.Field)
		if err != nil {
			return "", err
		}
		if (!ok || strings.TrimSpace(value) == "") && routing != nil {
			value = routing[component.Field]
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("computed shard component %q missing in document", component.Field)
		}
		parts = append(parts, transformComponentValue(value, component.Transform))
	}

	return strings.Join(parts, "-"), nil
}

func extractTopLevelString(payload json.RawMessage, field string) (string, bool, error) {
	if len(payload) == 0 {
		return "", false, nil
	}

	result := gjson.GetBytes(payload, field)
	if !result.Exists() {
		return "", false, nil
	}

	// gjson handles type conversion automatically
	switch result.Type {
	case gjson.String:
		return strings.TrimSpace(result.String()), true, nil
	case gjson.Number:
		// Preserve numeric precision
		return result.Raw, true, nil
	case gjson.True:
		return "true", true, nil
	case gjson.False:
		return "false", true, nil
	case gjson.Null:
		return "", true, nil
	default:
		// For objects/arrays, return the raw JSON
		return strings.TrimSpace(result.Raw), true, nil
	}
}

func transformComponentValue(value string, transform indexes.ComputedTransform) string {
	switch transform {
	case indexes.ComputedTransformExact:
		return sanitizeKey(value)
	case indexes.ComputedTransformSlug:
		return sanitizeKey(value)
	default:
		return sanitizeKey(value)
	}
}

func parseDateValue(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
		"2006-01",
		"2006/01/02",
		"2006/01",
	}

	if unix, err := strconv.ParseInt(value, 10, 64); err == nil {
		switch len(value) {
		case 13:
			return time.UnixMilli(unix), nil
		case 16:
			return time.UnixMicro(unix), nil
		default:
			return time.Unix(unix, 0), nil
		}
	}

	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported date layout")
}
