package documents

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"plastic-engine-core/internal/core/cluster/indexes"
)

// NormalizePayload validates and coerces the incoming payload according to the index mapping.
func NormalizePayload(def indexes.IndexDefinition, raw json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)

	var doc map[string]any
	if len(trimmed) == 0 {
		doc = make(map[string]any)
	} else {
		if err := json.Unmarshal(trimmed, &doc); err != nil {
			return nil, indexes.NewValidationError("payload must be a JSON object")
		}
		if doc == nil {
			doc = make(map[string]any)
		}
	}

	normalized := make(map[string]any, len(doc))
	for key, value := range doc {
		normalized[key] = value
	}

	for _, field := range def.FieldMappings {
		value, exists := normalized[field.Name]
		if !exists || isMissingValue(value) {
			if field.Required {
				return nil, indexes.NewValidationError(fmt.Sprintf("field %q is required", field.Name))
			}
			continue
		}

		coerced, err := normalizeFieldValue(field, value)
		if err != nil {
			return nil, err
		}
		normalized[field.Name] = coerced
	}

	payloadBytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("marshal normalized payload: %w", err)
	}
	if len(payloadBytes) == 0 {
		payloadBytes = []byte("{}")
	}
	return json.RawMessage(payloadBytes), nil
}

func isMissingValue(value any) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v) == ""
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	default:
		return false
	}
}

func normalizeFieldValue(field indexes.FieldMapping, value any) (any, error) {
	switch field.Type {
	case indexes.FieldTypeText, indexes.FieldTypeKeyword:
		str, ok := value.(string)
		if !ok {
			return nil, indexes.NewValidationError(fmt.Sprintf("field %q must be a string", field.Name))
		}
		if field.Required && strings.TrimSpace(str) == "" {
			return nil, indexes.NewValidationError(fmt.Sprintf("field %q cannot be empty", field.Name))
		}
		return str, nil

	case indexes.FieldTypeInteger:
		n, err := normalizeIntegerValue(field.Name, value)
		if err != nil {
			return nil, err
		}
		return n, nil

	case indexes.FieldTypeDate:
		str, err := normalizeDateValue(field.Name, value)
		if err != nil {
			return nil, err
		}
		return str, nil

	default:
		return value, nil
	}
}

func normalizeIntegerValue(fieldName string, value any) (int64, error) {
	switch v := value.(type) {
	case float64:
		if math.Trunc(v) != v {
			return 0, indexes.NewValidationError(fmt.Sprintf("field %q must be an integer", fieldName))
		}
		return int64(v), nil
	case string:
		str := strings.TrimSpace(v)
		if str == "" {
			return 0, indexes.NewValidationError(fmt.Sprintf("field %q must be an integer", fieldName))
		}
		parsed, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return 0, indexes.NewValidationError(fmt.Sprintf("field %q must be an integer", fieldName))
		}
		return parsed, nil
	case json.Number:
		parsed, err := v.Int64()
		if err != nil {
			return 0, indexes.NewValidationError(fmt.Sprintf("field %q must be an integer", fieldName))
		}
		return parsed, nil
	default:
		return 0, indexes.NewValidationError(fmt.Sprintf("field %q must be an integer", fieldName))
	}
}

func normalizeDateValue(fieldName string, value any) (string, error) {
	ts, err := parseDateInput(value)
	if err != nil {
		return "", indexes.NewValidationError(fmt.Sprintf("field %q must be a valid date", fieldName))
	}
	return ts.UTC().Format(time.RFC3339Nano), nil
}

func parseDateInput(value any) (time.Time, error) {
	switch v := value.(type) {
	case float64:
		if math.Trunc(v) != v {
			return time.Time{}, fmt.Errorf("invalid numeric date")
		}
		return parseDateFromInteger(int64(v))
	case int64:
		return parseDateFromInteger(v)
	case string:
		str := strings.TrimSpace(v)
		if str == "" {
			return time.Time{}, fmt.Errorf("empty date string")
		}
		return parseDateFromString(str)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return parseDateFromInteger(i)
		}
		return time.Time{}, fmt.Errorf("invalid numeric date")
	default:
		return time.Time{}, fmt.Errorf("unsupported date type")
	}
}

func parseDateFromInteger(value int64) (time.Time, error) {
	switch {
	case value >= 1e15 || value <= -1e15:
		return time.Unix(0, value*int64(time.Microsecond)), nil
	case value >= 1e12 || value <= -1e12:
		return time.UnixMilli(value), nil
	default:
		return time.Unix(value, 0), nil
	}
}

func parseDateFromString(value string) (time.Time, error) {
	if unix, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parseDateFromInteger(unix)
	}

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
		"2006-01",
		"2006/01/02",
		"2006/01",
	}

	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported date format")
}

