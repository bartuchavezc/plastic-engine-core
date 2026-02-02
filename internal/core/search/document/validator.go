package document

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	gojson "github.com/goccy/go-json"

	"plastic-engine-core/internal/core/cluster/indexes"
)

// Using goccy/go-json for fast JSON parsing (2-3x faster than encoding/json).
// This library has good Go version compatibility unlike bytedance/sonic.
var jsonUnmarshal = gojson.Unmarshal

// ValidationError represents a document validation failure.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation error: field %q: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("validation error: %s", e.Message)
}

// ValidationResult contains the outcome of document validation.
type ValidationResult struct {
	Valid     bool
	Parsed    map[string]any // The parsed and normalized document
	Error     error
}

// Validator validates documents against index mappings using sonic for fast JSON parsing.
type Validator struct{}

// NewValidator creates a new document validator.
func NewValidator() *Validator {
	return &Validator{}
}

// Validate parses raw JSON bytes with sonic and validates against the index definition.
// Returns the parsed document on success for further processing.
func (v *Validator) Validate(rawPayload []byte, indexDef indexes.IndexDefinition) ValidationResult {
	// Handle empty payload
	if len(rawPayload) == 0 || string(rawPayload) == "{}" {
		// Check if any required fields exist
		for _, field := range indexDef.FieldMappings {
			if field.Required {
				return ValidationResult{
					Error: &ValidationError{Field: field.Name, Message: "required field is missing"},
				}
			}
		}
		return ValidationResult{Valid: true, Parsed: make(map[string]any)}
	}

	// Parse JSON payload
	var doc map[string]any
	if err := jsonUnmarshal(rawPayload, &doc); err != nil {
		return ValidationResult{
			Error: &ValidationError{Message: fmt.Sprintf("invalid JSON: %v", err)},
		}
	}

	if doc == nil {
		doc = make(map[string]any)
	}

	// Build field lookup map
	fieldMap := make(map[string]indexes.FieldMapping, len(indexDef.FieldMappings))
	for _, f := range indexDef.FieldMappings {
		fieldMap[f.Name] = f
	}

	// Validate and normalize each field
	normalized := make(map[string]any, len(doc))

	// First, check required fields
	for _, field := range indexDef.FieldMappings {
		if field.Required {
			value, exists := doc[field.Name]
			if !exists || isMissing(value) {
				return ValidationResult{
					Error: &ValidationError{Field: field.Name, Message: "required field is missing"},
				}
			}
		}
	}

	// Validate and normalize each provided field
	for key, value := range doc {
		field, defined := fieldMap[key]
		if !defined {
			// Field not in mapping - pass through (dynamic mapping handled elsewhere)
			normalized[key] = value
			continue
		}

		// Validate and coerce type
		coerced, err := validateAndCoerce(field, value)
		if err != nil {
			return ValidationResult{
				Error: &ValidationError{Field: field.Name, Message: err.Error()},
			}
		}
		normalized[key] = coerced
	}

	return ValidationResult{
		Valid:  true,
		Parsed: normalized,
	}
}

// ValidateStrict is like Validate but rejects documents with unmapped fields.
func (v *Validator) ValidateStrict(rawPayload []byte, indexDef indexes.IndexDefinition) ValidationResult {
	result := v.Validate(rawPayload, indexDef)
	if !result.Valid {
		return result
	}

	// Build field lookup
	fieldMap := make(map[string]bool, len(indexDef.FieldMappings))
	for _, f := range indexDef.FieldMappings {
		fieldMap[f.Name] = true
	}

	// Check for unmapped fields
	for key := range result.Parsed {
		if !fieldMap[key] {
			return ValidationResult{
				Error: &ValidationError{Field: key, Message: "unmapped field not allowed in strict mode"},
			}
		}
	}

	return result
}

func isMissing(value any) bool {
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
	}
	return false
}

func validateAndCoerce(field indexes.FieldMapping, value any) (any, error) {
	switch field.Type {
	case indexes.FieldTypeText, indexes.FieldTypeKeyword:
		return coerceString(value)
	case indexes.FieldTypeInteger:
		return coerceInteger(value)
	case indexes.FieldTypeDate:
		return coerceDate(value)
	default:
		// Unknown type, pass through
		return value, nil
	}
}

func coerceString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	case nil:
		return "", nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func coerceInteger(value any) (int64, error) {
	switch v := value.(type) {
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("must be an integer, got float")
		}
		return int64(v), nil
	case string:
		str := strings.TrimSpace(v)
		if str == "" {
			return 0, fmt.Errorf("must be an integer, got empty string")
		}
		parsed, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("must be an integer, got %q", str)
		}
		return parsed, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("must be an integer, got %T", value)
	}
}

func coerceDate(value any) (string, error) {
	var ts time.Time
	var err error

	switch v := value.(type) {
	case float64:
		ts, err = parseDateFromNumber(int64(v))
	case string:
		ts, err = parseDateFromStr(v)
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("must be a date, got %T", value)
	}

	if err != nil {
		return "", fmt.Errorf("invalid date: %v", err)
	}

	return ts.UTC().Format(time.RFC3339Nano), nil
}

func parseDateFromNumber(value int64) (time.Time, error) {
	switch {
	case value >= 1e15 || value <= -1e15:
		return time.Unix(0, value*int64(time.Microsecond)), nil
	case value >= 1e12 || value <= -1e12:
		return time.UnixMilli(value), nil
	default:
		return time.Unix(value, 0), nil
	}
}

func parseDateFromStr(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty date string")
	}

	// Try parsing as numeric first
	if unix, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parseDateFromNumber(unix)
	}

	// Try common date formats
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
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
