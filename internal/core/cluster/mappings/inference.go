package mappings

import (
	"regexp"
	"strings"
	"time"
)

// Common date formats to detect
var dateFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"2006/01/02",
	"01/02/2006",
	"02-01-2006",
}

// ISO 8601 date pattern for quick check
var iso8601Pattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}`)

// InferFieldType determines the appropriate FieldType for a given value.
// This is used for dynamic mapping when a field is not explicitly defined.
func InferFieldType(value any) FieldType {
	if value == nil {
		return FieldTypeKeyword // Default for null values
	}

	switch v := value.(type) {
	case bool:
		return FieldTypeBoolean

	case float64:
		// JSON numbers are always float64
		// Check if it's actually an integer
		if v == float64(int64(v)) {
			return FieldTypeLong
		}
		return FieldTypeFloat

	case int, int32, int64:
		return FieldTypeLong

	case float32:
		return FieldTypeFloat

	case string:
		return inferStringType(v)

	case map[string]any:
		return FieldTypeObject

	case []any:
		// For arrays, infer type from first element
		if len(v) > 0 {
			return InferFieldType(v[0])
		}
		return FieldTypeKeyword

	default:
		return FieldTypeText
	}
}

// inferStringType determines the type for a string value.
func inferStringType(s string) FieldType {
	s = strings.TrimSpace(s)

	if s == "" {
		return FieldTypeKeyword
	}

	// Check for boolean strings
	lower := strings.ToLower(s)
	if lower == "true" || lower == "false" {
		return FieldTypeBoolean
	}

	// Quick check for date-like patterns
	if iso8601Pattern.MatchString(s) {
		if isDateString(s) {
			return FieldTypeDate
		}
	}

	// Use heuristics based on string length and content
	// Short strings (like IDs, codes, tags) → keyword
	// Long strings (descriptions, content) → text
	if len(s) < 256 && !containsWhitespace(s) {
		return FieldTypeKeyword
	}

	// Strings with multiple words → text (full-text searchable)
	if strings.Count(s, " ") >= 2 {
		return FieldTypeText
	}

	return FieldTypeKeyword
}

// isDateString checks if a string can be parsed as a date.
func isDateString(s string) bool {
	for _, format := range dateFormats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}
	return false
}

// containsWhitespace checks if a string contains spaces or tabs.
func containsWhitespace(s string) bool {
	return strings.ContainsAny(s, " \t\n\r")
}

// InferFields analyzes a document and returns inferred field definitions.
// It recursively processes nested objects.
func InferFields(doc map[string]any, prefix string) []InferredField {
	var fields []InferredField

	for name, value := range doc {
		fullName := name
		if prefix != "" {
			fullName = prefix + "." + name
		}

		fieldType := InferFieldType(value)

		// For objects, recurse into nested fields
		if fieldType == FieldTypeObject {
			if nested, ok := value.(map[string]any); ok {
				nestedFields := InferFields(nested, fullName)
				fields = append(fields, nestedFields...)
			}
			continue
		}

		fields = append(fields, InferredField{
			Name:    fullName,
			Type:    fieldType,
			Sample:  value,
			Indexed: true,
		})
	}

	return fields
}

// DefaultFieldConfig returns default configuration for a field based on its type.
func DefaultFieldConfig(fieldType FieldType) (analyzer, tokenizer string, stored bool) {
	switch fieldType {
	case FieldTypeText:
		return "standard", "standard", false
	case FieldTypeKeyword:
		return "", "", false
	case FieldTypeDate:
		return "", "", false
	case FieldTypeLong, FieldTypeInteger, FieldTypeFloat:
		return "", "", false
	case FieldTypeBoolean:
		return "", "", false
	default:
		return "standard", "standard", false
	}
}

// ToField converts an InferredField to a Field with default configuration.
func (inf InferredField) ToField() Field {
	analyzer, tokenizer, stored := DefaultFieldConfig(inf.Type)
	return Field{
		Name:      inf.Name,
		Type:      inf.Type,
		Analyzer:  analyzer,
		Tokenizer: tokenizer,
		Stored:    stored,
		Required:  false,
		Indexed:   inf.Indexed,
	}
}

