package mappings_test

import (
	"testing"

	"plastic-engine-core/internal/core/cluster/mappings"
)

func TestInferFieldType(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected mappings.FieldType
	}{
		// Booleans
		{"bool true", true, mappings.FieldTypeBoolean},
		{"bool false", false, mappings.FieldTypeBoolean},
		{"string true", "true", mappings.FieldTypeBoolean},
		{"string false", "FALSE", mappings.FieldTypeBoolean},

		// Numbers
		{"integer", float64(42), mappings.FieldTypeLong},
		{"float", 3.14, mappings.FieldTypeFloat},
		{"zero", float64(0), mappings.FieldTypeLong},
		{"negative int", float64(-100), mappings.FieldTypeLong},
		{"negative float", -3.14, mappings.FieldTypeFloat},

		// Dates
		{"iso date", "2024-01-15", mappings.FieldTypeDate},
		{"iso datetime", "2024-01-15T10:30:00Z", mappings.FieldTypeDate},
		{"iso datetime nano", "2024-01-15T10:30:00.123456789Z", mappings.FieldTypeDate},
		{"datetime space", "2024-01-15 10:30:00", mappings.FieldTypeDate},

		// Strings
		{"short string", "hello", mappings.FieldTypeKeyword},
		{"id string", "abc-123-xyz", mappings.FieldTypeKeyword},
		{"empty string", "", mappings.FieldTypeKeyword},
		{"long text", "This is a longer piece of text that contains multiple words and should be classified as text for full-text search", mappings.FieldTypeText},
		{"sentence", "hello world foo", mappings.FieldTypeText},

		// Objects
		{"object", map[string]any{"key": "value"}, mappings.FieldTypeObject},
		{"nested object", map[string]any{"a": map[string]any{"b": 1}}, mappings.FieldTypeObject},

		// Arrays
		{"string array", []any{"a", "b"}, mappings.FieldTypeKeyword},
		{"int array", []any{float64(1), float64(2)}, mappings.FieldTypeLong},
		{"empty array", []any{}, mappings.FieldTypeKeyword},

		// Null
		{"null", nil, mappings.FieldTypeKeyword},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mappings.InferFieldType(tt.value)
			if got != tt.expected {
				t.Errorf("InferFieldType(%v) = %s, want %s", tt.value, got, tt.expected)
			}
		})
	}
}

func TestInferFields(t *testing.T) {
	doc := map[string]any{
		"title":     "Hello World",
		"count":     float64(42),
		"price":     19.99,
		"active":    true,
		"created":   "2024-01-15",
		"tags":      []any{"go", "search"},
		"metadata":  map[string]any{"source": "api", "version": float64(1)},
	}

	fields := mappings.InferFields(doc, "")

	// Build a map for easier assertions
	fieldMap := make(map[string]mappings.InferredField)
	for _, f := range fields {
		fieldMap[f.Name] = f
	}

	// Check each expected field
	expected := map[string]mappings.FieldType{
		"title":            mappings.FieldTypeKeyword, // "Hello World" has one space, short
		"count":            mappings.FieldTypeLong,
		"price":            mappings.FieldTypeFloat,
		"active":           mappings.FieldTypeBoolean,
		"created":          mappings.FieldTypeDate,
		"tags":             mappings.FieldTypeKeyword,
		"metadata.source":  mappings.FieldTypeKeyword,
		"metadata.version": mappings.FieldTypeLong,
	}

	for name, expectedType := range expected {
		f, ok := fieldMap[name]
		if !ok {
			t.Errorf("Expected field %q not found", name)
			continue
		}
		if f.Type != expectedType {
			t.Errorf("Field %q: got type %s, want %s", name, f.Type, expectedType)
		}
	}

	// Ensure nested object itself is NOT in the list (only its fields)
	if _, ok := fieldMap["metadata"]; ok {
		t.Error("Object field 'metadata' should not be in the list, only its nested fields")
	}
}

func TestInferredFieldToField(t *testing.T) {
	inf := mappings.InferredField{
		Name:    "title",
		Type:    mappings.FieldTypeText,
		Sample:  "Hello World",
		Indexed: true,
	}

	f := inf.ToField()

	if f.Name != "title" {
		t.Errorf("Name = %q, want %q", f.Name, "title")
	}
	if f.Type != mappings.FieldTypeText {
		t.Errorf("Type = %s, want %s", f.Type, mappings.FieldTypeText)
	}
	if f.Analyzer != "standard" {
		t.Errorf("Analyzer = %q, want %q", f.Analyzer, "standard")
	}
	if f.Tokenizer != "standard" {
		t.Errorf("Tokenizer = %q, want %q", f.Tokenizer, "standard")
	}
	if f.Required {
		t.Error("Required should be false by default")
	}
	if !f.Indexed {
		t.Error("Indexed should be true")
	}
}

func TestDefaultFieldConfig(t *testing.T) {
	tests := []struct {
		fieldType       mappings.FieldType
		expectAnalyzer  string
		expectTokenizer string
	}{
		{mappings.FieldTypeText, "standard", "standard"},
		{mappings.FieldTypeKeyword, "", ""},
		{mappings.FieldTypeDate, "", ""},
		{mappings.FieldTypeLong, "", ""},
		{mappings.FieldTypeFloat, "", ""},
		{mappings.FieldTypeBoolean, "", ""},
	}

	for _, tt := range tests {
		t.Run(string(tt.fieldType), func(t *testing.T) {
			analyzer, tokenizer, _ := mappings.DefaultFieldConfig(tt.fieldType)
			if analyzer != tt.expectAnalyzer {
				t.Errorf("Analyzer = %q, want %q", analyzer, tt.expectAnalyzer)
			}
			if tokenizer != tt.expectTokenizer {
				t.Errorf("Tokenizer = %q, want %q", tokenizer, tt.expectTokenizer)
			}
		})
	}
}

