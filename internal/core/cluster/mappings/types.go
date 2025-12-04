// Package mappings handles field mapping definitions and dynamic type inference.
// Mappings define how document fields are indexed, stored, and analyzed.
package mappings

import "time"

// FieldType describes the semantics of a field stored in the index.
type FieldType string

const (
	// FieldTypeText stores analyzed full-text content.
	FieldTypeText FieldType = "text"
	// FieldTypeKeyword stores exact-match strings (not analyzed).
	FieldTypeKeyword FieldType = "keyword"
	// FieldTypeLong stores 64-bit integer values.
	FieldTypeLong FieldType = "long"
	// FieldTypeInteger stores 32-bit integer values.
	FieldTypeInteger FieldType = "integer"
	// FieldTypeFloat stores floating-point values.
	FieldTypeFloat FieldType = "float"
	// FieldTypeBoolean stores true/false values.
	FieldTypeBoolean FieldType = "boolean"
	// FieldTypeDate stores temporal values.
	FieldTypeDate FieldType = "date"
	// FieldTypeObject stores nested JSON objects.
	FieldTypeObject FieldType = "object"
)

// DynamicMode controls how unmapped fields are handled during indexing.
type DynamicMode string

const (
	// DynamicTrue infers and adds new fields automatically (default).
	DynamicTrue DynamicMode = "true"
	// DynamicFalse ignores unmapped fields (stored but not indexed).
	DynamicFalse DynamicMode = "false"
	// DynamicStrict rejects documents with unmapped fields.
	DynamicStrict DynamicMode = "strict"
)

// Field defines how a single field is indexed and stored.
type Field struct {
	Name      string    `json:"name"`
	Type      FieldType `json:"type"`
	Analyzer  string    `json:"analyzer,omitempty"`
	Tokenizer string    `json:"tokenizer,omitempty"`
	Stored    bool      `json:"stored"`
	Required  bool      `json:"required"`
	Indexed   bool      `json:"indexed"`
}

// Mapping represents the complete field configuration for an index.
type Mapping struct {
	IndexID   string      `json:"index_id"`
	Version   int         `json:"version"`
	Dynamic   DynamicMode `json:"dynamic"`
	Fields    []Field     `json:"fields"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// AddFieldRequest encapsulates a request to add a new field to an existing mapping.
type AddFieldRequest struct {
	IndexID string
	Field   Field
}

// UpdateMappingRequest encapsulates a request to update mapping settings.
type UpdateMappingRequest struct {
	IndexID string
	Dynamic *DynamicMode // nil = don't change
	Fields  []Field      // Fields to add (cannot modify existing)
}

// InferredField represents a field that was automatically detected.
type InferredField struct {
	Name    string
	Type    FieldType
	Sample  any // The value that was used for inference
	Indexed bool
}

