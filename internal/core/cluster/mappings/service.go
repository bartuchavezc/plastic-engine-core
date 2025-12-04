package mappings

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrMappingNotFound indicates the requested mapping does not exist.
	ErrMappingNotFound = errors.New("mapping not found")
	// ErrFieldExists indicates a field with that name already exists.
	ErrFieldExists = errors.New("field already exists")
	// ErrFieldTypeConflict indicates an attempt to change a field's type.
	ErrFieldTypeConflict = errors.New("cannot change field type")
	// ErrStrictModeViolation indicates unmapped fields in strict mode.
	ErrStrictModeViolation = errors.New("unmapped fields not allowed in strict mode")
)

// Repository defines the persistence interface for mappings.
type Repository interface {
	GetMapping(ctx context.Context, indexID string) (Mapping, error)
	SaveMapping(ctx context.Context, mapping Mapping) error
	AddFields(ctx context.Context, indexID string, fields []Field) error
	UpdateDynamic(ctx context.Context, indexID string, mode DynamicMode) error
}

// Service handles mapping operations and dynamic field inference.
type Service struct {
	repo Repository
}

// NewService creates a new mapping service.
func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// Get retrieves the mapping for an index.
func (s *Service) Get(ctx context.Context, indexID string) (Mapping, error) {
	return s.repo.GetMapping(ctx, indexID)
}

// ProcessDocument analyzes a document against the current mapping and returns
// fields that need to be added (dynamic mapping) or an error if validation fails.
//
// Returns:
//   - nil, nil: document conforms to mapping, no changes needed
//   - []Field, nil: new fields inferred (caller should persist them)
//   - nil, error: validation failed (strict mode violation, type conflict, etc.)
func (s *Service) ProcessDocument(ctx context.Context, indexID string, doc map[string]any) ([]Field, error) {
	mapping, err := s.repo.GetMapping(ctx, indexID)
	if err != nil {
		return nil, fmt.Errorf("get mapping: %w", err)
	}

	// Build a map of existing fields for quick lookup
	existingFields := make(map[string]Field, len(mapping.Fields))
	for _, f := range mapping.Fields {
		existingFields[f.Name] = f
	}

	// Infer fields from the document
	inferred := InferFields(doc, "")

	// Check each inferred field against the mapping
	var newFields []Field
	var unmappedNames []string

	for _, inf := range inferred {
		existing, exists := existingFields[inf.Name]
		if exists {
			// Field exists - check for type conflict
			if existing.Type != inf.Type {
				// Allow some type coercions
				if !isTypeCompatible(existing.Type, inf.Type) {
					return nil, fmt.Errorf("%w: field %q is %s, got %s",
						ErrFieldTypeConflict, inf.Name, existing.Type, inf.Type)
				}
			}
			continue
		}

		// Field doesn't exist in mapping
		unmappedNames = append(unmappedNames, inf.Name)
		newFields = append(newFields, inf.ToField())
	}

	// Handle based on dynamic mode
	switch mapping.Dynamic {
	case DynamicStrict:
		if len(unmappedNames) > 0 {
			return nil, fmt.Errorf("%w: %s",
				ErrStrictModeViolation, strings.Join(unmappedNames, ", "))
		}
		return nil, nil

	case DynamicFalse:
		// Ignore unmapped fields, don't add them
		return nil, nil

	case DynamicTrue, "":
		// Default: add new fields automatically
		if len(newFields) > 0 {
			return newFields, nil
		}
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown dynamic mode: %s", mapping.Dynamic)
	}
}

// AddField adds a new field to an existing mapping.
// Returns an error if the field already exists.
func (s *Service) AddField(ctx context.Context, indexID string, field Field) error {
	mapping, err := s.repo.GetMapping(ctx, indexID)
	if err != nil {
		return fmt.Errorf("get mapping: %w", err)
	}

	// Check if field already exists
	for _, f := range mapping.Fields {
		if f.Name == field.Name {
			return ErrFieldExists
		}
	}

	return s.repo.AddFields(ctx, indexID, []Field{field})
}

// AddFields adds multiple new fields to an existing mapping.
func (s *Service) AddFields(ctx context.Context, indexID string, fields []Field) error {
	if len(fields) == 0 {
		return nil
	}

	mapping, err := s.repo.GetMapping(ctx, indexID)
	if err != nil {
		return fmt.Errorf("get mapping: %w", err)
	}

	// Build existing field map
	existing := make(map[string]bool, len(mapping.Fields))
	for _, f := range mapping.Fields {
		existing[f.Name] = true
	}

	// Filter to only new fields
	var newFields []Field
	for _, f := range fields {
		if !existing[f.Name] {
			newFields = append(newFields, f)
		}
	}

	if len(newFields) == 0 {
		return nil
	}

	return s.repo.AddFields(ctx, indexID, newFields)
}

// SetDynamic updates the dynamic mode for a mapping.
func (s *Service) SetDynamic(ctx context.Context, indexID string, mode DynamicMode) error {
	// Validate the mode
	switch mode {
	case DynamicTrue, DynamicFalse, DynamicStrict:
		// OK
	default:
		return fmt.Errorf("invalid dynamic mode: %s", mode)
	}

	return s.repo.UpdateDynamic(ctx, indexID, mode)
}

// Validate checks if a document conforms to the mapping without processing it.
// Returns nil if valid, or an error describing the validation failure.
func (s *Service) Validate(ctx context.Context, indexID string, doc map[string]any) error {
	mapping, err := s.repo.GetMapping(ctx, indexID)
	if err != nil {
		return fmt.Errorf("get mapping: %w", err)
	}

	// Build a map of existing fields
	existingFields := make(map[string]Field, len(mapping.Fields))
	for _, f := range mapping.Fields {
		existingFields[f.Name] = f
	}

	// Check required fields
	for _, field := range mapping.Fields {
		if field.Required {
			if _, exists := doc[field.Name]; !exists {
				return fmt.Errorf("required field %q is missing", field.Name)
			}
		}
	}

	// Check type compatibility for provided fields
	inferred := InferFields(doc, "")
	for _, inf := range inferred {
		existing, exists := existingFields[inf.Name]
		if !exists {
			if mapping.Dynamic == DynamicStrict {
				return fmt.Errorf("%w: %s", ErrStrictModeViolation, inf.Name)
			}
			continue
		}

		if !isTypeCompatible(existing.Type, inf.Type) {
			return fmt.Errorf("field %q: expected %s, got %s",
				inf.Name, existing.Type, inf.Type)
		}
	}

	return nil
}

// isTypeCompatible checks if an inferred type is compatible with an expected type.
func isTypeCompatible(expected, actual FieldType) bool {
	if expected == actual {
		return true
	}

	// Allow numeric coercions
	numericTypes := map[FieldType]bool{
		FieldTypeLong:    true,
		FieldTypeInteger: true,
		FieldTypeFloat:   true,
	}
	if numericTypes[expected] && numericTypes[actual] {
		return true
	}

	// Allow string coercions
	stringTypes := map[FieldType]bool{
		FieldTypeText:    true,
		FieldTypeKeyword: true,
	}
	if stringTypes[expected] && stringTypes[actual] {
		return true
	}

	return false
}

