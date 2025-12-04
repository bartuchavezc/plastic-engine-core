package mappings_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"plastic-engine-core/internal/core/cluster/mappings"
)

// mockRepository implements mappings.Repository for testing.
type mockRepository struct {
	mapping   mappings.Mapping
	getErr    error
	saveErr   error
	addErr    error
	updateErr error
	// Track calls
	addFieldsCalls [][]mappings.Field
}

func (m *mockRepository) GetMapping(ctx context.Context, indexID string) (mappings.Mapping, error) {
	if m.getErr != nil {
		return mappings.Mapping{}, m.getErr
	}
	return m.mapping, nil
}

func (m *mockRepository) SaveMapping(ctx context.Context, mapping mappings.Mapping) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.mapping = mapping
	return nil
}

func (m *mockRepository) AddFields(ctx context.Context, indexID string, fields []mappings.Field) error {
	if m.addErr != nil {
		return m.addErr
	}
	m.addFieldsCalls = append(m.addFieldsCalls, fields)
	m.mapping.Fields = append(m.mapping.Fields, fields...)
	return nil
}

func (m *mockRepository) UpdateDynamic(ctx context.Context, indexID string, mode mappings.DynamicMode) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.mapping.Dynamic = mode
	return nil
}

func TestServiceProcessDocument_DynamicTrue(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicTrue,
			Fields: []mappings.Field{
				{Name: "existing", Type: mappings.FieldTypeKeyword},
			},
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	doc := map[string]any{
		"existing":  "value",
		"new_field": "new_value",
		"count":     float64(42),
	}

	newFields, err := svc.ProcessDocument(ctx, "test-index", doc)
	if err != nil {
		t.Fatalf("ProcessDocument failed: %v", err)
	}

	if len(newFields) != 2 {
		t.Errorf("Expected 2 new fields, got %d", len(newFields))
	}

	// Check that new fields were inferred
	fieldNames := make(map[string]bool)
	for _, f := range newFields {
		fieldNames[f.Name] = true
	}
	if !fieldNames["new_field"] {
		t.Error("Expected new_field to be inferred")
	}
	if !fieldNames["count"] {
		t.Error("Expected count to be inferred")
	}
}

func TestServiceProcessDocument_DynamicFalse(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicFalse,
			Fields: []mappings.Field{
				{Name: "existing", Type: mappings.FieldTypeKeyword},
			},
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	doc := map[string]any{
		"existing":  "value",
		"new_field": "ignored",
	}

	newFields, err := svc.ProcessDocument(ctx, "test-index", doc)
	if err != nil {
		t.Fatalf("ProcessDocument failed: %v", err)
	}

	if len(newFields) != 0 {
		t.Errorf("Expected no new fields (dynamic=false), got %d", len(newFields))
	}
}

func TestServiceProcessDocument_DynamicStrict(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicStrict,
			Fields: []mappings.Field{
				{Name: "existing", Type: mappings.FieldTypeKeyword},
			},
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	doc := map[string]any{
		"existing":  "value",
		"new_field": "should fail",
	}

	_, err := svc.ProcessDocument(ctx, "test-index", doc)
	if err == nil {
		t.Fatal("Expected error in strict mode, got nil")
	}

	if !errors.Is(err, mappings.ErrStrictModeViolation) {
		t.Errorf("Expected ErrStrictModeViolation, got: %v", err)
	}
}

func TestServiceProcessDocument_TypeConflict(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicTrue,
			Fields: []mappings.Field{
				{Name: "count", Type: mappings.FieldTypeBoolean}, // Stored as boolean
			},
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	doc := map[string]any{
		"count": float64(42), // Sent as number - type conflict
	}

	_, err := svc.ProcessDocument(ctx, "test-index", doc)
	if err == nil {
		t.Fatal("Expected type conflict error, got nil")
	}

	if !errors.Is(err, mappings.ErrFieldTypeConflict) {
		t.Errorf("Expected ErrFieldTypeConflict, got: %v", err)
	}
}

func TestServiceProcessDocument_CompatibleTypes(t *testing.T) {
	// Test that long/integer/float are compatible
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicTrue,
			Fields: []mappings.Field{
				{Name: "num", Type: mappings.FieldTypeLong},
			},
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	doc := map[string]any{
		"num": 3.14, // Float value for Long field - should be compatible
	}

	_, err := svc.ProcessDocument(ctx, "test-index", doc)
	if err != nil {
		t.Fatalf("Expected compatible types, got error: %v", err)
	}
}

func TestServiceValidate_RequiredFields(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicTrue,
			Fields: []mappings.Field{
				{Name: "title", Type: mappings.FieldTypeText, Required: true},
				{Name: "optional", Type: mappings.FieldTypeKeyword, Required: false},
			},
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	// Missing required field
	doc := map[string]any{
		"optional": "value",
	}

	err := svc.Validate(ctx, "test-index", doc)
	if err == nil {
		t.Fatal("Expected validation error for missing required field")
	}

	// With required field
	doc["title"] = "Hello"
	err = svc.Validate(ctx, "test-index", doc)
	if err != nil {
		t.Errorf("Expected validation to pass, got: %v", err)
	}
}

func TestServiceAddField(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID:   "test-index",
			Version:   1,
			Dynamic:   mappings.DynamicTrue,
			Fields:    []mappings.Field{},
			CreatedAt: time.Now(),
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	newField := mappings.Field{
		Name:    "new_field",
		Type:    mappings.FieldTypeKeyword,
		Indexed: true,
	}

	err := svc.AddField(ctx, "test-index", newField)
	if err != nil {
		t.Fatalf("AddField failed: %v", err)
	}

	if len(repo.addFieldsCalls) != 1 {
		t.Errorf("Expected 1 AddFields call, got %d", len(repo.addFieldsCalls))
	}
}

func TestServiceAddField_Duplicate(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicTrue,
			Fields: []mappings.Field{
				{Name: "existing", Type: mappings.FieldTypeKeyword},
			},
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	duplicateField := mappings.Field{
		Name: "existing",
		Type: mappings.FieldTypeText,
	}

	err := svc.AddField(ctx, "test-index", duplicateField)
	if !errors.Is(err, mappings.ErrFieldExists) {
		t.Errorf("Expected ErrFieldExists, got: %v", err)
	}
}

func TestServiceSetDynamic(t *testing.T) {
	repo := &mockRepository{
		mapping: mappings.Mapping{
			IndexID: "test-index",
			Version: 1,
			Dynamic: mappings.DynamicTrue,
		},
	}
	svc := mappings.NewService(repo)
	ctx := context.Background()

	err := svc.SetDynamic(ctx, "test-index", mappings.DynamicStrict)
	if err != nil {
		t.Fatalf("SetDynamic failed: %v", err)
	}

	if repo.mapping.Dynamic != mappings.DynamicStrict {
		t.Errorf("Expected dynamic=strict, got %s", repo.mapping.Dynamic)
	}
}

