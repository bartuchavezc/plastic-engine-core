package document_test

import (
	"testing"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
)

func TestFieldPlannerBuildsPlans(t *testing.T) {
	t.Parallel()

	fields := []indexes.FieldMapping{
		{Name: "title", Type: indexes.FieldTypeText, Analyzer: "simple", Tokenizer: "whitespace", Indexed: true},
		{Name: "order_id", Type: indexes.FieldTypeKeyword, Indexed: true},
		{Name: "ignored", Type: indexes.FieldTypeText, Indexed: false},
	}

	planner := document.NewFieldPlanner(document.TokenizerFactory{}, document.AnalyzerFactory{})
	plans, err := planner.BuildPlans(indexes.IndexDefinition{FieldMappings: fields})
	if err != nil {
		t.Fatalf("BuildPlans: %v", err)
	}

	if len(plans) != 2 {
		t.Fatalf("plans len = %d, want 2", len(plans))
	}

	if plans[0].Field.Name != "title" || plans[0].Tokenizer == nil || plans[0].Analyzer == nil {
		t.Fatalf("unexpected plan[0]: %+v", plans[0])
	}

	if plans[1].Field.Name != "order_id" || plans[1].Tokenizer != nil {
		t.Fatalf("unexpected plan[1]: %+v", plans[1])
	}
}

func TestFieldPlannerErrorsOnUnknownTokenizer(t *testing.T) {
	t.Parallel()

	fields := []indexes.FieldMapping{
		{Name: "title", Type: indexes.FieldTypeText, Tokenizer: "unknown", Indexed: true},
	}

	planner := document.NewFieldPlanner(document.TokenizerFactory{}, document.AnalyzerFactory{})
	if _, err := planner.BuildPlans(indexes.IndexDefinition{FieldMappings: fields}); err == nil {
		t.Fatalf("expected error for unknown tokenizer")
	}
}
