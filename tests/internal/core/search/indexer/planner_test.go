package indexer_test

import (
	"testing"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/search/indexer"
)

func TestFieldPlannerBuildsPlans(t *testing.T) {
	t.Parallel()

	fields := []coreindex.FieldMapping{
		{Name: "title", Type: coreindex.FieldTypeText, Analyzer: "simple", Tokenizer: "whitespace", Indexed: true},
		{Name: "order_id", Type: coreindex.FieldTypeKeyword, Indexed: true},
		{Name: "ignored", Type: coreindex.FieldTypeText, Indexed: false},
	}

	planner := indexer.NewFieldPlanner(indexer.TokenizerFactory{}, indexer.AnalyzerFactory{})
	plans, err := planner.BuildPlans(coreindex.IndexDefinition{FieldMappings: fields})
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

	fields := []coreindex.FieldMapping{
		{Name: "title", Type: coreindex.FieldTypeText, Tokenizer: "unknown", Indexed: true},
	}

	planner := indexer.NewFieldPlanner(indexer.TokenizerFactory{}, indexer.AnalyzerFactory{})
	if _, err := planner.BuildPlans(coreindex.IndexDefinition{FieldMappings: fields}); err == nil {
		t.Fatalf("expected error for unknown tokenizer")
	}
}
