package document

import (
	indexes "plastic-engine-core/internal/core/cluster/indexes"
)

// FieldPlan describes how a specific field should be processed for indexing.
type FieldPlan struct {
	Field indexes.FieldMapping

	Tokenizer Tokenizer
	Analyzer  Analyzer
}

// FieldPlanner decides which fields are indexable and prepares processors.
type FieldPlanner struct {
	tokenizers TokenizerFactory
	analyzers  AnalyzerFactory
}

// NewFieldPlanner builds a planner.
func NewFieldPlanner(tokenizers TokenizerFactory, analyzers AnalyzerFactory) *FieldPlanner {
	return &FieldPlanner{
		tokenizers: tokenizers,
		analyzers:  analyzers,
	}
}

// BuildPlans returns FieldPlans for mappings that should be indexed.
func (p *FieldPlanner) BuildPlans(def indexes.IndexDefinition) ([]FieldPlan, error) {
	plans := make([]FieldPlan, 0, len(def.FieldMappings))

	for _, mapping := range def.FieldMappings {
		if !mapping.Indexed {
			continue
		}

		switch mapping.Type {
		case indexes.FieldTypeText:
			tokenizer, err := p.tokenizers.NewTokenizer(mapping.Tokenizer)
			if err != nil {
				return nil, err
			}
			analyzer, err := p.analyzers.NewAnalyzer(mapping.Analyzer)
			if err != nil {
				return nil, err
			}

			plans = append(plans, FieldPlan{
				Field:     mapping,
				Tokenizer: tokenizer,
				Analyzer:  analyzer,
			})

		case indexes.FieldTypeKeyword, indexes.FieldTypeInteger:
			plans = append(plans, FieldPlan{
				Field: mapping,
			})

		default:
			// Unsupported types are skipped silently for now.
			continue
		}
	}

	return plans, nil
}
