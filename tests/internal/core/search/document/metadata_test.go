package document_test

import (
	"context"
	"errors"
	"testing"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/document"
	shards "plastic-engine-core/internal/core/search/shards"
)

func TestMetadataResolverUsesAssignmentFields(t *testing.T) {
	t.Parallel()

	resolver := document.NewMetadataResolver(nil)
	assignment := shards.Assignment{
		IndexID:        "idx",
		ShardStrategy:  indexes.ShardStrategyComputed,
		Analyzer:       "simple",
		Tokenizer:      "whitespace",
		MappingVersion: 2,
		Fields: []indexes.FieldMapping{
			{Name: "title", Type: indexes.FieldTypeText, Indexed: true},
		},
	}

	def, err := resolver.Resolve(context.Background(), assignment)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if def.MappingVersion != 2 {
		t.Fatalf("MappingVersion = %d, want 2", def.MappingVersion)
	}
	if len(def.FieldMappings) != 1 || def.FieldMappings[0].Name != "title" {
		t.Fatalf("FieldMappings = %+v, want title", def.FieldMappings)
	}
}

func TestMetadataResolverFetchesWhenFieldsMissing(t *testing.T) {
	t.Parallel()

	fetcher := &stubDefinitionFetcher{
		definition: indexes.IndexDefinition{
			ID:               "idx",
			ShardStrategy:    indexes.ShardStrategyAutomatic,
			DefaultAnalyzer:  "simple",
			DefaultTokenizer: "whitespace",
			MappingVersion:   3,
			FieldMappings: []indexes.FieldMapping{
				{Name: "value", Type: indexes.FieldTypeInteger, Indexed: true},
			},
		},
	}

	resolver := document.NewMetadataResolver(fetcher)
	assignment := shards.Assignment{
		IndexID:        "idx",
		MappingVersion: 3,
	}

	def, err := resolver.Resolve(context.Background(), assignment)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if len(def.FieldMappings) != 1 || def.FieldMappings[0].Name != "value" {
		t.Fatalf("FieldMappings = %+v, want value", def.FieldMappings)
	}
	if fetcher.calls != 1 {
		t.Fatalf("fetcher calls = %d, want 1", fetcher.calls)
	}
}

func TestMetadataResolverVersionMismatch(t *testing.T) {
	t.Parallel()

	fetcher := &stubDefinitionFetcher{
		definition: indexes.IndexDefinition{
			ID:               "idx",
			MappingVersion:   1,
			DefaultAnalyzer:  "simple",
			DefaultTokenizer: "whitespace",
		},
	}

	resolver := document.NewMetadataResolver(fetcher)

	_, err := resolver.Resolve(context.Background(), shards.Assignment{
		IndexID:        "idx",
		MappingVersion: 2,
	})
	if err == nil {
		t.Fatalf("expected error on version mismatch")
	}
}

type stubDefinitionFetcher struct {
	definition indexes.IndexDefinition
	err        error
	calls      int
}

func (s *stubDefinitionFetcher) FetchIndexDefinition(_ context.Context, indexID string) (indexes.IndexDefinition, error) {
	s.calls++
	if s.err != nil {
		return indexes.IndexDefinition{}, s.err
	}
	if indexID != s.definition.ID {
		return indexes.IndexDefinition{}, errors.New("unknown index")
	}
	return s.definition, nil
}
