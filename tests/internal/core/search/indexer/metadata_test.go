package indexer_test

import (
	"context"
	"errors"
	"testing"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/search/indexer"
	searchshard "plastic-engine-core/internal/core/search/shard"
)

func TestMetadataResolverUsesAssignmentFields(t *testing.T) {
	t.Parallel()

	resolver := indexer.NewMetadataResolver(nil)
	assignment := searchshard.Assignment{
		IndexID:        "idx",
		ShardStrategy:  coreindex.ShardStrategyComputed,
		Analyzer:       "simple",
		Tokenizer:      "whitespace",
		MappingVersion: 2,
		Fields: []coreindex.FieldMapping{
			{Name: "title", Type: coreindex.FieldTypeText, Indexed: true},
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
		definition: coreindex.IndexDefinition{
			ID:               "idx",
			ShardStrategy:    coreindex.ShardStrategyAutomatic,
			DefaultAnalyzer:  "simple",
			DefaultTokenizer: "whitespace",
			MappingVersion:   3,
			FieldMappings: []coreindex.FieldMapping{
				{Name: "value", Type: coreindex.FieldTypeInteger, Indexed: true},
			},
		},
	}

	resolver := indexer.NewMetadataResolver(fetcher)
	assignment := searchshard.Assignment{
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
		definition: coreindex.IndexDefinition{
			ID:               "idx",
			MappingVersion:   1,
			DefaultAnalyzer:  "simple",
			DefaultTokenizer: "whitespace",
		},
	}

	resolver := indexer.NewMetadataResolver(fetcher)

	_, err := resolver.Resolve(context.Background(), searchshard.Assignment{
		IndexID:        "idx",
		MappingVersion: 2,
	})
	if err == nil {
		t.Fatalf("expected error on version mismatch")
	}
}

type stubDefinitionFetcher struct {
	definition coreindex.IndexDefinition
	err        error
	calls      int
}

func (s *stubDefinitionFetcher) FetchIndexDefinition(_ context.Context, indexID string) (coreindex.IndexDefinition, error) {
	s.calls++
	if s.err != nil {
		return coreindex.IndexDefinition{}, s.err
	}
	if indexID != s.definition.ID {
		return coreindex.IndexDefinition{}, errors.New("unknown index")
	}
	return s.definition, nil
}
