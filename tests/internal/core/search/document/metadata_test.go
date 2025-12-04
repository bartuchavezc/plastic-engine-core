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

func TestMetadataResolverVersionMismatch_FetchesLatest(t *testing.T) {
	t.Parallel()

	// When there's a version mismatch, the resolver should fetch the latest
	// from the coordinator and return it (no error).
	fetcher := &stubDefinitionFetcher{
		definition: indexes.IndexDefinition{
			ID:               "idx",
			MappingVersion:   3, // Coordinator has version 3
			DefaultAnalyzer:  "simple",
			DefaultTokenizer: "whitespace",
		},
	}

	resolver := document.NewMetadataResolver(fetcher)

	// Assignment has version 2, but coordinator has version 3
	def, err := resolver.Resolve(context.Background(), shards.Assignment{
		IndexID:        "idx",
		MappingVersion: 2,
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Should return the fetched version (3), not the assignment version (2)
	if def.MappingVersion != 3 {
		t.Errorf("MappingVersion = %d, want 3", def.MappingVersion)
	}
	if fetcher.calls != 1 {
		t.Errorf("fetcher calls = %d, want 1", fetcher.calls)
	}
}

func TestMetadataResolverGetLocalVersion(t *testing.T) {
	t.Parallel()

	fetcher := &stubDefinitionFetcher{
		definition: indexes.IndexDefinition{
			ID:               "idx",
			MappingVersion:   5,
			DefaultAnalyzer:  "simple",
			DefaultTokenizer: "whitespace",
		},
	}

	resolver := document.NewMetadataResolver(fetcher)

	// Before any fetch, local version should be 0
	if got := resolver.GetLocalVersion("idx"); got != 0 {
		t.Errorf("GetLocalVersion (before fetch) = %d, want 0", got)
	}

	// Fetch the definition
	_, err := resolver.Resolve(context.Background(), shards.Assignment{
		IndexID:        "idx",
		MappingVersion: 5,
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// After fetch, local version should match
	if got := resolver.GetLocalVersion("idx"); got != 5 {
		t.Errorf("GetLocalVersion (after fetch) = %d, want 5", got)
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
