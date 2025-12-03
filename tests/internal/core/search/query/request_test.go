package query_test

import (
	"encoding/json"
	"testing"

	searchquery "plastic-engine-core/internal/core/search/query"
)

func TestRequestNormalizeAppliesDefaultsAndBoostParsing(t *testing.T) {
	t.Parallel()

	req := searchquery.Request{
		IndexID: " idx-logs ",
		Query: searchquery.Clause{
			Match: &searchquery.MatchQuery{
				Field: "title^100",
				Value: " SQL ",
			},
		},
	}

	if err := req.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}

	if req.IndexID != "idx-logs" {
		t.Fatalf("IndexID normalize mismatch: got %q", req.IndexID)
	}
	if req.Limit != searchquery.DefaultLimit {
		t.Fatalf("Limit = %d, want %d", req.Limit, searchquery.DefaultLimit)
	}
	if req.Query.Match == nil {
		t.Fatalf("expected match clause")
	}
	if req.Query.Match.Field != "title" {
		t.Fatalf("Match field = %q, want %q", req.Query.Match.Field, "title")
	}
	if req.Query.Match.Boost != 100 {
		t.Fatalf("Boost = %f, want 100", req.Query.Match.Boost)
	}
	if req.Query.Match.Value != "SQL" {
		t.Fatalf("Match value = %q, want %q", req.Query.Match.Value, "SQL")
	}
}

func TestRequestValidateRejectsInvalidPayloads(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		payload string
	}{
		{
			name: "missing index id",
			payload: `{
				"query": {"term": {"field": "status", "value": "ready"}}
			}`,
		},
		{
			name: "missing query clause",
			payload: `{
				"index_id": "idx",
				"query": {}
			}`,
		},
		{
			name: "filter with match clause",
			payload: `{
				"index_id": "idx",
				"query": {"term": {"field": "status", "value": "ready"}},
				"filters": [
					{"match": {"field": "title", "value": "test"}}
				]
			}`,
		},
		{
			name: "limit above max",
			payload: `{
				"index_id": "idx",
				"limit": 1000,
				"query": {"term": {"field": "status", "value": "ready"}}
			}`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var req searchquery.Request
			if err := json.Unmarshal([]byte(tc.payload), &req); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}

			if err := req.Normalize(); err == nil {
				if err := req.Validate(); err == nil {
					t.Fatalf("expected validation failure")
				}
			}
		})
	}
}

func TestRequestValidateAcceptsTermAndRangeFilters(t *testing.T) {
	t.Parallel()

	payload := `{
		"index_id": "idx",
		"limit": 10,
		"query": {"match": {"field": "title", "value": "logs"}},
		"filters": [
			{"term": {"field": "status", "value": "ready"}},
			{"range": {"field": "created_at", "gte": "2025-01-01T00:00:00Z"}}
		]
	}`

	var req searchquery.Request
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if err := req.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}
