package pebble_test

import (
	"testing"

	"plastic-engine-core/internal/adapters/storage/pebble"
)

func TestTermRegistryKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		field string
		term  string
		want  string
	}{
		{"title", "hello", "term:title:hello"},
		{"body", "world", "term:body:world"},
		{"category", "electronics", "term:category:electronics"},
	}

	for _, tt := range tests {
		got := pebble.TermRegistryKey(tt.field, tt.term)
		if got != tt.want {
			t.Errorf("TermRegistryKey(%q, %q) = %q, want %q", tt.field, tt.term, got, tt.want)
		}
	}
}

func TestParseTermRegistryKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key       string
		wantField string
		wantTerm  string
		wantOK    bool
	}{
		{"term:title:hello", "title", "hello", true},
		{"term:body:world", "body", "world", true},
		{"term:field:term:with:colons", "field", "term:with:colons", true},
		{"inv:something", "", "", false},
		{"term:noterm", "", "", false},
		{"", "", "", false},
	}

	for _, tt := range tests {
		field, term, ok := pebble.ParseTermRegistryKey(tt.key)
		if ok != tt.wantOK {
			t.Errorf("ParseTermRegistryKey(%q) ok = %v, want %v", tt.key, ok, tt.wantOK)
			continue
		}
		if ok && (field != tt.wantField || term != tt.wantTerm) {
			t.Errorf("ParseTermRegistryKey(%q) = (%q, %q), want (%q, %q)",
				tt.key, field, term, tt.wantField, tt.wantTerm)
		}
	}
}

func TestGenerateTermID(t *testing.T) {
	t.Parallel()

	// Deterministic - same input should produce same output
	id1 := pebble.GenerateTermID("title", "hello")
	id2 := pebble.GenerateTermID("title", "hello")
	if id1 != id2 {
		t.Errorf("GenerateTermID not deterministic: %q != %q", id1, id2)
	}

	// Different inputs should produce different IDs
	id3 := pebble.GenerateTermID("title", "world")
	if id1 == id3 {
		t.Errorf("GenerateTermID collision: %q == %q for different terms", id1, id3)
	}

	// Different fields should produce different IDs
	id4 := pebble.GenerateTermID("body", "hello")
	if id1 == id4 {
		t.Errorf("GenerateTermID collision: %q == %q for different fields", id1, id4)
	}

	// Format check
	if len(id1) < 10 || id1[:2] != "t_" {
		t.Errorf("GenerateTermID format unexpected: %q", id1)
	}
}

func TestInvertedKey(t *testing.T) {
	t.Parallel()

	termID := "t_abc123"
	docID := "doc-1"

	key := pebble.InvertedKey(termID, docID)
	want := "inv:t_abc123:doc-1"
	if key != want {
		t.Errorf("InvertedKey = %q, want %q", key, want)
	}

	prefix := pebble.InvertedPrefix(termID)
	wantPrefix := "inv:t_abc123:"
	if prefix != wantPrefix {
		t.Errorf("InvertedPrefix = %q, want %q", prefix, wantPrefix)
	}
}

func TestParseInvertedKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key        string
		wantTermID string
		wantDocID  string
		wantOK     bool
	}{
		{"inv:t_abc123:doc-1", "t_abc123", "doc-1", true},
		{"inv:t_xyz:doc-with-dashes", "t_xyz", "doc-with-dashes", true},
		{"term:something", "", "", false},
		{"inv:nocolon", "", "", false},
	}

	for _, tt := range tests {
		termID, docID, ok := pebble.ParseInvertedKey(tt.key)
		if ok != tt.wantOK {
			t.Errorf("ParseInvertedKey(%q) ok = %v, want %v", tt.key, ok, tt.wantOK)
			continue
		}
		if ok && (termID != tt.wantTermID || docID != tt.wantDocID) {
			t.Errorf("ParseInvertedKey(%q) = (%q, %q), want (%q, %q)",
				tt.key, termID, docID, tt.wantTermID, tt.wantDocID)
		}
	}
}

func TestForwardKey(t *testing.T) {
	t.Parallel()

	key := pebble.ForwardKey("doc-123")
	want := "fwd:doc-123"
	if key != want {
		t.Errorf("ForwardKey = %q, want %q", key, want)
	}

	docID, ok := pebble.ParseForwardKey(key)
	if !ok || docID != "doc-123" {
		t.Errorf("ParseForwardKey(%q) = (%q, %v), want (%q, true)", key, docID, ok, "doc-123")
	}
}

func TestMetadataKeys(t *testing.T) {
	t.Parallel()

	if got := pebble.DocCountKey(); got != "meta:doc_count" {
		t.Errorf("DocCountKey = %q, want %q", got, "meta:doc_count")
	}

	if got := pebble.AvgDocLenKey("title"); got != "meta:avg_doc_len:title" {
		t.Errorf("AvgDocLenKey(title) = %q, want %q", got, "meta:avg_doc_len:title")
	}

	if got := pebble.FieldDocsKey("body"); got != "meta:field_docs:body" {
		t.Errorf("FieldDocsKey(body) = %q, want %q", got, "meta:field_docs:body")
	}
}

