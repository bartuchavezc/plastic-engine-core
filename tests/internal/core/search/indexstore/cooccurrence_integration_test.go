package indexstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"plastic-engine-core/internal/core/search/indexstore"
)

// TestCooccurrenceToTermMatrix indexes documents and verifies that
// co-occurrence pairs are drained into the term co-occurrence matrix as PMI edges.
// The term matrix is separate from the knowledge graph.
func TestCooccurrenceToTermMatrix(t *testing.T) {
	dir := t.TempDir()

	// Create manager with co-occurrence enabled and short drain interval
	mgr, err := indexstore.NewManager(indexstore.Config{
		DataDir: dir,
		CooccurrenceConfig: indexstore.CooccurrenceConfig{
			WindowSize:     10,
			MaxDFThreshold: 100000,
			DrainInterval:  500 * time.Millisecond, // fast hot drain for test
			EpochInterval:  1 * time.Second,         // fast cold epoch for test
			MinPairCount:   2,                        // need at least 2 co-occurrences
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	// The term matrix is created automatically by the Manager
	termMatrix := mgr.TermMatrix()
	if termMatrix == nil {
		t.Fatal("TermMatrix() returned nil")
	}

	ctx := context.Background()

	// Index 50 documents where "plastic" and "engine" always co-occur,
	// and "random" appears alone in different docs.
	docs := make([]indexstore.DocumentBatch, 0, 60)

	for i := 0; i < 50; i++ {
		docs = append(docs, indexstore.DocumentBatch{
			DocID: fmt.Sprintf("cooc_%d", i),
			FieldTerms: map[string][]indexstore.TermPosting{
				"title": {
					{Term: "plastic", TF: 1, Positions: []int{1}},
					{Term: "engine", TF: 1, Positions: []int{2}},
					{Term: "search", TF: 1, Positions: []int{3}},
				},
			},
		})
	}

	// 10 documents with only "random" + "noise" (no co-occurrence with plastic/engine)
	for i := 0; i < 10; i++ {
		docs = append(docs, indexstore.DocumentBatch{
			DocID: fmt.Sprintf("random_%d", i),
			FieldTerms: map[string][]indexstore.TermPosting{
				"title": {
					{Term: "random", TF: 1, Positions: []int{1}},
					{Term: "noise", TF: 1, Positions: []int{5}},
				},
			},
		})
	}

	if err := mgr.IndexDocumentBatch(ctx, docs); err != nil {
		t.Fatalf("IndexDocumentBatch: %v", err)
	}

	// Wait for co-occurrence drain to fire (500ms interval + margin)
	time.Sleep(2 * time.Second)

	// === Verify posting store DF (authoritative) ===
	if df := mgr.GetDF("title\x00plastic"); df != 50 {
		t.Errorf("posting store DF(plastic)=%d, want 50", df)
	}
	if df := mgr.GetDF("title\x00engine"); df != 50 {
		t.Errorf("posting store DF(engine)=%d, want 50", df)
	}
	if df := mgr.GetDF("title\x00random"); df != 10 {
		t.Errorf("posting store DF(random)=%d, want 10", df)
	}

	// === Verify term co-occurrence matrix ===
	stats := termMatrix.Stats()
	t.Logf("Term matrix stats: terms=%d edges=%d nodes=%d", stats.TermCount, stats.EdgeCount, stats.NodeCount)

	if stats.EdgeCount == 0 {
		t.Fatal("expected edges in term matrix, got 0")
	}

	plasticKey := "title\x00plastic"
	engineKey := "title\x00engine"
	searchKey := "title\x00search"
	randomKey := "title\x00random"

	// Check edges FROM plastic (should reach engine and search)
	plasticEdges := termMatrix.GetEdges(plasticKey, 0)
	t.Logf("Edges from %q:", plasticKey)
	for _, e := range plasticEdges {
		t.Logf("  → %s  weight=%.4f type=%s source=%s", e.To, e.Data.Weight, e.Data.EdgeType, e.Data.Source)
	}

	foundEngine := false
	foundSearch := false
	for _, e := range plasticEdges {
		if e.To == engineKey && e.Data.Source == "pmi" {
			foundEngine = true
			if e.Data.Weight <= 0 {
				t.Errorf("expected positive weight for plastic→engine, got %.4f", e.Data.Weight)
			}
		}
		if e.To == searchKey && e.Data.Source == "pmi" {
			foundSearch = true
		}
	}
	if !foundEngine {
		t.Error("expected co-occurrence edge plastic → engine")
	}
	if !foundSearch {
		t.Error("expected co-occurrence edge plastic → search")
	}

	// Check reverse: edges FROM engine should reach plastic
	engineEdges := termMatrix.GetEdges(engineKey, 0)
	foundReverse := false
	for _, e := range engineEdges {
		if e.To == plasticKey && e.Data.Source == "pmi" {
			foundReverse = true
		}
	}
	if !foundReverse {
		t.Error("expected reverse co-occurrence edge engine → plastic")
	}

	// random↔plastic should NOT have an edge
	randomEdges := termMatrix.GetEdges(randomKey, 0)
	for _, e := range randomEdges {
		if e.To == plasticKey {
			t.Errorf("unexpected edge random → plastic (weight=%.4f)", e.Data.Weight)
		}
	}

	// Test spreading: from plastic, should reach engine and search in 1 hop
	spread := termMatrix.Spread(plasticKey, 1, 0.7)
	t.Logf("Spread from %q (1 hop, decay=0.7):", plasticKey)
	for node, weight := range spread {
		t.Logf("  %s → %.4f", node, weight)
	}

	if _, ok := spread[engineKey]; !ok {
		t.Error("spread from plastic should reach engine")
	}
	if _, ok := spread[searchKey]; !ok {
		t.Error("spread from plastic should reach search")
	}
	if _, ok := spread[randomKey]; ok {
		t.Error("spread from plastic should NOT reach random")
	}

	// === Verify knowledge graph is NOT polluted ===
	// Matrix() should be nil (we never called SetMatrix)
	if mgr.Matrix() != nil {
		t.Error("knowledge graph matrix should be nil (not set)")
	}
}
