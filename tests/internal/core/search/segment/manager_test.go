package segment_test

import (
	"context"
	"fmt"
	"testing"

	"plastic-engine-core/internal/core/search/segment"
)

func TestManagerBasicIndexAndSearch(t *testing.T) {
	dir := t.TempDir()

	config := segment.Config{
		DataDir: dir,
	}

	mgr, err := segment.NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Index some documents
	err = mgr.Index(ctx, "title", "hello", "doc1", 2, []int{0, 5})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	err = mgr.Index(ctx, "title", "world", "doc1", 1, []int{1})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	err = mgr.Index(ctx, "title", "hello", "doc2", 1, []int{0})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Search
	hits, err := mgr.Search(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(hits) != 2 {
		t.Errorf("expected 2 hits, got %d", len(hits))
	}

	// Verify hits contain expected docs
	docIDs := make(map[string]bool)
	for _, hit := range hits {
		docIDs[hit.DocID] = true
	}

	if !docIDs["doc1"] {
		t.Error("expected doc1 in results")
	}
	if !docIDs["doc2"] {
		t.Error("expected doc2 in results")
	}
}

func TestManagerBatchIndex(t *testing.T) {
	dir := t.TempDir()

	mgr, err := segment.NewManager(segment.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	docs := []segment.DocumentBatch{
		{
			DocID: "doc1",
			FieldTerms: map[string][]segment.TermPosting{
				"title": {
					{Term: "hello", TF: 2, Positions: []int{1, 5}},
					{Term: "world", TF: 1, Positions: []int{2}},
				},
			},
		},
		{
			DocID: "doc2",
			FieldTerms: map[string][]segment.TermPosting{
				"title": {
					{Term: "hello", TF: 1, Positions: []int{1}},
					{Term: "foo", TF: 1, Positions: []int{2}},
				},
			},
		},
	}

	if err := mgr.IndexDocumentBatch(ctx, docs); err != nil {
		t.Fatalf("IndexDocumentBatch: %v", err)
	}

	// Search
	hits, err := mgr.Search(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 {
		t.Errorf("expected 2 hits for hello, got %d", len(hits))
	}

	// Verify DF
	df := mgr.GetDF("title\x00hello")
	if df != 2 {
		t.Errorf("expected DF=2 for hello, got %d", df)
	}

	// Verify total docs
	total := mgr.GetTotalDocs()
	if total != 2 {
		t.Errorf("expected 2 total docs, got %d", total)
	}

	// Verify positions using SearchWithPositions (Search returns TF-only)
	posHits, err := mgr.SearchWithPositions(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("SearchWithPositions: %v", err)
	}
	for _, hit := range posHits {
		if hit.DocID == "doc1" {
			if hit.TF != 2 {
				t.Errorf("expected TF=2 for doc1, got %d", hit.TF)
			}
			if len(hit.Positions) != 2 {
				t.Errorf("expected 2 positions for doc1, got %d", len(hit.Positions))
			}
		}
	}
}

func TestManagerSearchByTermID(t *testing.T) {
	dir := t.TempDir()

	mgr, err := segment.NewManager(segment.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	err = mgr.Index(ctx, "title", "car", "doc1", 1, nil)
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	termID := "title\x00car"

	hits, err := mgr.SearchByTermID(termID)
	if err != nil {
		t.Fatalf("SearchByTermID: %v", err)
	}
	if len(hits) != 1 {
		t.Errorf("expected 1 hit, got %d", len(hits))
	}
	if len(hits) > 0 && hits[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", hits[0].DocID)
	}

	df := mgr.GetDF(termID)
	if df != 1 {
		t.Errorf("expected DF=1, got %d", df)
	}
}

func TestManagerGetTermsWithPrefix(t *testing.T) {
	dir := t.TempDir()

	mgr, err := segment.NewManager(segment.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	mgr.Index(ctx, "title", "apple", "doc1", 1, []int{1})
	mgr.Index(ctx, "title", "application", "doc2", 1, []int{1})
	mgr.Index(ctx, "title", "banana", "doc3", 1, []int{1})

	terms := mgr.GetTermsWithPrefix(ctx, "title", "app")
	if len(terms) != 2 {
		t.Errorf("expected 2 terms with prefix 'app', got %d: %v", len(terms), terms)
	}
}

func TestManagerPersistence(t *testing.T) {
	dir := t.TempDir()

	// Index and close
	mgr, err := segment.NewManager(segment.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx := context.Background()
	mgr.Index(ctx, "title", "hello", "doc1", 1, []int{1})
	mgr.Close()

	// Reopen and verify
	mgr2, err := segment.NewManager(segment.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	defer mgr2.Close()

	hits, err := mgr2.Search(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("Search after reopen: %v", err)
	}
	if len(hits) != 1 {
		t.Errorf("expected 1 hit after reopen, got %d", len(hits))
	}

	total := mgr2.GetTotalDocs()
	if total != 1 {
		t.Errorf("expected 1 total doc after reopen, got %d", total)
	}
}

func TestManagerBulkIngest(t *testing.T) {
	dir := t.TempDir()

	mgr, err := segment.NewManager(segment.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Index 1000 docs in batches
	batchSize := 100
	for batch := 0; batch < 10; batch++ {
		docs := make([]segment.DocumentBatch, batchSize)
		for i := 0; i < batchSize; i++ {
			docID := fmt.Sprintf("doc_%d_%d", batch, i)
			docs[i] = segment.DocumentBatch{
				DocID: docID,
				FieldTerms: map[string][]segment.TermPosting{
					"title": {
						{Term: "common", TF: 1, Positions: []int{1}},
						{Term: fmt.Sprintf("unique_%d", batch*batchSize+i), TF: 1, Positions: []int{2}},
					},
				},
			}
		}
		if err := mgr.IndexDocumentBatch(ctx, docs); err != nil {
			t.Fatalf("batch %d: %v", batch, err)
		}
	}

	// Verify total
	total := mgr.GetTotalDocs()
	if total != 1000 {
		t.Errorf("expected 1000 total docs, got %d", total)
	}

	// Search common term
	hits, err := mgr.Search(ctx, "title", "common")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1000 {
		t.Errorf("expected 1000 hits for 'common', got %d", len(hits))
	}

	// Search unique term
	hits, err = mgr.Search(ctx, "title", "unique_42")
	if err != nil {
		t.Fatalf("Search unique: %v", err)
	}
	if len(hits) != 1 {
		t.Errorf("expected 1 hit for 'unique_42', got %d", len(hits))
	}
}

func TestBM25Scorer(t *testing.T) {
	scorer := segment.NewBM25Scorer()

	ctx := segment.ScoringContext{
		TotalDocs: 1000,
		AvgDocLen: 100,
		TermDF: map[string]int64{
			"term1": 10,
			"term2": 500,
		},
		DocLens: map[string]int{
			"doc1": 100,
			"doc2": 50,
		},
	}

	scoreRare := scorer.Score(ctx, "term1", 1, 100)
	scoreCommon := scorer.Score(ctx, "term2", 1, 100)

	if scoreRare <= scoreCommon {
		t.Errorf("rare term should score higher: rare=%f, common=%f", scoreRare, scoreCommon)
	}

	scoreTF1 := scorer.Score(ctx, "term1", 1, 100)
	scoreTF3 := scorer.Score(ctx, "term1", 3, 100)

	if scoreTF3 <= scoreTF1 {
		t.Errorf("higher TF should score higher: TF1=%f, TF3=%f", scoreTF1, scoreTF3)
	}

	scoreShort := scorer.Score(ctx, "term1", 1, 50)
	scoreLong := scorer.Score(ctx, "term1", 1, 200)

	if scoreShort <= scoreLong {
		t.Errorf("shorter doc should score higher: short=%f, long=%f", scoreShort, scoreLong)
	}
}

func TestBloomFilter(t *testing.T) {
	bloom := segment.NewBloomFilter(segment.BloomFilterConfig{
		ExpectedItems:     1000,
		FalsePositiveRate: 0.01,
	})

	items := []string{"term1", "term2", "term3", "hello", "world"}
	for _, item := range items {
		bloom.Add(item)
	}

	for _, item := range items {
		if !bloom.MayContain(item) {
			t.Errorf("bloom filter should contain %s", item)
		}
	}

	notFound := 0
	testItems := []string{"notexist1", "notexist2", "notexist3", "foo", "bar", "baz"}
	for _, item := range testItems {
		if !bloom.MayContain(item) {
			notFound++
		}
	}

	if notFound == 0 {
		t.Error("bloom filter has too many false positives")
	}
}

func TestPostingEncoding(t *testing.T) {
	// Test binary encoding roundtrip via the posting store
	dir := t.TempDir()

	mgr, err := segment.NewManager(segment.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Index with specific positions
	err = mgr.Index(ctx, "body", "test", "doc1", 3, []int{1, 5, 10})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Use SearchWithPositions to verify position roundtrip (Search returns TF-only)
	hits, err := mgr.SearchWithPositions(ctx, "body", "test")
	if err != nil {
		t.Fatalf("SearchWithPositions: %v", err)
	}

	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}

	hit := hits[0]
	if hit.TF != 3 {
		t.Errorf("expected TF=3, got %d", hit.TF)
	}
	if len(hit.Positions) != 3 {
		t.Errorf("expected 3 positions, got %d", len(hit.Positions))
	}
	if hit.Positions[0] != 1 || hit.Positions[1] != 5 || hit.Positions[2] != 10 {
		t.Errorf("unexpected positions: %v", hit.Positions)
	}
}
