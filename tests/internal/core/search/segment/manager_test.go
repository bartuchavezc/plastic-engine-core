package segment_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"plastic-engine-core/internal/core/search/segment"
)

func TestManagerBasicIndexAndSearch(t *testing.T) {
	dir := t.TempDir()

	config := segment.Config{
		FlushThreshold:      100,
		MaxSegmentsPerLevel: 5,
		LevelSizeMultiplier: 10,
		DataDir:             dir,
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

func TestManagerFlush(t *testing.T) {
	dir := t.TempDir()

	config := segment.Config{
		FlushThreshold:      10, // Low threshold for testing
		MaxSegmentsPerLevel: 5,
		LevelSizeMultiplier: 10,
		DataDir:             dir,
	}

	mgr, err := segment.NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Index enough documents to trigger flush
	for i := 0; i < 15; i++ {
		docID := "doc" + string(rune('a'+i))
		err = mgr.Index(ctx, "title", "term", docID, 1, nil)
		if err != nil {
			t.Fatalf("Index: %v", err)
		}
	}

	// Force flush
	mgr.Flush()

	// Check that segment files were created
	segmentDir := filepath.Join(dir, "segments")
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	hasSegment := false
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".seg" {
			hasSegment = true
			break
		}
	}

	if !hasSegment {
		t.Error("expected at least one segment file after flush")
	}

	// Search should still work
	hits, err := mgr.Search(ctx, "title", "term")
	if err != nil {
		t.Fatalf("Search after flush: %v", err)
	}

	if len(hits) < 10 {
		t.Errorf("expected at least 10 hits after flush, got %d", len(hits))
	}
}

func TestManagerAlias(t *testing.T) {
	dir := t.TempDir()

	config := segment.Config{
		FlushThreshold:      100,
		MaxSegmentsPerLevel: 5,
		LevelSizeMultiplier: 10,
		DataDir:             dir,
	}

	mgr, err := segment.NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Index with original term
	err = mgr.Index(ctx, "title", "car", "doc1", 1, nil)
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Search with original term
	hits, err := mgr.Search(ctx, "title", "car")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 {
		t.Errorf("expected 1 hit for 'car', got %d", len(hits))
	}

	// Get the term ID for "car"
	termID, found := mgr.Registry().Get(ctx, "title", "car")
	if !found {
		t.Fatal("term 'car' not found in registry")
	}

	// Create alias "automobile" -> same term_id as "car"
	err = mgr.CreateAlias("title", "automobile", termID)
	if err != nil {
		t.Fatalf("CreateAlias: %v", err)
	}

	// Search with alias - should find same documents!
	hits, err = mgr.Search(ctx, "title", "automobile")
	if err != nil {
		t.Fatalf("Search alias: %v", err)
	}
	if len(hits) != 1 {
		t.Errorf("expected 1 hit for alias 'automobile', got %d", len(hits))
	}
	if hits[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", hits[0].DocID)
	}
}

func TestMemSegment(t *testing.T) {
	seg := segment.NewMemSegment("test_seg")

	// Add postings
	seg.Add("term1", "doc1", 2, []int{0, 5})
	seg.Add("term1", "doc2", 1, []int{0})
	seg.Add("term2", "doc1", 1, []int{3})

	// Check counts
	if seg.DocCount() != 2 {
		t.Errorf("expected 2 docs, got %d", seg.DocCount())
	}

	if seg.TermCount() != 2 {
		t.Errorf("expected 2 terms, got %d", seg.TermCount())
	}

	// Search
	hits := seg.Search("term1")
	if len(hits) != 2 {
		t.Errorf("expected 2 hits for term1, got %d", len(hits))
	}

	// Check DF
	df := seg.GetLocalDF("term1")
	if df != 2 {
		t.Errorf("expected DF=2 for term1, got %d", df)
	}
}

func TestDiskSegmentWriteAndRead(t *testing.T) {
	dir := t.TempDir()

	// Create a memory segment
	memSeg := segment.NewMemSegment("test_seg")
	memSeg.Add("term1", "doc1", 2, []int{0, 5})
	memSeg.Add("term1", "doc2", 1, []int{0})
	memSeg.Add("term2", "doc1", 1, []int{3})

	// Flush to disk
	diskSeg, err := segment.FlushMemSegment(memSeg, dir)
	if err != nil {
		t.Fatalf("FlushMemSegment: %v", err)
	}
	defer diskSeg.Close()

	// Verify metadata
	if diskSeg.DocCount() != 2 {
		t.Errorf("expected 2 docs, got %d", diskSeg.DocCount())
	}

	// Search
	hits := diskSeg.Search("term1")
	if len(hits) != 2 {
		t.Errorf("expected 2 hits for term1, got %d", len(hits))
	}

	// Check DF
	df := diskSeg.GetLocalDF("term1")
	if df != 2 {
		t.Errorf("expected DF=2 for term1, got %d", df)
	}

	// Reopen and verify
	diskSeg2, err := segment.OpenDiskSegment(diskSeg.Path())
	if err != nil {
		t.Fatalf("OpenDiskSegment: %v", err)
	}
	defer diskSeg2.Close()

	hits2 := diskSeg2.Search("term1")
	if len(hits2) != 2 {
		t.Errorf("after reopen: expected 2 hits, got %d", len(hits2))
	}
}

func TestTermRegistry(t *testing.T) {
	dir := t.TempDir()

	reg, err := segment.NewFSTTermRegistry(segment.FSTRegistryConfig{
		DataDir:          dir,
		RebuildThreshold: 100,
	})
	if err != nil {
		t.Fatalf("NewFSTTermRegistry: %v", err)
	}
	defer reg.Close()

	ctx := context.Background()

	// Create term
	termID1, err := reg.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	if termID1 == "" {
		t.Error("expected non-empty term ID")
	}

	// Get same term again
	termID2, err := reg.GetOrCreate(ctx, "title", "hello")
	if err != nil {
		t.Fatalf("GetOrCreate second: %v", err)
	}

	if termID1 != termID2 {
		t.Errorf("expected same term ID, got %s != %s", termID1, termID2)
	}

	// Create alias
	err = reg.CreateAlias("title", "hi", termID1)
	if err != nil {
		t.Fatalf("CreateAlias: %v", err)
	}

	// Lookup alias
	aliasID, found := reg.Get(ctx, "title", "hi")
	if !found {
		t.Error("expected alias to be found")
	}
	if aliasID != termID1 {
		t.Errorf("expected alias to point to %s, got %s", termID1, aliasID)
	}

	// Test DF
	reg.IncrementDF(termID1, 5)
	df := reg.GetDF(termID1)
	if df != 5 {
		t.Errorf("expected DF=5, got %d", df)
	}
}

func TestBM25Scorer(t *testing.T) {
	scorer := segment.NewBM25Scorer()

	ctx := segment.ScoringContext{
		TotalDocs: 1000,
		AvgDocLen: 100,
		TermDF: map[string]int64{
			"term1": 10,  // Rare term
			"term2": 500, // Common term
		},
		DocLens: map[string]int{
			"doc1": 100,
			"doc2": 50,
		},
	}

	// Rare term should score higher than common term with same TF
	scoreRare := scorer.Score(ctx, "term1", 1, 100)
	scoreCommon := scorer.Score(ctx, "term2", 1, 100)

	if scoreRare <= scoreCommon {
		t.Errorf("rare term should score higher: rare=%f, common=%f", scoreRare, scoreCommon)
	}

	// Higher TF should score higher
	scoreTF1 := scorer.Score(ctx, "term1", 1, 100)
	scoreTF3 := scorer.Score(ctx, "term1", 3, 100)

	if scoreTF3 <= scoreTF1 {
		t.Errorf("higher TF should score higher: TF1=%f, TF3=%f", scoreTF1, scoreTF3)
	}

	// Shorter docs should score higher (for same TF)
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

	// Add some items
	items := []string{"term1", "term2", "term3", "hello", "world"}
	for _, item := range items {
		bloom.Add(item)
	}

	// Test that added items are found
	for _, item := range items {
		if !bloom.MayContain(item) {
			t.Errorf("bloom filter should contain %s", item)
		}
	}

	// Test that non-existent items are (mostly) not found
	// Note: Some false positives are expected
	notFound := 0
	testItems := []string{"notexist1", "notexist2", "notexist3", "foo", "bar", "baz"}
	for _, item := range testItems {
		if !bloom.MayContain(item) {
			notFound++
		}
	}

	// At least some should not be found (false positive rate is 1%)
	if notFound == 0 {
		t.Error("bloom filter has too many false positives")
	}
}

func TestBloomFilterInDiskSegment(t *testing.T) {
	dir := t.TempDir()

	// Create a memory segment with terms
	memSeg := segment.NewMemSegment("test_bloom_seg")
	memSeg.Add("term_alpha", "doc1", 2, []int{0, 5})
	memSeg.Add("term_beta", "doc2", 1, []int{0})
	memSeg.Add("term_gamma", "doc1", 1, []int{3})

	// Flush to disk (this creates bloom filter)
	diskSeg, err := segment.FlushMemSegment(memSeg, dir)
	if err != nil {
		t.Fatalf("FlushMemSegment: %v", err)
	}
	defer diskSeg.Close()

	// Search for existing terms - should find them
	hits := diskSeg.Search("term_alpha")
	if len(hits) == 0 {
		t.Error("expected to find term_alpha")
	}

	hits = diskSeg.Search("term_beta")
	if len(hits) == 0 {
		t.Error("expected to find term_beta")
	}

	// Search for non-existent term - bloom filter should reject quickly
	hits = diskSeg.Search("term_nonexistent")
	if len(hits) != 0 {
		t.Error("expected no hits for non-existent term")
	}

	// Reopen and verify bloom filter persisted
	diskSeg2, err := segment.OpenDiskSegment(diskSeg.Path())
	if err != nil {
		t.Fatalf("OpenDiskSegment: %v", err)
	}
	defer diskSeg2.Close()

	// Should still find existing terms
	hits = diskSeg2.Search("term_alpha")
	if len(hits) == 0 {
		t.Error("after reopen: expected to find term_alpha")
	}

	// Should still reject non-existent terms
	hits = diskSeg2.Search("term_nonexistent")
	if len(hits) != 0 {
		t.Error("after reopen: expected no hits for non-existent term")
	}
}
