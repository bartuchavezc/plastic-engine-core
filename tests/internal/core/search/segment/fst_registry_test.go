package segment_test

import (
	"context"
	"testing"

	"plastic-engine-core/internal/core/search/segment"
)

func TestFSTRegistryGetOrCreate(t *testing.T) {
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

	// Get should find it
	foundID, found := reg.Get(ctx, "title", "hello")
	if !found {
		t.Error("expected term to be found")
	}
	if foundID != termID1 {
		t.Errorf("expected %s, got %s", termID1, foundID)
	}

	// Non-existent term should not be found
	_, found = reg.Get(ctx, "title", "nonexistent")
	if found {
		t.Error("expected term to not be found")
	}
}

func TestFSTRegistryCreateAlias(t *testing.T) {
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

	// Create original term
	termID, err := reg.GetOrCreate(ctx, "title", "car")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Create alias
	err = reg.CreateAlias("title", "automobile", termID)
	if err != nil {
		t.Fatalf("CreateAlias: %v", err)
	}

	// Alias should resolve to same term ID
	aliasID, found := reg.Get(ctx, "title", "automobile")
	if !found {
		t.Error("expected alias to be found")
	}
	if aliasID != termID {
		t.Errorf("expected alias to point to %s, got %s", termID, aliasID)
	}
}

func TestFSTRegistryListTermsByPrefix(t *testing.T) {
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

	// Create terms
	terms := []string{"hello", "help", "helicopter", "world", "wonder"}
	for _, term := range terms {
		_, err := reg.GetOrCreate(ctx, "title", term)
		if err != nil {
			t.Fatalf("GetOrCreate %s: %v", term, err)
		}
	}

	// Search by prefix
	entries, err := reg.ListTermsByPrefix(ctx, "title", "hel", 10)
	if err != nil {
		t.Fatalf("ListTermsByPrefix: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("expected 3 entries for prefix 'hel', got %d", len(entries))
	}

	// Verify all start with "hel"
	for _, e := range entries {
		if len(e.Term) < 3 || e.Term[:3] != "hel" {
			t.Errorf("expected term starting with 'hel', got %s", e.Term)
		}
	}
}

func TestFSTRegistryListTermsByFuzzy(t *testing.T) {
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

	// Create terms
	terms := []string{"hello", "hallo", "jello", "world", "help"}
	for _, term := range terms {
		_, err := reg.GetOrCreate(ctx, "title", term)
		if err != nil {
			t.Fatalf("GetOrCreate %s: %v", term, err)
		}
	}

	// Fuzzy search for "hello" with distance 1
	entries, err := reg.ListTermsByFuzzy(ctx, "title", "hello", 1, 10)
	if err != nil {
		t.Fatalf("ListTermsByFuzzy: %v", err)
	}

	// Should find "hello" (distance 0) and "hallo" (distance 1) and "jello" (distance 1)
	if len(entries) < 2 {
		t.Errorf("expected at least 2 fuzzy matches, got %d", len(entries))
	}

	// Verify "hello" is included
	found := false
	for _, e := range entries {
		if e.Term == "hello" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'hello' in fuzzy results")
	}
}

func TestFSTRegistryListTermsByRegex(t *testing.T) {
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

	// Create terms
	terms := []string{"hello", "hallo", "help", "world", "wonder"}
	for _, term := range terms {
		_, err := reg.GetOrCreate(ctx, "title", term)
		if err != nil {
			t.Fatalf("GetOrCreate %s: %v", term, err)
		}
	}

	// Regex search for "h.*o" (terms starting with h and ending with o)
	entries, err := reg.ListTermsByRegex(ctx, "title", "^h.*o$", 10)
	if err != nil {
		t.Fatalf("ListTermsByRegex: %v", err)
	}

	// Should find "hello" and "hallo"
	if len(entries) != 2 {
		t.Errorf("expected 2 regex matches, got %d", len(entries))
	}

	for _, e := range entries {
		if e.Term != "hello" && e.Term != "hallo" {
			t.Errorf("unexpected term in regex results: %s", e.Term)
		}
	}
}

func TestFSTRegistryListTerms(t *testing.T) {
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

	// Create terms in different fields
	_, _ = reg.GetOrCreate(ctx, "title", "hello")
	_, _ = reg.GetOrCreate(ctx, "title", "world")
	_, _ = reg.GetOrCreate(ctx, "body", "foo")
	_, _ = reg.GetOrCreate(ctx, "body", "bar")

	// List all terms
	entries, total, err := reg.ListTerms(ctx, "", 10, 0)
	if err != nil {
		t.Fatalf("ListTerms: %v", err)
	}

	if total != 4 {
		t.Errorf("expected 4 total terms, got %d", total)
	}

	if len(entries) != 4 {
		t.Errorf("expected 4 entries, got %d", len(entries))
	}

	// List only title field
	entries, total, err = reg.ListTerms(ctx, "title", 10, 0)
	if err != nil {
		t.Fatalf("ListTerms title: %v", err)
	}

	if total != 2 {
		t.Errorf("expected 2 title terms, got %d", total)
	}
}

func TestFSTRegistryRebuild(t *testing.T) {
	dir := t.TempDir()

	reg, err := segment.NewFSTTermRegistry(segment.FSTRegistryConfig{
		DataDir:          dir,
		RebuildThreshold: 1000, // High threshold so we manually trigger rebuild
	})
	if err != nil {
		t.Fatalf("NewFSTTermRegistry: %v", err)
	}
	defer reg.Close()

	ctx := context.Background()

	// Create terms
	termID1, _ := reg.GetOrCreate(ctx, "title", "hello")
	termID2, _ := reg.GetOrCreate(ctx, "title", "world")

	// Rebuild
	err = reg.Rebuild()
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// Terms should still be accessible
	foundID1, found := reg.Get(ctx, "title", "hello")
	if !found || foundID1 != termID1 {
		t.Error("term 'hello' not found after rebuild")
	}

	foundID2, found := reg.Get(ctx, "title", "world")
	if !found || foundID2 != termID2 {
		t.Error("term 'world' not found after rebuild")
	}

	// Stats should show FST terms
	stats := reg.Stats()
	if stats.FSTTermCount != 2 {
		t.Errorf("expected 2 FST terms after rebuild, got %d", stats.FSTTermCount)
	}
	if stats.PendingTermCount != 0 {
		t.Errorf("expected 0 pending terms after rebuild, got %d", stats.PendingTermCount)
	}
}

func TestFSTRegistryDF(t *testing.T) {
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
	termID, _ := reg.GetOrCreate(ctx, "title", "hello")

	// Initial DF should be 0
	df := reg.GetDF(termID)
	if df != 0 {
		t.Errorf("expected initial DF=0, got %d", df)
	}

	// Increment DF
	reg.IncrementDF(termID, 5)

	df = reg.GetDF(termID)
	if df != 5 {
		t.Errorf("expected DF=5, got %d", df)
	}

	// Increment again
	reg.IncrementDF(termID, 3)

	df = reg.GetDF(termID)
	if df != 8 {
		t.Errorf("expected DF=8, got %d", df)
	}
}

func TestFSTRegistryStats(t *testing.T) {
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

	// Create terms
	_, _ = reg.GetOrCreate(ctx, "title", "hello")
	_, _ = reg.GetOrCreate(ctx, "title", "world")

	stats := reg.Stats()

	if stats.PendingTermCount != 2 {
		t.Errorf("expected 2 pending terms, got %d", stats.PendingTermCount)
	}

	if stats.TotalTermCount != 2 {
		t.Errorf("expected 2 total terms, got %d", stats.TotalTermCount)
	}

	if !stats.IsDirty {
		t.Error("expected registry to be dirty")
	}
}

func TestManagerWithFSTRegistry(t *testing.T) {
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

	// Verify FST registry is used (always the case now)
	fstReg := mgr.FSTRegistry()
	if fstReg == nil {
		t.Fatal("expected FST registry")
	}

	ctx := context.Background()

	// Index some documents
	err = mgr.Index(ctx, "title", "hello", "doc1", 2, []int{0, 5})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}
	err = mgr.Index(ctx, "title", "help", "doc2", 1, []int{0})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}
	err = mgr.Index(ctx, "title", "helicopter", "doc3", 1, []int{0})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}
	err = mgr.Index(ctx, "title", "world", "doc4", 1, []int{0})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Test prefix search
	entries, err := mgr.ListTermsByPrefix(ctx, "title", "hel", 10)
	if err != nil {
		t.Fatalf("ListTermsByPrefix: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries for prefix 'hel', got %d", len(entries))
	}

	// Test fuzzy search
	entries, err = mgr.ListTermsByFuzzy(ctx, "title", "hello", 1, 10)
	if err != nil {
		t.Fatalf("ListTermsByFuzzy: %v", err)
	}
	// Should find "hello" (distance 0) and "help" (distance 2) - actually distance > 1
	if len(entries) < 1 {
		t.Errorf("expected at least 1 fuzzy match, got %d", len(entries))
	}

	// Test regex search
	entries, err = mgr.ListTermsByRegex(ctx, "title", "^hel.*", 10)
	if err != nil {
		t.Fatalf("ListTermsByRegex: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 regex matches for '^hel.*', got %d", len(entries))
	}

	// Test list all terms
	entries, total, err := mgr.ListTerms(ctx, "title", 10, 0)
	if err != nil {
		t.Fatalf("ListTerms: %v", err)
	}
	if total != 4 {
		t.Errorf("expected 4 total terms, got %d", total)
	}
	if len(entries) != 4 {
		t.Errorf("expected 4 entries, got %d", len(entries))
	}
}
