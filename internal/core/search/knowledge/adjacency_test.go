package knowledge

import (
	"testing"
	"time"
)

func TestAdjacencyMatrixTermDF(t *testing.T) {
	dir := t.TempDir()
	am, err := NewAdjacencyMatrix(AdjacencyMatrixConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("NewAdjacencyMatrix: %v", err)
	}
	defer am.Close()

	// Ingest term batch
	deltas := map[string]int64{
		"title\x00hello": 5,
		"title\x00world": 3,
		"body\x00hello":  2,
	}
	if err := am.IngestTermBatch(deltas); err != nil {
		t.Fatalf("IngestTermBatch: %v", err)
	}

	// Check DF
	if df := am.GetDF("title", "hello"); df != 5 {
		t.Errorf("expected DF=5 for title/hello, got %d", df)
	}
	if df := am.GetDF("title", "world"); df != 3 {
		t.Errorf("expected DF=3 for title/world, got %d", df)
	}
	if df := am.GetDF("body", "hello"); df != 2 {
		t.Errorf("expected DF=2 for body/hello, got %d", df)
	}

	// Increment again
	deltas2 := map[string]int64{
		"title\x00hello": 10,
	}
	if err := am.IngestTermBatch(deltas2); err != nil {
		t.Fatalf("IngestTermBatch 2: %v", err)
	}

	if df := am.GetDF("title", "hello"); df != 15 {
		t.Errorf("expected DF=15 after second batch, got %d", df)
	}
}

func TestAdjacencyMatrixEdges(t *testing.T) {
	dir := t.TempDir()
	am, err := NewAdjacencyMatrix(AdjacencyMatrixConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("NewAdjacencyMatrix: %v", err)
	}
	defer am.Close()

	now := time.Now().Unix()

	// Add edges
	err = am.AddEdge("golang", "programming", EdgeData{
		Weight:    0.8,
		EdgeType:  "related",
		CreatedAt: now,
		Source:    "agent",
	})
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	err = am.AddEdge("golang", "concurrency", EdgeData{
		Weight:    0.6,
		EdgeType:  "feature",
		CreatedAt: now,
		Source:    "agent",
	})
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	// Get outgoing edges
	edges := am.GetEdges("golang", 0)
	if len(edges) != 2 {
		t.Fatalf("expected 2 outgoing edges, got %d", len(edges))
	}

	// Get edges with min weight filter
	edges = am.GetEdges("golang", 0.7)
	if len(edges) != 1 {
		t.Errorf("expected 1 edge with weight >= 0.7, got %d", len(edges))
	}
	if edges[0].To != "programming" {
		t.Errorf("expected edge to 'programming', got '%s'", edges[0].To)
	}

	// Get incoming edges
	inEdges := am.GetIncomingEdges("programming", 0)
	if len(inEdges) != 1 {
		t.Errorf("expected 1 incoming edge to 'programming', got %d", len(inEdges))
	}

	// Delete edge
	err = am.DeleteEdge("golang", "concurrency")
	if err != nil {
		t.Fatalf("DeleteEdge: %v", err)
	}

	edges = am.GetEdges("golang", 0)
	if len(edges) != 1 {
		t.Errorf("expected 1 edge after deletion, got %d", len(edges))
	}
}

func TestAdjacencyMatrixSpread(t *testing.T) {
	dir := t.TempDir()
	am, err := NewAdjacencyMatrix(AdjacencyMatrixConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("NewAdjacencyMatrix: %v", err)
	}
	defer am.Close()

	now := time.Now().Unix()

	// Build a small graph: A → B → C, A → D
	_ = am.AddEdge("A", "B", EdgeData{Weight: 0.9, CreatedAt: now})
	_ = am.AddEdge("B", "C", EdgeData{Weight: 0.8, CreatedAt: now})
	_ = am.AddEdge("A", "D", EdgeData{Weight: 0.5, CreatedAt: now})

	// Spread from A with 2 hops, decay 0.7
	result := am.Spread("A", 2, 0.7)

	// B should be reachable (1 hop): 1.0 * 0.9 * 0.7 = 0.63
	if w, ok := result["B"]; !ok {
		t.Error("expected B in spread result")
	} else if w < 0.62 || w > 0.64 {
		t.Errorf("expected B weight ~0.63, got %f", w)
	}

	// C should be reachable (2 hops): 0.63 * 0.8 * 0.7 = 0.3528
	if w, ok := result["C"]; !ok {
		t.Error("expected C in spread result")
	} else if w < 0.35 || w > 0.36 {
		t.Errorf("expected C weight ~0.353, got %f", w)
	}

	// D should be reachable (1 hop): 1.0 * 0.5 * 0.7 = 0.35
	if w, ok := result["D"]; !ok {
		t.Error("expected D in spread result")
	} else if w < 0.34 || w > 0.36 {
		t.Errorf("expected D weight ~0.35, got %f", w)
	}
}

func TestAdjacencyMatrixDecay(t *testing.T) {
	dir := t.TempDir()
	am, err := NewAdjacencyMatrix(AdjacencyMatrixConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("NewAdjacencyMatrix: %v", err)
	}
	defer am.Close()

	now := time.Now().Unix()

	_ = am.AddEdge("A", "B", EdgeData{Weight: 1.0, CreatedAt: now})
	_ = am.AddEdge("A", "C", EdgeData{Weight: 0.001, CreatedAt: now}) // Will be deleted after decay (0.001 * 0.5 = 0.0005 < 0.001)

	err = am.DecayEdges(0.5)
	if err != nil {
		t.Fatalf("DecayEdges: %v", err)
	}

	edges := am.GetEdges("A", 0)
	if len(edges) != 1 {
		t.Errorf("expected 1 edge after decay (C should be deleted), got %d", len(edges))
	}
	if len(edges) > 0 && edges[0].Data.Weight != 0.5 {
		t.Errorf("expected weight 0.5 after decay, got %f", edges[0].Data.Weight)
	}
}

func TestAdjacencyMatrixNodes(t *testing.T) {
	dir := t.TempDir()
	am, err := NewAdjacencyMatrix(AdjacencyMatrixConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("NewAdjacencyMatrix: %v", err)
	}
	defer am.Close()

	// Set node
	err = am.SetNode("concept_1", NodeData{
		Label: "Machine Learning",
		Type:  "concept",
		Metadata: map[string]string{
			"domain": "AI",
		},
	})
	if err != nil {
		t.Fatalf("SetNode: %v", err)
	}

	// Get node
	data, found := am.GetNode("concept_1")
	if !found {
		t.Fatal("expected node to be found")
	}
	if data.Label != "Machine Learning" {
		t.Errorf("expected label 'Machine Learning', got '%s'", data.Label)
	}
	if data.Metadata["domain"] != "AI" {
		t.Errorf("expected domain 'AI', got '%s'", data.Metadata["domain"])
	}

	// List nodes
	nodes := am.ListNodes("", 0)
	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	}
}

func TestAdjacencyMatrixListTermsByPrefix(t *testing.T) {
	dir := t.TempDir()
	am, err := NewAdjacencyMatrix(AdjacencyMatrixConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("NewAdjacencyMatrix: %v", err)
	}
	defer am.Close()

	deltas := map[string]int64{
		"title\x00apple":   10,
		"title\x00app":     5,
		"title\x00banana":  3,
		"body\x00apple":    2,
	}
	if err := am.IngestTermBatch(deltas); err != nil {
		t.Fatalf("IngestTermBatch: %v", err)
	}

	// List terms with prefix "app" in field "title"
	entries := am.ListTermsByPrefix("title", "app", 0)
	if len(entries) != 2 {
		t.Errorf("expected 2 terms with prefix 'app' in title, got %d", len(entries))
	}

	// List with limit
	entries = am.ListTermsByPrefix("title", "app", 1)
	if len(entries) != 1 {
		t.Errorf("expected 1 term with limit=1, got %d", len(entries))
	}
}
