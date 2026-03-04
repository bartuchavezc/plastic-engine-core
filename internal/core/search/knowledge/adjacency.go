package knowledge

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	pebblestore "plastic-engine-core/internal/adapters/storage/pebble"
)

// Key namespace prefixes for the adjacency matrix.
// Using uppercase letters + \x00 as namespace separator.
const (
	// termPrefix: "T\x00" + field + "\x00" + term → DF (int64, merge operator)
	termPrefix = "T\x00"

	// edgeOutPrefix: "E\x00out\x00" + termA + "\x00" + termB → EdgeData JSON
	edgeOutPrefix = "E\x00out\x00"

	// edgeInPrefix: "E\x00in\x00" + termB + "\x00" + termA → EdgeData JSON
	edgeInPrefix = "E\x00in\x00"

	// nodePrefix: "N\x00" + nodeID → NodeData JSON
	nodePrefix = "N\x00"

	// metaTotalTerms: total number of unique terms
	metaTotalTerms = "M\x00total_terms"
)

// EdgeData represents the data associated with an edge in the adjacency matrix.
type EdgeData struct {
	Weight    float64 `json:"w"`
	EdgeType  string  `json:"t,omitempty"`
	Decay     float64 `json:"d,omitempty"`
	CreatedAt int64   `json:"c"`
	Source    string  `json:"s,omitempty"` // "idf", "agent", "user"
}

// NodeData represents metadata for a node in the knowledge graph.
type NodeData struct {
	Label    string            `json:"label,omitempty"`
	Type     string            `json:"type,omitempty"`
	Metadata map[string]string `json:"meta,omitempty"`
}

// Edge represents a resolved edge with both endpoints.
type Edge struct {
	From string   `json:"from"`
	To   string   `json:"to"`
	Data EdgeData `json:"data"`
}

// TermEntry represents a term with its document frequency.
type TermEntry struct {
	Field string `json:"field"`
	Term  string `json:"term"`
	DF    int64  `json:"df"`
}

// AdjacencyMatrixConfig configures an AdjacencyMatrix.
type AdjacencyMatrixConfig struct {
	DataDir string
}

// AdjacencyMatrix is a Pebble-backed adjacency matrix for term/concept relationships.
// It supports two key patterns:
//   - Term DF tracking: T\x00field\x00term → int64
//   - Edge storage: E\x00out\x00nodeA\x00nodeB → EdgeData (+ reverse index)
//
// All graph operations are prefix scans on Pebble — no in-memory graph.
type AdjacencyMatrix struct {
	db *pebblestore.PebbleStore
}

// NewAdjacencyMatrix opens or creates an adjacency matrix at the given path.
func NewAdjacencyMatrix(cfg AdjacencyMatrixConfig) (*AdjacencyMatrix, error) {
	dbPath := filepath.Join(cfg.DataDir, "adjacency")
	db, err := pebblestore.NewPebbleStoreWithConfig(dbPath, pebblestore.StoreConfig{
		SyncWrites:               false,
		MaxConcurrentCompactions: 1,
		MemTableSize:             8 * 1024 * 1024, // 8MB
	})
	if err != nil {
		return nil, fmt.Errorf("open adjacency matrix at %s: %w", dbPath, err)
	}
	return &AdjacencyMatrix{db: db}, nil
}

// Close closes the adjacency matrix.
func (am *AdjacencyMatrix) Close() error {
	if am.db != nil {
		return am.db.Close()
	}
	return nil
}

// --- Term DF operations ---

// GetDF returns the document frequency for a term.
func (am *AdjacencyMatrix) GetDF(field, term string) int64 {
	key := termPrefix + field + "\x00" + term
	val, err := am.db.GetInt64(key)
	if err != nil {
		return 0
	}
	return val
}

// IngestTermBatch atomically increments DF for a batch of terms.
// The deltas map has keys in the format "field\x00term" → df delta.
func (am *AdjacencyMatrix) IngestTermBatch(deltas map[string]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	batch := am.db.NewBatch()
	for termKey, delta := range deltas {
		key := termPrefix + termKey
		_ = batch.MergeInt64(key, delta)
	}
	err := batch.Commit()
	batch.Close()
	return err
}

// --- Edge operations ---

// AddEdge adds or updates a directed edge between two nodes.
// Stores both outgoing (A→B) and incoming (B→A) keys for bidirectional traversal.
func (am *AdjacencyMatrix) AddEdge(nodeA, nodeB string, data EdgeData) error {
	edgeJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal edge data: %w", err)
	}

	outKey := edgeOutPrefix + nodeA + "\x00" + nodeB
	inKey := edgeInPrefix + nodeB + "\x00" + nodeA

	batch := am.db.NewBatch()
	_ = batch.Set(outKey, string(edgeJSON))
	_ = batch.Set(inKey, string(edgeJSON))
	err = batch.Commit()
	batch.Close()
	return err
}

// DeleteEdge removes a directed edge between two nodes.
func (am *AdjacencyMatrix) DeleteEdge(nodeA, nodeB string) error {
	outKey := edgeOutPrefix + nodeA + "\x00" + nodeB
	inKey := edgeInPrefix + nodeB + "\x00" + nodeA

	// Delete both directions
	if err := am.db.Delete(outKey); err != nil && !pebblestore.IsNotFound(err) {
		return err
	}
	if err := am.db.Delete(inKey); err != nil && !pebblestore.IsNotFound(err) {
		return err
	}
	return nil
}

// GetEdges returns all outgoing edges from a node, optionally filtered by minimum weight.
func (am *AdjacencyMatrix) GetEdges(node string, minWeight float64) []Edge {
	prefix := edgeOutPrefix + node + "\x00"
	kvs, err := am.db.PrefixScan(prefix)
	if err != nil {
		return nil
	}

	var edges []Edge
	for _, kv := range kvs {
		var data EdgeData
		if err := json.Unmarshal([]byte(kv.Value), &data); err != nil {
			continue
		}
		if data.Weight < minWeight {
			continue
		}

		// Extract target node from key: "E\x00out\x00" + node + "\x00" + target
		target := kv.Key[len(prefix):]
		edges = append(edges, Edge{
			From: node,
			To:   target,
			Data: data,
		})
	}
	return edges
}

// GetIncomingEdges returns all incoming edges to a node.
func (am *AdjacencyMatrix) GetIncomingEdges(node string, minWeight float64) []Edge {
	prefix := edgeInPrefix + node + "\x00"
	kvs, err := am.db.PrefixScan(prefix)
	if err != nil {
		return nil
	}

	var edges []Edge
	for _, kv := range kvs {
		var data EdgeData
		if err := json.Unmarshal([]byte(kv.Value), &data); err != nil {
			continue
		}
		if data.Weight < minWeight {
			continue
		}

		// Extract source node from key: "E\x00in\x00" + node + "\x00" + source
		source := kv.Key[len(prefix):]
		edges = append(edges, Edge{
			From: source,
			To:   node,
			Data: data,
		})
	}
	return edges
}

// Spread performs a multi-hop traversal from a starting node, accumulating
// weight with decay at each hop. Returns nodes reachable within `hops` steps
// with their accumulated weights.
func (am *AdjacencyMatrix) Spread(startNode string, hops int, decay float64) map[string]float64 {
	if hops <= 0 {
		return nil
	}
	if decay <= 0 || decay > 1 {
		decay = 0.7
	}

	result := make(map[string]float64)
	frontier := map[string]float64{startNode: 1.0}

	for hop := 0; hop < hops; hop++ {
		nextFrontier := make(map[string]float64)
		for node, weight := range frontier {
			edges := am.GetEdges(node, 0)
			for _, edge := range edges {
				if edge.To == startNode {
					continue // skip self-loops back to start
				}
				propagated := weight * edge.Data.Weight * decay
				if propagated > nextFrontier[edge.To] {
					nextFrontier[edge.To] = propagated
				}
			}
		}
		for node, weight := range nextFrontier {
			if weight > result[node] {
				result[node] = weight
			}
		}
		frontier = nextFrontier
	}

	return result
}

// ListTermsByPrefix returns terms in the matrix matching a field+prefix.
func (am *AdjacencyMatrix) ListTermsByPrefix(field, prefix string, limit int) []TermEntry {
	searchPrefix := termPrefix + field + "\x00" + prefix
	fieldPrefix := termPrefix + field + "\x00"

	var kvs []pebblestore.KeyValue
	var err error
	if limit > 0 {
		kvs, err = am.db.PrefixScanLimit(searchPrefix, limit)
	} else {
		kvs, err = am.db.PrefixScan(searchPrefix)
	}
	if err != nil {
		return nil
	}

	entries := make([]TermEntry, 0, len(kvs))
	for _, kv := range kvs {
		if len(kv.Key) <= len(fieldPrefix) {
			continue
		}
		term := kv.Key[len(fieldPrefix):]
		df, _ := am.db.GetInt64(kv.Key)
		entries = append(entries, TermEntry{
			Field: field,
			Term:  term,
			DF:    df,
		})
	}
	return entries
}

// DecayEdges multiplies all edge weights by the given factor (0 < factor < 1).
// Edges with weight below a threshold (0.001) are deleted.
func (am *AdjacencyMatrix) DecayEdges(factor float64) error {
	if factor <= 0 || factor >= 1 {
		return fmt.Errorf("decay factor must be between 0 and 1 (exclusive), got %f", factor)
	}

	// Scan all outgoing edges
	kvs, err := am.db.PrefixScan(edgeOutPrefix)
	if err != nil {
		return err
	}

	batch := am.db.NewBatch()
	var toDelete []string

	for _, kv := range kvs {
		var data EdgeData
		if err := json.Unmarshal([]byte(kv.Value), &data); err != nil {
			continue
		}

		data.Weight *= factor
		if data.Weight < 0.001 {
			// Mark for deletion
			toDelete = append(toDelete, kv.Key)
			continue
		}

		edgeJSON, err := json.Marshal(data)
		if err != nil {
			continue
		}

		// Update outgoing edge
		_ = batch.Set(kv.Key, string(edgeJSON))

		// Update corresponding incoming edge
		// Parse nodes from key: "E\x00out\x00" + nodeA + "\x00" + nodeB
		rest := kv.Key[len(edgeOutPrefix):]
		sep := strings.Index(rest, "\x00")
		if sep < 0 {
			continue
		}
		nodeA := rest[:sep]
		nodeB := rest[sep+1:]
		inKey := edgeInPrefix + nodeB + "\x00" + nodeA
		_ = batch.Set(inKey, string(edgeJSON))
	}

	// Delete edges below threshold
	for _, key := range toDelete {
		_ = batch.Delete(key)
		// Also delete incoming edge
		rest := key[len(edgeOutPrefix):]
		sep := strings.Index(rest, "\x00")
		if sep >= 0 {
			nodeA := rest[:sep]
			nodeB := rest[sep+1:]
			inKey := edgeInPrefix + nodeB + "\x00" + nodeA
			_ = batch.Delete(inKey)
		}
	}

	err = batch.Commit()
	batch.Close()
	return err
}

// SetNode stores metadata for a node.
func (am *AdjacencyMatrix) SetNode(nodeID string, data NodeData) error {
	nodeJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal node data: %w", err)
	}
	return am.db.Set(nodePrefix+nodeID, string(nodeJSON))
}

// GetNode retrieves metadata for a node.
func (am *AdjacencyMatrix) GetNode(nodeID string) (NodeData, bool) {
	val, err := am.db.Get(nodePrefix + nodeID)
	if err != nil {
		return NodeData{}, false
	}
	var data NodeData
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		return NodeData{}, false
	}
	return data, true
}

// DeleteNode removes a node and all its edges.
func (am *AdjacencyMatrix) DeleteNode(nodeID string) error {
	// Delete node metadata
	_ = am.db.Delete(nodePrefix + nodeID)

	// Delete outgoing edges
	outEdges := am.GetEdges(nodeID, 0)
	for _, edge := range outEdges {
		_ = am.DeleteEdge(nodeID, edge.To)
	}

	// Delete incoming edges
	inEdges := am.GetIncomingEdges(nodeID, 0)
	for _, edge := range inEdges {
		_ = am.DeleteEdge(edge.From, nodeID)
	}

	return nil
}

// ListNodes returns all node IDs, optionally filtered by prefix.
func (am *AdjacencyMatrix) ListNodes(prefix string, limit int) []string {
	searchPrefix := nodePrefix + prefix
	var kvs []pebblestore.KeyValue
	var err error
	if limit > 0 {
		kvs, err = am.db.PrefixScanLimit(searchPrefix, limit)
	} else {
		kvs, err = am.db.PrefixScan(searchPrefix)
	}
	if err != nil {
		return nil
	}

	nodes := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		nodeID := kv.Key[len(nodePrefix):]
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// Stats returns statistics about the adjacency matrix.
type AdjacencyStats struct {
	TermCount int `json:"term_count"`
	EdgeCount int `json:"edge_count"`
	NodeCount int `json:"node_count"`
}

func (am *AdjacencyMatrix) Stats() AdjacencyStats {
	terms, _ := am.db.PrefixScanKeys(termPrefix)
	edges, _ := am.db.PrefixScanKeys(edgeOutPrefix)
	nodes, _ := am.db.PrefixScanKeys(nodePrefix)
	return AdjacencyStats{
		TermCount: len(terms),
		EdgeCount: len(edges),
		NodeCount: len(nodes),
	}
}

