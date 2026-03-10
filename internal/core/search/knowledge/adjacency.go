package knowledge

import (
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"

	pebblestore "plastic-engine-core/internal/adapters/storage/pebble"

	pebbledb "github.com/cockroachdb/pebble"
)

// errShortEdge is returned when a binary edge payload is too short to decode.
var errShortEdge = fmt.Errorf("edge data too short for binary decode")

// sourceToEnum maps a Source string to a compact byte enum.
func sourceToEnum(s string) byte {
	switch s {
	case "pmi":
		return 0
	case "idf":
		return 1
	case "agent":
		return 2
	case "user":
		return 3
	default:
		return 255
	}
}

// enumToSource maps a byte enum back to the Source string.
func enumToSource(b byte) string {
	switch b {
	case 0:
		return "pmi"
	case 1:
		return "idf"
	case 2:
		return "agent"
	case 3:
		return "user"
	default:
		return ""
	}
}

// encodeEdgeData encodes EdgeData into a compact binary format.
// Layout: [weight:8][decay:8][generation:1][source:1][typeLen:1][type:N]
func encodeEdgeData(d EdgeData) []byte {
	buf := make([]byte, 19+len(d.EdgeType))
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(d.Weight))
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(d.Decay))
	buf[16] = d.Generation
	buf[17] = sourceToEnum(d.Source)
	buf[18] = byte(len(d.EdgeType))
	copy(buf[19:], d.EdgeType)
	return buf
}

// decodeEdgeData decodes EdgeData from binary or falls back to JSON for legacy data.
func decodeEdgeData(b []byte) (EdgeData, error) {
	if len(b) == 0 {
		return EdgeData{}, errShortEdge
	}
	// JSON fallback: if first byte is '{', this is legacy JSON-encoded data
	if b[0] == '{' {
		var d EdgeData
		if err := json.Unmarshal(b, &d); err != nil {
			return EdgeData{}, err
		}
		return d, nil
	}
	if len(b) < 19 {
		return EdgeData{}, errShortEdge
	}
	d := EdgeData{
		Weight:     math.Float64frombits(binary.LittleEndian.Uint64(b[0:8])),
		Decay:      math.Float64frombits(binary.LittleEndian.Uint64(b[8:16])),
		Generation: b[16],
		Source:     enumToSource(b[17]),
	}
	typeLen := int(b[18])
	if len(b) >= 19+typeLen {
		d.EdgeType = string(b[19 : 19+typeLen])
	}
	return d, nil
}

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
	Generation uint8  `json:"g"`
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
	// Cache is an optional shared Pebble block cache (nil = Pebble default 8MB).
	Cache *pebbledb.Cache
	// EdgeCacheSize is the max entries in the in-memory LRU edge cache. 0 = disabled.
	EdgeCacheSize int
}

// AdjacencyMatrix is a Pebble-backed adjacency matrix for term/concept relationships.
// It supports two key patterns:
//   - Term DF tracking: T\x00field\x00term → int64
//   - Edge storage: E\x00out\x00nodeA\x00nodeB → EdgeData (+ reverse index)
//
// All graph operations are prefix scans on Pebble — no in-memory graph.
type AdjacencyMatrix struct {
	db        *pebblestore.PebbleStore
	edgeCache *edgeLRU // nil = disabled
}

// NewAdjacencyMatrix opens or creates an adjacency matrix at the given path.
func NewAdjacencyMatrix(cfg AdjacencyMatrixConfig) (*AdjacencyMatrix, error) {
	dbPath := filepath.Join(cfg.DataDir, "adjacency")
	db, err := pebblestore.NewPebbleStoreWithConfig(dbPath, pebblestore.StoreConfig{
		SyncWrites:               false,
		MaxConcurrentCompactions: 1,
		MemTableSize:             8 * 1024 * 1024, // 8MB
		Cache:                    cfg.Cache,
	})
	if err != nil {
		return nil, fmt.Errorf("open adjacency matrix at %s: %w", dbPath, err)
	}
	am := &AdjacencyMatrix{db: db}
	if cfg.EdgeCacheSize > 0 {
		am.edgeCache = newEdgeLRU(cfg.EdgeCacheSize)
	}
	return am, nil
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
	edgeBin := encodeEdgeData(data)

	outKey := edgeOutPrefix + nodeA + "\x00" + nodeB
	inKey := edgeInPrefix + nodeB + "\x00" + nodeA

	batch := am.db.NewBatch()
	_ = batch.SetBytes(outKey, edgeBin)
	_ = batch.SetBytes(inKey, edgeBin)
	err := batch.Commit()
	batch.Close()

	// Invalidate edge cache for both endpoints.
	// Note: during cold epoch bulk writes this causes thrashing, but TTL-based
	// expiry (60s) handles staleness for the read path. Only invalidate for
	// non-cooccurrence edges (agent, user) to avoid cache thrashing.
	if am.edgeCache != nil && data.Source != "pmi" {
		am.edgeCache.invalidate(nodeA)
		am.edgeCache.invalidate(nodeB)
	}

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
	// Check LRU cache first (only for minWeight=0 which is the common hot path)
	if am.edgeCache != nil && minWeight == 0 {
		if cached, ok := am.edgeCache.get(node); ok {
			return cached
		}
	}

	prefix := edgeOutPrefix + node + "\x00"
	kvs, err := am.db.PrefixScanBytes(prefix)
	if err != nil {
		return nil
	}

	var edges []Edge
	for _, kv := range kvs {
		data, err := decodeEdgeData(kv.Value)
		if err != nil {
			continue
		}

		// Extract target node from key: "E\x00out\x00" + node + "\x00" + target
		target := string(kv.Key[len(prefix):])
		edges = append(edges, Edge{
			From: node,
			To:   target,
			Data: data,
		})
	}

	// Store unfiltered results in cache
	if am.edgeCache != nil && minWeight == 0 {
		am.edgeCache.put(node, edges)
	}

	// Apply weight filter after caching
	if minWeight > 0 {
		filtered := edges[:0]
		for _, e := range edges {
			if e.Data.Weight >= minWeight {
				filtered = append(filtered, e)
			}
		}
		return filtered
	}
	return edges
}

// GetIncomingEdges returns all incoming edges to a node.
func (am *AdjacencyMatrix) GetIncomingEdges(node string, minWeight float64) []Edge {
	prefix := edgeInPrefix + node + "\x00"
	kvs, err := am.db.PrefixScanBytes(prefix)
	if err != nil {
		return nil
	}

	var edges []Edge
	for _, kv := range kvs {
		data, err := decodeEdgeData(kv.Value)
		if err != nil {
			continue
		}
		if data.Weight < minWeight {
			continue
		}

		// Extract source node from key: "E\x00in\x00" + node + "\x00" + source
		source := string(kv.Key[len(prefix):])
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

// SpreadTopK performs a multi-hop traversal like Spread, but caps the fan-out
// per frontier node to maxFanOut using a top-K selection with epsilon-greedy
// exploration. This prevents O(V^d) explosion while maintaining diversity.
func (am *AdjacencyMatrix) SpreadTopK(startNode string, hops int, decay float64, maxFanOut int, epsilon float64) map[string]float64 {
	if hops <= 0 {
		return nil
	}
	if decay <= 0 || decay > 1 {
		decay = 0.7
	}
	if maxFanOut <= 0 {
		maxFanOut = 50
	}
	if epsilon < 0 || epsilon > 1 {
		epsilon = 0.05
	}

	result := make(map[string]float64)
	frontier := map[string]float64{startNode: 1.0}

	for hop := 0; hop < hops; hop++ {
		nextFrontier := make(map[string]float64)
		for node, weight := range frontier {
			edges := am.GetEdges(node, 0)
			selected := selectTopKEpsilon(edges, startNode, maxFanOut, epsilon)
			for _, edge := range selected {
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

// selectTopKEpsilon selects up to maxFanOut edges using epsilon-greedy strategy:
// (1-epsilon) fraction from top by weight, epsilon fraction randomly from the tail.
func selectTopKEpsilon(edges []Edge, excludeNode string, maxFanOut int, epsilon float64) []Edge {
	// Filter out self-loops back to excludeNode
	filtered := make([]Edge, 0, len(edges))
	for _, e := range edges {
		if e.To != excludeNode {
			filtered = append(filtered, e)
		}
	}

	if len(filtered) <= maxFanOut {
		return filtered
	}

	// Sort by weight descending
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Data.Weight > filtered[j].Data.Weight
	})

	exploreSlots := int(float64(maxFanOut) * epsilon)
	if exploreSlots < 1 {
		exploreSlots = 1
	}
	greedySlots := maxFanOut - exploreSlots

	result := make([]Edge, 0, maxFanOut)
	result = append(result, filtered[:greedySlots]...)

	// Fisher-Yates partial shuffle on the tail to pick exploreSlots random edges
	tail := filtered[greedySlots:]
	for i := 0; i < exploreSlots && i < len(tail); i++ {
		j := i + rand.Intn(len(tail)-i)
		tail[i], tail[j] = tail[j], tail[i]
		result = append(result, tail[i])
	}

	return result
}

// --------------------------------------------------------------------------
// SpreadActivation — priority-queue based graph traversal with iterator reuse
// --------------------------------------------------------------------------

// activationEntry is a node queued for exploration during spread activation.
type activationEntry struct {
	node   string
	energy float64
	hops   int
}

// activationHeap is a max-heap by energy.
type activationHeap []activationEntry

func (h activationHeap) Len() int            { return len(h) }
func (h activationHeap) Less(i, j int) bool  { return h[i].energy > h[j].energy }
func (h activationHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *activationHeap) Push(x interface{}) { *h = append(*h, x.(activationEntry)) }
func (h *activationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// SpreadActivation performs best-first graph traversal from startNode using a
// priority queue. Unlike BFS-based Spread/SpreadTopK, it explores highest-energy
// paths first and prunes low-energy branches via energyThreshold.
//
// Parameters:
//   - entryEnergy: initial energy for startNode (caller sets 1.0/len(tokens) for repartition)
//   - maxHops: maximum traversal depth
//   - decay: multiplicative decay per hop (0,1]
//   - maxFanOut: max outgoing edges explored per node
//   - epsilon: exploration fraction for selectTopKEpsilon
//   - energyThreshold: minimum energy to enqueue a node (prunes noise)
//
// Returns a map of reachable node → accumulated energy (excludes startNode).
func (am *AdjacencyMatrix) SpreadActivation(
	startNode string, entryEnergy float64, maxHops int, decay float64,
	maxFanOut int, epsilon float64, energyThreshold float64,
) map[string]float64 {
	if maxHops <= 0 || entryEnergy <= 0 {
		return nil
	}
	if decay <= 0 || decay > 1 {
		decay = 0.7
	}
	if maxFanOut <= 0 {
		maxFanOut = 50
	}
	if epsilon < 0 || epsilon > 1 {
		epsilon = 0.05
	}

	// Create a reusable bounded iterator for edge scans.
	bi, err := am.db.NewBoundedIterator()
	if err != nil {
		return nil
	}
	defer bi.Close()

	result := make(map[string]float64)
	visited := make(map[string]struct{})

	h := &activationHeap{{node: startNode, energy: entryEnergy, hops: 0}}
	heap.Init(h)

	for h.Len() > 0 {
		cur := heap.Pop(h).(activationEntry)

		if _, seen := visited[cur.node]; seen {
			continue
		}
		visited[cur.node] = struct{}{}

		// Record in result (skip startNode itself)
		if cur.node != startNode {
			result[cur.node] = cur.energy
		}

		if cur.hops >= maxHops {
			continue
		}

		// Get edges using cache or bounded iterator
		edges := am.getEdgesCachedOrIter(cur.node, bi)
		selected := selectTopKEpsilon(edges, startNode, maxFanOut, epsilon)

		for _, edge := range selected {
			if _, seen := visited[edge.To]; seen {
				continue
			}
			newEnergy := cur.energy * edge.Data.Weight * decay
			if newEnergy >= energyThreshold {
				heap.Push(h, activationEntry{
					node:   edge.To,
					energy: newEnergy,
					hops:   cur.hops + 1,
				})
			}
		}
	}

	return result
}

// getEdgesCachedOrIter retrieves edges for a node, checking the LRU cache first.
// On cache miss it uses the provided BoundedIterator to scan edges inline.
// Does NOT populate the cache to avoid thrashing during traversal.
func (am *AdjacencyMatrix) getEdgesCachedOrIter(node string, bi *pebblestore.BoundedIterator) []Edge {
	// Check LRU cache first
	if am.edgeCache != nil {
		if cached, ok := am.edgeCache.get(node); ok {
			return cached
		}
	}

	// Scan edges using bounded iterator (no allocation of []KeyValueBytes)
	prefix := []byte(edgeOutPrefix + node + "\x00")
	var edges []Edge
	_ = bi.ScanPrefix(prefix, func(key, value []byte) bool {
		data, err := decodeEdgeData(value)
		if err != nil {
			return true // skip bad entries
		}
		target := string(key[len(prefix):])
		edges = append(edges, Edge{
			From: node,
			To:   target,
			Data: data,
		})
		return true
	})

	return edges
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
	kvs, err := am.db.PrefixScanBytes(edgeOutPrefix)
	if err != nil {
		return err
	}

	batch := am.db.NewBatch()
	var toDelete []string

	for _, kv := range kvs {
		data, err := decodeEdgeData(kv.Value)
		if err != nil {
			continue
		}

		key := string(kv.Key)
		data.Weight *= factor
		if data.Weight < 0.001 {
			// Mark for deletion
			toDelete = append(toDelete, key)
			continue
		}

		edgeBin := encodeEdgeData(data)

		// Update outgoing edge
		_ = batch.SetBytes(key, edgeBin)

		// Update corresponding incoming edge
		// Parse nodes from key: "E\x00out\x00" + nodeA + "\x00" + nodeB
		rest := key[len(edgeOutPrefix):]
		sep := strings.Index(rest, "\x00")
		if sep < 0 {
			continue
		}
		nodeA := rest[:sep]
		nodeB := rest[sep+1:]
		inKey := edgeInPrefix + nodeB + "\x00" + nodeA
		_ = batch.SetBytes(inKey, edgeBin)
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

