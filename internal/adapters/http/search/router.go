package searchhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/core/search/knowledge"
	searchquery "plastic-engine-core/internal/core/search/query"
	"plastic-engine-core/internal/core/search/segment"
	"plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// Indexer defines the contract required to index documents.
type Indexer interface {
	Index(ctx context.Context, cmd document.Command) error
	IndexBulk(ctx context.Context, cmds []document.Command) []error
}

// Searcher defines the contract required to execute search queries.
type Searcher interface {
	Search(ctx context.Context, req searchquery.Request) (searchquery.Response, error)
}

// ShardSyncer handles shard synchronization operations.
type ShardSyncer interface {
	Sync(assignments []shards.Assignment) error
	UnloadIndex(indexID string) error
}

// TermLookup provides term lookup operations for a shard.
type TermLookup interface {
	// ListTerms returns all terms for a shard, optionally filtered by field.
	ListTerms(ctx context.Context, shardID, field string, limit, offset int) ([]segment.TermEntry, int64, error)
	// ListTermsByPrefix returns terms matching a prefix.
	ListTermsByPrefix(ctx context.Context, shardID, field, prefix string, limit int) ([]segment.TermEntry, error)
	// ListTermsByFuzzy returns terms within Levenshtein distance.
	ListTermsByFuzzy(ctx context.Context, shardID, field, query string, maxDistance, limit int) ([]segment.TermEntry, error)
	// ListTermsByRegex returns terms matching a regex pattern.
	ListTermsByRegex(ctx context.Context, shardID, field, pattern string, limit int) ([]segment.TermEntry, error)
}

// KnowledgeStore manages named knowledge graphs (Type B indices).
type KnowledgeStore interface {
	GetOrCreate(name string) (*knowledge.AdjacencyMatrix, error)
	Get(name string) (*knowledge.AdjacencyMatrix, bool)
	Delete(name string) error
	List() []string
}

// RouterConfig holds configuration for creating a Router.
type RouterConfig struct {
	Indexer        Indexer
	Searcher       Searcher
	ShardSyncer    ShardSyncer
	TermLookup     TermLookup
	KnowledgeStore KnowledgeStore
	Logger         logger.Logger
}

// NewRouter exposes the HTTP surface of a search node.
func NewRouter(idx Indexer, searcher Searcher, syncer ShardSyncer, log logger.Logger) http.Handler {
	return NewRouterWithConfig(RouterConfig{
		Indexer:     idx,
		Searcher:    searcher,
		ShardSyncer: syncer,
		Logger:      log,
	})
}

// NewRouterWithConfig creates a router with full configuration.
func NewRouterWithConfig(cfg RouterConfig) http.Handler {
	log := cfg.Logger
	if log == nil {
		log = logger.DefaultLogger()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/documents", func(w http.ResponseWriter, r *http.Request) {
		handleDocumentIngest(w, r, cfg.Indexer, log)
	})

	mux.HandleFunc("/documents/bulk", func(w http.ResponseWriter, r *http.Request) {
		handleBulkDocumentIngest(w, r, cfg.Indexer, log)
	})

	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		handleSearch(w, r, cfg.Searcher, log)
	})

	mux.HandleFunc("/shards/sync", func(w http.ResponseWriter, r *http.Request) {
		handleShardSync(w, r, cfg.ShardSyncer, log)
	})

	mux.HandleFunc("/indexes/deleted", func(w http.ResponseWriter, r *http.Request) {
		handleIndexDeleted(w, r, cfg.ShardSyncer, log)
	})

	// Term lookup APIs
	mux.HandleFunc("/terms", func(w http.ResponseWriter, r *http.Request) {
		handleTermLookup(w, r, cfg.TermLookup, log)
	})

	// Knowledge graph APIs (Type B)
	mux.HandleFunc("/knowledge/graphs", func(w http.ResponseWriter, r *http.Request) {
		handleKnowledgeGraphs(w, r, cfg.KnowledgeStore, log)
	})
	mux.HandleFunc("/knowledge/edges", func(w http.ResponseWriter, r *http.Request) {
		handleKnowledgeEdges(w, r, cfg.KnowledgeStore, log)
	})
	mux.HandleFunc("/knowledge/spread", func(w http.ResponseWriter, r *http.Request) {
		handleKnowledgeSpread(w, r, cfg.KnowledgeStore, log)
	})
	mux.HandleFunc("/knowledge/decay", func(w http.ResponseWriter, r *http.Request) {
		handleKnowledgeDecay(w, r, cfg.KnowledgeStore, log)
	})
	mux.HandleFunc("/knowledge/nodes", func(w http.ResponseWriter, r *http.Request) {
		handleKnowledgeNodes(w, r, cfg.KnowledgeStore, log)
	})
	mux.HandleFunc("/knowledge/stats", func(w http.ResponseWriter, r *http.Request) {
		handleKnowledgeStats(w, r, cfg.KnowledgeStore, log)
	})

	return mux
}

type ingestRequest struct {
	IndexID    string            `json:"index_id"`
	ShardID    string            `json:"shard_id"`
	DocumentID string            `json:"document_id"`
	Routing    map[string]string `json:"routing"`
	Payload    json.RawMessage   `json:"payload"`
}

func handleDocumentIngest(w http.ResponseWriter, r *http.Request, idx Indexer, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if idx == nil {
		http.Error(w, "indexer unavailable", http.StatusServiceUnavailable)
		return
	}

	defer r.Body.Close()
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	// Pass raw payload bytes directly - validation happens at worker level
	rawPayload := req.Payload
	if len(rawPayload) == 0 {
		rawPayload = []byte("{}")
	}

	cmd := document.Command{
		IndexID:    req.IndexID,
		ShardID:    req.ShardID,
		DocumentID: req.DocumentID,
		Routing:    req.Routing,
		RawPayload: rawPayload,
	}

	if err := idx.Index(r.Context(), cmd); err != nil {
		switch {
		case errors.Is(err, document.ErrInvalidCommand):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, document.ErrShardNotLoaded):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, document.ErrBackpressure):
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			http.Error(w, "failed to index document", http.StatusInternalServerError)
		}
		return
	}

	log.Info("document ingested",
		logger.Field{Key: "index_id", Value: req.IndexID},
		logger.Field{Key: "shard_id", Value: req.ShardID},
		logger.Field{Key: "document_id", Value: req.DocumentID},
	)

	w.WriteHeader(http.StatusOK)
}

// bulkIngestRequest matches the BulkIngestRequest from the documents package.
type bulkIngestRequest struct {
	Documents []bulkDocument `json:"documents"`
}

type bulkDocument struct {
	IndexID        string            `json:"index_id"`
	ShardID        string            `json:"shard_id"`
	DocumentID     string            `json:"document_id"`
	Routing        map[string]string `json:"routing,omitempty"`
	Payload        json.RawMessage   `json:"payload"`
	MappingVersion int               `json:"mapping_version"`
}

type bulkIngestResponse struct {
	Indexed int            `json:"indexed"`
	Errors  []bulkDocError `json:"errors,omitempty"`
}

type bulkDocError struct {
	DocumentID string `json:"document_id"`
	Error      string `json:"error"`
}

func handleBulkDocumentIngest(w http.ResponseWriter, r *http.Request, idx Indexer, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if idx == nil {
		http.Error(w, "indexer unavailable", http.StatusServiceUnavailable)
		return
	}

	defer r.Body.Close()
	var req bulkIngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	if len(req.Documents) == 0 {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(bulkIngestResponse{Indexed: 0})
		return
	}

	cmds := make([]document.Command, len(req.Documents))
	for i, doc := range req.Documents {
		rawPayload := doc.Payload
		if len(rawPayload) == 0 {
			rawPayload = []byte("{}")
		}
		cmds[i] = document.Command{
			IndexID:    doc.IndexID,
			ShardID:    doc.ShardID,
			DocumentID: doc.DocumentID,
			Routing:    doc.Routing,
			RawPayload: rawPayload,
		}
	}

	docErrors := idx.IndexBulk(r.Context(), cmds)

	response := bulkIngestResponse{}
	for i, err := range docErrors {
		if err != nil {
			response.Errors = append(response.Errors, bulkDocError{
				DocumentID: req.Documents[i].DocumentID,
				Error:      err.Error(),
			})
		} else {
			response.Indexed++
		}
	}

	log.Info("bulk documents ingested",
		logger.Field{Key: "indexed", Value: response.Indexed},
		logger.Field{Key: "errors", Value: len(response.Errors)},
		logger.Field{Key: "total", Value: len(req.Documents)},
	)

	w.Header().Set("Content-Type", "application/json")
	if len(response.Errors) > 0 && response.Indexed == 0 {
		w.WriteHeader(http.StatusBadRequest)
	} else if len(response.Errors) > 0 {
		w.WriteHeader(http.StatusMultiStatus)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(response)
}

func handleSearch(w http.ResponseWriter, r *http.Request, searcher Searcher, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if searcher == nil {
		http.Error(w, "search service unavailable", http.StatusServiceUnavailable)
		return
	}

	defer r.Body.Close()
	var req searchquery.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid search payload", http.StatusBadRequest)
		return
	}

	if err := req.Normalize(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// In standalone search node mode, shard_ids must be specified
	if len(req.ShardIDs) == 0 {
		http.Error(w, "shard_ids are required for search in standalone mode", http.StatusBadRequest)
		return
	}

	response, err := searcher.Search(r.Context(), req)
	if err != nil {
		log.Error("search execution failed", logger.Field{Key: "error", Value: err})
		http.Error(w, "search failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error("failed to encode search response", logger.Field{Key: "error", Value: err})
		http.Error(w, "encoding failed", http.StatusInternalServerError)
		return
	}

	log.Info("search executed",
		logger.Field{Key: "shard_count", Value: len(req.ShardIDs)},
		logger.Field{Key: "hits_total", Value: response.Total},
		logger.Field{Key: "hits_returned", Value: len(response.Hits)},
	)
}

type shardSyncRequest struct {
	Assignments []shards.Assignment `json:"assignments"`
}

func handleShardSync(w http.ResponseWriter, r *http.Request, syncer ShardSyncer, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if syncer == nil {
		http.Error(w, "shard syncer unavailable", http.StatusServiceUnavailable)
		return
	}

	defer r.Body.Close()
	var req shardSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	if len(req.Assignments) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := syncer.Sync(req.Assignments); err != nil {
		log.Error("shard sync failed", logger.Field{Key: "error", Value: err})
		http.Error(w, "sync failed", http.StatusInternalServerError)
		return
	}

	log.Info("synced shards from coordinator",
		logger.Field{Key: "count", Value: len(req.Assignments)},
	)

	w.WriteHeader(http.StatusOK)
}

type indexDeletedRequest struct {
	IndexID string `json:"index_id"`
}

func handleIndexDeleted(w http.ResponseWriter, r *http.Request, syncer ShardSyncer, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if syncer == nil {
		http.Error(w, "shard syncer unavailable", http.StatusServiceUnavailable)
		return
	}

	defer r.Body.Close()
	var req indexDeletedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	if req.IndexID == "" {
		http.Error(w, "index_id is required", http.StatusBadRequest)
		return
	}

	if err := syncer.UnloadIndex(req.IndexID); err != nil {
		log.Error("unload index failed",
			logger.Field{Key: "index_id", Value: req.IndexID},
			logger.Field{Key: "error", Value: err},
		)
		http.Error(w, "unload failed", http.StatusInternalServerError)
		return
	}

	log.Info("unloaded index",
		logger.Field{Key: "index_id", Value: req.IndexID},
	)

	w.WriteHeader(http.StatusOK)
}

// termLookupResponse is the response for term lookup APIs.
type termLookupResponse struct {
	Terms  []segment.TermEntry `json:"terms"`
	Total  int64               `json:"total,omitempty"`
	Limit  int                 `json:"limit"`
	Offset int                 `json:"offset,omitempty"`
}

// handleTermLookup handles term lookup requests.
// Query parameters:
//   - shard_id (required): The shard to query
//   - field (optional): Filter by field name
//   - limit (optional): Max results (default: 100, max: 1000)
//   - offset (optional): Pagination offset (default: 0)
//   - prefix (optional): Prefix search
//   - fuzzy (optional): Fuzzy search query
//   - distance (optional): Max Levenshtein distance for fuzzy (default: 1, max: 3)
//   - regex (optional): Regex pattern search
func handleTermLookup(w http.ResponseWriter, r *http.Request, lookup TermLookup, log logger.Logger) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if lookup == nil {
		http.Error(w, "term lookup unavailable", http.StatusServiceUnavailable)
		return
	}

	query := r.URL.Query()

	// Required: shard_id
	shardID := query.Get("shard_id")
	if shardID == "" {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}

	// Optional parameters
	field := query.Get("field")
	prefix := query.Get("prefix")
	fuzzy := query.Get("fuzzy")
	regex := query.Get("regex")

	// Parse limit (default: 100, max: 1000)
	limit := 100
	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > 1000 {
		limit = 1000
	}

	// Parse offset (default: 0)
	offset := 0
	if offsetStr := query.Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	// Parse distance for fuzzy (default: 1, max: 3)
	distance := 1
	if distStr := query.Get("distance"); distStr != "" {
		if d, err := strconv.Atoi(distStr); err == nil && d >= 1 && d <= 3 {
			distance = d
		}
	}

	ctx := r.Context()
	var terms []segment.TermEntry
	var total int64
	var err error

	// Determine which lookup to perform (priority: regex > fuzzy > prefix > list)
	switch {
	case regex != "":
		terms, err = lookup.ListTermsByRegex(ctx, shardID, field, regex, limit)
		if err != nil {
			log.Error("regex term lookup failed",
				logger.Field{Key: "shard_id", Value: shardID},
				logger.Field{Key: "regex", Value: regex},
				logger.Field{Key: "error", Value: err},
			)
			http.Error(w, "regex lookup failed: "+err.Error(), http.StatusBadRequest)
			return
		}
		total = int64(len(terms))

	case fuzzy != "":
		terms, err = lookup.ListTermsByFuzzy(ctx, shardID, field, fuzzy, distance, limit)
		if err != nil {
			log.Error("fuzzy term lookup failed",
				logger.Field{Key: "shard_id", Value: shardID},
				logger.Field{Key: "fuzzy", Value: fuzzy},
				logger.Field{Key: "error", Value: err},
			)
			http.Error(w, "fuzzy lookup failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		total = int64(len(terms))

	case prefix != "":
		terms, err = lookup.ListTermsByPrefix(ctx, shardID, field, prefix, limit)
		if err != nil {
			log.Error("prefix term lookup failed",
				logger.Field{Key: "shard_id", Value: shardID},
				logger.Field{Key: "prefix", Value: prefix},
				logger.Field{Key: "error", Value: err},
			)
			http.Error(w, "prefix lookup failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		total = int64(len(terms))

	default:
		terms, total, err = lookup.ListTerms(ctx, shardID, field, limit, offset)
		if err != nil {
			log.Error("term list failed",
				logger.Field{Key: "shard_id", Value: shardID},
				logger.Field{Key: "error", Value: err},
			)
			http.Error(w, "term list failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	response := termLookupResponse{
		Terms:  terms,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}

	log.Info("term lookup executed",
		logger.Field{Key: "shard_id", Value: shardID},
		logger.Field{Key: "field", Value: field},
		logger.Field{Key: "results", Value: len(terms)},
	)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error("failed to encode term lookup response", logger.Field{Key: "error", Value: err})
		http.Error(w, "encoding failed", http.StatusInternalServerError)
		return
	}
}

// --- Knowledge graph handlers (Type B) ---

// handleKnowledgeGraphs handles CRUD for knowledge graphs.
// POST: create graph (query: name)
// DELETE: delete graph (query: name)
// GET: list all graphs
func handleKnowledgeGraphs(w http.ResponseWriter, r *http.Request, store KnowledgeStore, log logger.Logger) {
	if store == nil {
		http.Error(w, "knowledge store unavailable", http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodGet:
		names := store.List()
		writeJSON(w, http.StatusOK, map[string]any{
			"graphs": names,
		})

	case http.MethodPost:
		name := r.URL.Query().Get("name")
		if strings.TrimSpace(name) == "" {
			http.Error(w, "name is required", http.StatusBadRequest)
			return
		}
		if _, err := store.GetOrCreate(name); err != nil {
			http.Error(w, "failed to create graph: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{
			"name":   name,
			"status": "created",
		})

	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if name == "" {
			http.Error(w, "name is required", http.StatusBadRequest)
			return
		}
		if err := store.Delete(name); err != nil {
			http.Error(w, "failed to delete graph: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleKnowledgeEdges handles edge operations.
// POST: add edge (query: graph; body: {node_a, node_b, weight, type, source})
// GET: get edges (query: graph, node, direction, min_weight)
// DELETE: delete edge (query: graph, node_a, node_b)
func handleKnowledgeEdges(w http.ResponseWriter, r *http.Request, store KnowledgeStore, log logger.Logger) {
	if store == nil {
		http.Error(w, "knowledge store unavailable", http.StatusServiceUnavailable)
		return
	}

	query := r.URL.Query()
	graphName := query.Get("graph")
	if graphName == "" {
		http.Error(w, "graph is required", http.StatusBadRequest)
		return
	}

	graph, ok := store.Get(graphName)
	if !ok {
		http.Error(w, "graph not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPost:
		defer r.Body.Close()
		var req struct {
			NodeA    string  `json:"node_a"`
			NodeB    string  `json:"node_b"`
			Weight   float64 `json:"weight"`
			EdgeType string  `json:"type,omitempty"`
			Source   string  `json:"source,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.NodeA == "" || req.NodeB == "" {
			http.Error(w, "node_a and node_b are required", http.StatusBadRequest)
			return
		}
		if req.Weight <= 0 {
			req.Weight = 1.0
		}
		err := graph.AddEdge(req.NodeA, req.NodeB, knowledge.EdgeData{
			Weight:     req.Weight,
			EdgeType:   req.EdgeType,
			Generation: 0,
			Source:     req.Source,
		})
		if err != nil {
			http.Error(w, "failed to add edge: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)

	case http.MethodGet:
		node := query.Get("node")
		if node == "" {
			http.Error(w, "node is required", http.StatusBadRequest)
			return
		}
		minWeight := 0.0
		if mw := query.Get("min_weight"); mw != "" {
			if v, err := strconv.ParseFloat(mw, 64); err == nil {
				minWeight = v
			}
		}
		direction := query.Get("direction")
		var edges []knowledge.Edge
		switch direction {
		case "in":
			edges = graph.GetIncomingEdges(node, minWeight)
		case "both":
			edges = append(graph.GetEdges(node, minWeight), graph.GetIncomingEdges(node, minWeight)...)
		default:
			edges = graph.GetEdges(node, minWeight)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"node":  node,
			"edges": edges,
			"count": len(edges),
		})

	case http.MethodDelete:
		nodeA := query.Get("node_a")
		nodeB := query.Get("node_b")
		if nodeA == "" || nodeB == "" {
			http.Error(w, "node_a and node_b are required", http.StatusBadRequest)
			return
		}
		if err := graph.DeleteEdge(nodeA, nodeB); err != nil {
			http.Error(w, "failed to delete edge: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleKnowledgeSpread handles graph traversal.
// GET: spread from node (query: graph, node, hops, decay)
func handleKnowledgeSpread(w http.ResponseWriter, r *http.Request, store KnowledgeStore, log logger.Logger) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if store == nil {
		http.Error(w, "knowledge store unavailable", http.StatusServiceUnavailable)
		return
	}

	query := r.URL.Query()
	graphName := query.Get("graph")
	if graphName == "" {
		http.Error(w, "graph is required", http.StatusBadRequest)
		return
	}

	graph, ok := store.Get(graphName)
	if !ok {
		http.Error(w, "graph not found", http.StatusNotFound)
		return
	}

	node := query.Get("node")
	if node == "" {
		http.Error(w, "node is required", http.StatusBadRequest)
		return
	}

	hops := 2
	if h := query.Get("hops"); h != "" {
		if v, err := strconv.Atoi(h); err == nil && v > 0 && v <= 10 {
			hops = v
		}
	}

	decay := 0.7
	if d := query.Get("decay"); d != "" {
		if v, err := strconv.ParseFloat(d, 64); err == nil && v > 0 && v <= 1 {
			decay = v
		}
	}

	result := graph.Spread(node, hops, decay)
	writeJSON(w, http.StatusOK, map[string]any{
		"node":   node,
		"hops":   hops,
		"decay":  decay,
		"spread": result,
	})
}

// handleKnowledgeDecay handles edge weight decay.
// POST: decay edges (query: graph; body: {factor})
func handleKnowledgeDecay(w http.ResponseWriter, r *http.Request, store KnowledgeStore, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if store == nil {
		http.Error(w, "knowledge store unavailable", http.StatusServiceUnavailable)
		return
	}

	query := r.URL.Query()
	graphName := query.Get("graph")
	if graphName == "" {
		http.Error(w, "graph is required", http.StatusBadRequest)
		return
	}

	graph, ok := store.Get(graphName)
	if !ok {
		http.Error(w, "graph not found", http.StatusNotFound)
		return
	}

	defer r.Body.Close()
	var req struct {
		Factor float64 `json:"factor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Factor <= 0 || req.Factor >= 1 {
		http.Error(w, "factor must be between 0 and 1 (exclusive)", http.StatusBadRequest)
		return
	}

	if err := graph.DecayEdges(req.Factor); err != nil {
		http.Error(w, "failed to decay edges: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleKnowledgeNodes handles node metadata operations.
// PUT: set node (query: graph, node_id; body: {label, type, metadata})
// GET: get node or list nodes (query: graph, node_id or prefix, limit)
// DELETE: delete node (query: graph, node_id)
func handleKnowledgeNodes(w http.ResponseWriter, r *http.Request, store KnowledgeStore, log logger.Logger) {
	if store == nil {
		http.Error(w, "knowledge store unavailable", http.StatusServiceUnavailable)
		return
	}

	query := r.URL.Query()
	graphName := query.Get("graph")
	if graphName == "" {
		http.Error(w, "graph is required", http.StatusBadRequest)
		return
	}

	graph, ok := store.Get(graphName)
	if !ok {
		http.Error(w, "graph not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		nodeID := query.Get("node_id")
		if nodeID == "" {
			http.Error(w, "node_id is required", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req struct {
			Label    string            `json:"label,omitempty"`
			Type     string            `json:"type,omitempty"`
			Metadata map[string]string `json:"metadata,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		err := graph.SetNode(nodeID, knowledge.NodeData{
			Label:    req.Label,
			Type:     req.Type,
			Metadata: req.Metadata,
		})
		if err != nil {
			http.Error(w, "failed to set node: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		nodeID := query.Get("node_id")
		if nodeID != "" {
			data, found := graph.GetNode(nodeID)
			if !found {
				http.Error(w, "node not found", http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"node_id":  nodeID,
				"label":    data.Label,
				"type":     data.Type,
				"metadata": data.Metadata,
			})
		} else {
			prefix := query.Get("prefix")
			limit := 100
			if l := query.Get("limit"); l != "" {
				if v, err := strconv.Atoi(l); err == nil && v > 0 {
					limit = v
				}
			}
			nodes := graph.ListNodes(prefix, limit)
			writeJSON(w, http.StatusOK, map[string]any{
				"nodes": nodes,
				"count": len(nodes),
			})
		}

	case http.MethodDelete:
		nodeID := query.Get("node_id")
		if nodeID == "" {
			http.Error(w, "node_id is required", http.StatusBadRequest)
			return
		}
		if err := graph.DeleteNode(nodeID); err != nil {
			http.Error(w, "failed to delete node: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleKnowledgeStats returns stats for a knowledge graph.
// GET: stats (query: graph)
func handleKnowledgeStats(w http.ResponseWriter, r *http.Request, store KnowledgeStore, log logger.Logger) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if store == nil {
		http.Error(w, "knowledge store unavailable", http.StatusServiceUnavailable)
		return
	}

	graphName := r.URL.Query().Get("graph")
	if graphName == "" {
		http.Error(w, "graph is required", http.StatusBadRequest)
		return
	}

	graph, ok := store.Get(graphName)
	if !ok {
		http.Error(w, "graph not found", http.StatusNotFound)
		return
	}

	stats := graph.Stats()
	writeJSON(w, http.StatusOK, stats)
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
