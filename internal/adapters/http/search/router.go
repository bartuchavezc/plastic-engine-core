package searchhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"plastic-engine-core/internal/core/search/document"
	searchquery "plastic-engine-core/internal/core/search/query"
	"plastic-engine-core/internal/core/search/segment"
	"plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// Indexer defines the contract required to index documents.
type Indexer interface {
	Index(ctx context.Context, cmd document.Command) error
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

// RouterConfig holds configuration for creating a Router.
type RouterConfig struct {
	Indexer     Indexer
	Searcher    Searcher
	ShardSyncer ShardSyncer
	TermLookup  TermLookup
	Logger      logger.Logger
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

	w.WriteHeader(http.StatusAccepted)
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

	response := bulkIngestResponse{}

	for _, doc := range req.Documents {
		// Pass raw payload bytes directly - validation happens at worker level
		rawPayload := doc.Payload
		if len(rawPayload) == 0 {
			rawPayload = []byte("{}")
		}

		cmd := document.Command{
			IndexID:    doc.IndexID,
			ShardID:    doc.ShardID,
			DocumentID: doc.DocumentID,
			Routing:    doc.Routing,
			RawPayload: rawPayload,
		}

		if err := idx.Index(r.Context(), cmd); err != nil {
			response.Errors = append(response.Errors, bulkDocError{
				DocumentID: doc.DocumentID,
				Error:      err.Error(),
			})
			continue
		}

		response.Indexed++
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
		w.WriteHeader(http.StatusAccepted)
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
