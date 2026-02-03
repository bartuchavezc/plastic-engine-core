package clustersearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"plastic-engine-core/internal/core/cluster"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/shards"
	searchquery "plastic-engine-core/internal/core/search/query"
	"plastic-engine-core/internal/core/search/segment"
)

var tracer = otel.Tracer("cluster/http/search")

// Mount registers the search endpoint exposed by the cluster.
func Mount(r chi.Router, coord *cluster.Coordinator, httpClient *http.Client) {
	handler := &handler{
		coord:      coord,
		httpClient: httpClient,
	}

	r.Post("/search", handler.handleSearch)
	r.Get("/terms", handler.handleTermLookup)
}

type handler struct {
	coord      *cluster.Coordinator
	httpClient *http.Client
}

func (h *handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.search")
	defer span.End()

	var req searchquery.Request
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid search payload", http.StatusBadRequest)
		return
	}

	if err := req.Normalize(); err != nil {
		h.respondValidationError(w, err)
		return
	}

	if err := req.Validate(); err != nil {
		h.respondValidationError(w, err)
		return
	}

	// Verify index exists
	_, err := h.coord.GetIndex(ctx, req.IndexID)
	if err != nil {
		if errors.Is(err, indexes.ErrIndexNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, "failed to resolve index", http.StatusInternalServerError)
		return
	}

	span.SetAttributes(
		attribute.String("search.index_id", req.IndexID),
		attribute.Int("search.limit", req.Limit),
	)

	// TODO(bypasscash): integrate SearchService for full search execution
	shards, err := h.coord.ListShards(ctx, shards.ShardFilter{IndexID: req.IndexID})
	if err != nil {
		http.Error(w, "failed to get shards", http.StatusInternalServerError)
		return
	}

	if len(shards) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(searchquery.Response{Hits: []searchquery.Hit{}, Total: 0})
		return
	}

	nodeRequests := h.groupShardsByNode(shards)
	results := h.executeDistributedSearch(ctx, nodeRequests, req)
	response := h.mergeResults(results, req.Limit)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (h *handler) respondValidationError(w http.ResponseWriter, err error) {
	var validationErr *searchquery.ValidationError
	if errors.As(err, &validationErr) {
		http.Error(w, validationErr.Error(), http.StatusBadRequest)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func (h *handler) collectBoostInfo(req searchquery.Request) map[string]float64 {
	boosts := make(map[string]float64)

	addBoosts := func(clause searchquery.Clause) {
		if clause.Match != nil && clause.Match.Boost > 0 {
			boosts[clause.Match.Field] = clause.Match.Boost
		}
	}

	addBoosts(req.Query)

	for _, clause := range req.Filters {
		addBoosts(clause)
	}

	return boosts
}

func (h *handler) groupShardsByNode(shards []shards.ShardRecord) map[string][]string {
	nodeShards := make(map[string][]string)

	for _, shard := range shards {
		if shard.PrimaryNode == "" {
			continue
		}

		nodeInfo, err := h.coord.ShardsService().LookupNode(context.Background(), shard.PrimaryNode)
		if err != nil {
			continue
		}

		nodeShards[nodeInfo.AdvertiseAddr] = append(nodeShards[nodeInfo.AdvertiseAddr], shard.ID)
	}

	return nodeShards
}

func (h *handler) executeDistributedSearch(ctx context.Context, nodeRequests map[string][]string, originalReq searchquery.Request) []searchquery.Response {
	type searchResult struct {
		response searchquery.Response
		err      error
	}

	results := make([]searchquery.Response, 0, len(nodeRequests))
	resultCh := make(chan searchResult, len(nodeRequests))

	for nodeAddr, shardIDs := range nodeRequests {
		go func(addr string, shards []string) {
			req := originalReq
			req.ShardIDs = shards
			req.IndexID = ""

			resp, err := h.searchSingleNode(ctx, addr, req)
			resultCh <- searchResult{response: resp, err: err}
		}(nodeAddr, shardIDs)
	}

	for i := 0; i < len(nodeRequests); i++ {
		result := <-resultCh
		if result.err != nil {
			continue
		}
		results = append(results, result.response)
	}

	return results
}

// searchSingleNode executes search on a single node via HTTP direct request.
func (h *handler) searchSingleNode(ctx context.Context, nodeAddr string, req searchquery.Request) (searchquery.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return searchquery.Response{}, fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("http://%s/search", strings.TrimPrefix(nodeAddr, "http://"))
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return searchquery.Response{}, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(httpReq)
	if err != nil {
		return searchquery.Response{}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return searchquery.Response{}, fmt.Errorf("search failed (%d): %s", resp.StatusCode, string(body))
	}

	var result searchquery.Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return searchquery.Response{}, fmt.Errorf("decode response: %w", err)
	}

	return result, nil
}

func (h *handler) mergeResults(results []searchquery.Response, limit int) searchquery.Response {
	var allHits []searchquery.Hit
	var total int64

	for _, result := range results {
		allHits = append(allHits, result.Hits...)
		total += result.Total
	}

	// Sort by score descending
	sort.Slice(allHits, func(i, j int) bool {
		return allHits[i].Score > allHits[j].Score
	})

	// Apply global limit
	if limit > 0 && len(allHits) > limit {
		allHits = allHits[:limit]
	}

	return searchquery.Response{
		Hits:  allHits,
		Total: total,
	}
}

// termLookupRequest represents the term lookup query parameters.
type termLookupRequest struct {
	IndexID   string
	Field     string
	Prefix    string
	Fuzzy     string
	Distance  int
	Regex     string
	Limit     int
	Offset    int
	StartTime *time.Time // For date-based shard filtering
	EndTime   *time.Time // For date-based shard filtering
}

// termLookupResponse is the aggregated response from term lookup.
type termLookupResponse struct {
	Terms      []segment.TermEntry `json:"terms"`
	Total      int64               `json:"total"`
	Limit      int                 `json:"limit"`
	Offset     int                 `json:"offset,omitempty"`
	ShardCount int                 `json:"shard_count"`
}

// handleTermLookup handles term lookup requests with intelligent shard routing.
// Query parameters:
//   - index_id (required): The index to query
//   - field (optional): Filter by field name
//   - limit (optional): Max results per shard (default: 100, max: 1000)
//   - offset (optional): Pagination offset (default: 0) - only for list mode
//   - prefix (optional): Prefix search
//   - fuzzy (optional): Fuzzy search query
//   - distance (optional): Max Levenshtein distance for fuzzy (default: 1, max: 3)
//   - regex (optional): Regex pattern search
//   - start_time (optional): Start time for date-based shard filtering (RFC3339)
//   - end_time (optional): End time for date-based shard filtering (RFC3339)
func (h *handler) handleTermLookup(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.term_lookup")
	defer span.End()

	query := r.URL.Query()

	// Required: index_id
	indexID := query.Get("index_id")
	if indexID == "" {
		http.Error(w, "index_id is required", http.StatusBadRequest)
		return
	}

	// Get index definition for sharding strategy
	def, err := h.coord.GetIndex(ctx, indexID)
	if err != nil {
		if errors.Is(err, indexes.ErrIndexNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, "failed to resolve index", http.StatusInternalServerError)
		return
	}

	// Parse request parameters
	req := termLookupRequest{
		IndexID:  indexID,
		Field:    query.Get("field"),
		Prefix:   query.Get("prefix"),
		Fuzzy:    query.Get("fuzzy"),
		Regex:    query.Get("regex"),
		Limit:    100,
		Offset:   0,
		Distance: 1,
	}

	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			req.Limit = l
		}
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}

	if offsetStr := query.Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			req.Offset = o
		}
	}

	if distStr := query.Get("distance"); distStr != "" {
		if d, err := strconv.Atoi(distStr); err == nil && d >= 1 && d <= 3 {
			req.Distance = d
		}
	}

	// Parse time filters for date-based sharding
	if startStr := query.Get("start_time"); startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			req.StartTime = &t
		}
	}
	if endStr := query.Get("end_time"); endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			req.EndTime = &t
		}
	}

	span.SetAttributes(
		attribute.String("term_lookup.index_id", indexID),
		attribute.String("term_lookup.field", req.Field),
		attribute.Int("term_lookup.limit", req.Limit),
	)

	// Get shards for this index
	allShards, err := h.coord.ListShards(ctx, shards.ShardFilter{IndexID: indexID})
	if err != nil {
		http.Error(w, "failed to get shards", http.StatusInternalServerError)
		return
	}

	if len(allShards) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(termLookupResponse{Terms: []segment.TermEntry{}, Total: 0, ShardCount: 0})
		return
	}

	// Filter shards based on sharding strategy and time range
	filteredShards := h.filterShardsByTimeRange(allShards, def, req)

	span.SetAttributes(
		attribute.Int("term_lookup.total_shards", len(allShards)),
		attribute.Int("term_lookup.filtered_shards", len(filteredShards)),
	)

	// Group shards by node
	nodeRequests := h.groupShardsByNode(filteredShards)

	// Execute distributed term lookup
	results := h.executeDistributedTermLookup(ctx, nodeRequests, req)

	// Merge and deduplicate results
	response := h.mergeTermResults(results, req.Limit, len(filteredShards))

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

// filterShardsByTimeRange filters shards based on date-based sharding strategy.
func (h *handler) filterShardsByTimeRange(allShards []shards.ShardRecord, def indexes.IndexDefinition, req termLookupRequest) []shards.ShardRecord {
	// If not date-based sharding or no time filters, return all shards
	if def.ShardStrategy != indexes.ShardStrategyDate {
		return allShards
	}
	if req.StartTime == nil && req.EndTime == nil {
		return allShards
	}

	var filtered []shards.ShardRecord
	for _, shard := range allShards {
		// ShardKey for date strategy is like "2024-01" or "2024-01-15"
		if h.shardKeyInTimeRange(shard.ShardKey, req.StartTime, req.EndTime) {
			filtered = append(filtered, shard)
		}
	}

	// If no shards match the filter, return all (fail-safe)
	if len(filtered) == 0 {
		return allShards
	}

	return filtered
}

// shardKeyInTimeRange checks if a shard key falls within the time range.
func (h *handler) shardKeyInTimeRange(shardKey string, startTime, endTime *time.Time) bool {
	// Parse shard key as date (supports: 2024, 2024-01, 2024-01-15)
	var shardStart, shardEnd time.Time
	var err error

	switch len(shardKey) {
	case 4: // Year: 2024
		shardStart, err = time.Parse("2006", shardKey)
		if err != nil {
			return true // Can't parse, include shard
		}
		shardEnd = shardStart.AddDate(1, 0, 0)
	case 7: // Month: 2024-01
		shardStart, err = time.Parse("2006-01", shardKey)
		if err != nil {
			return true
		}
		shardEnd = shardStart.AddDate(0, 1, 0)
	case 10: // Day: 2024-01-15
		shardStart, err = time.Parse("2006-01-02", shardKey)
		if err != nil {
			return true
		}
		shardEnd = shardStart.AddDate(0, 0, 1)
	default:
		return true // Unknown format, include shard
	}

	// Check overlap with requested time range
	if startTime != nil && shardEnd.Before(*startTime) {
		return false
	}
	if endTime != nil && shardStart.After(*endTime) {
		return false
	}

	return true
}

// executeDistributedTermLookup executes term lookup across multiple nodes.
func (h *handler) executeDistributedTermLookup(ctx context.Context, nodeRequests map[string][]string, req termLookupRequest) []termNodeResponse {
	type lookupResult struct {
		response termNodeResponse
		err      error
	}

	results := make([]termNodeResponse, 0, len(nodeRequests))
	resultCh := make(chan lookupResult, len(nodeRequests))

	for nodeAddr, shardIDs := range nodeRequests {
		go func(addr string, shards []string) {
			resp, err := h.termLookupSingleNode(ctx, addr, shards, req)
			resultCh <- lookupResult{response: resp, err: err}
		}(nodeAddr, shardIDs)
	}

	for i := 0; i < len(nodeRequests); i++ {
		result := <-resultCh
		if result.err != nil {
			continue
		}
		results = append(results, result.response)
	}

	return results
}

// termNodeResponse is the response from a single node's term lookup.
type termNodeResponse struct {
	Terms []segment.TermEntry
	Total int64
}

// termLookupSingleNode executes term lookup on a single node.
func (h *handler) termLookupSingleNode(ctx context.Context, nodeAddr string, shardIDs []string, req termLookupRequest) (termNodeResponse, error) {
	var allTerms []segment.TermEntry
	var totalCount int64

	// Query each shard on this node
	for _, shardID := range shardIDs {
		terms, total, err := h.termLookupSingleShard(ctx, nodeAddr, shardID, req)
		if err != nil {
			continue // Skip failed shards
		}
		allTerms = append(allTerms, terms...)
		totalCount += total
	}

	return termNodeResponse{Terms: allTerms, Total: totalCount}, nil
}

// termLookupSingleShard executes term lookup on a single shard via HTTP.
func (h *handler) termLookupSingleShard(ctx context.Context, nodeAddr, shardID string, req termLookupRequest) ([]segment.TermEntry, int64, error) {
	// Build query string
	url := fmt.Sprintf("http://%s/terms?shard_id=%s&limit=%d",
		strings.TrimPrefix(nodeAddr, "http://"),
		shardID,
		req.Limit,
	)

	if req.Field != "" {
		url += "&field=" + req.Field
	}
	if req.Prefix != "" {
		url += "&prefix=" + req.Prefix
	}
	if req.Fuzzy != "" {
		url += fmt.Sprintf("&fuzzy=%s&distance=%d", req.Fuzzy, req.Distance)
	}
	if req.Regex != "" {
		url += "&regex=" + req.Regex
	}
	if req.Offset > 0 {
		url += fmt.Sprintf("&offset=%d", req.Offset)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	resp, err := h.httpClient.Do(httpReq)
	if err != nil {
		return nil, 0, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("term lookup failed (%d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		Terms  []segment.TermEntry `json:"terms"`
		Total  int64               `json:"total"`
		Limit  int                 `json:"limit"`
		Offset int                 `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("decode response: %w", err)
	}

	return result.Terms, result.Total, nil
}

// mergeTermResults merges and deduplicates term results from multiple shards.
func (h *handler) mergeTermResults(results []termNodeResponse, limit int, shardCount int) termLookupResponse {
	// Use map to deduplicate by field+term
	termMap := make(map[string]segment.TermEntry)
	var totalDF int64

	for _, result := range results {
		for _, entry := range result.Terms {
			key := entry.Field + ":" + entry.Term
			if existing, ok := termMap[key]; ok {
				// Aggregate DF across shards
				existing.DF += entry.DF
				termMap[key] = existing
			} else {
				termMap[key] = entry
			}
		}
		totalDF += result.Total
	}

	// Convert map to slice
	terms := make([]segment.TermEntry, 0, len(termMap))
	for _, entry := range termMap {
		terms = append(terms, entry)
	}

	// Sort by field, then term
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].Field != terms[j].Field {
			return terms[i].Field < terms[j].Field
		}
		return terms[i].Term < terms[j].Term
	})

	// Apply limit
	if limit > 0 && len(terms) > limit {
		terms = terms[:limit]
	}

	return termLookupResponse{
		Terms:      terms,
		Total:      int64(len(termMap)),
		Limit:      limit,
		ShardCount: shardCount,
	}
}
