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
	"strings"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"plastic-engine-core/internal/core/cluster"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/shards"
	searchquery "plastic-engine-core/internal/core/search/query"
)

var tracer = otel.Tracer("cluster/http/search")

// Mount registers the search endpoint exposed by the cluster.
func Mount(r chi.Router, coord *cluster.Coordinator, httpClient *http.Client) {
	handler := &handler{
		coord:      coord,
		httpClient: httpClient,
	}

	r.Post("/search", handler.handleSearch)
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
