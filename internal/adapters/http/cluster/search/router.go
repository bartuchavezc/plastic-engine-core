package clustersearch

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	corehttputil "plastic-engine-core/internal/adapters/http/cluster/httputil"
	coordinator "plastic-engine-core/internal/core/cluster/coordinator"
	coreindex "plastic-engine-core/internal/core/index"
	searchquery "plastic-engine-core/internal/core/search/query"
)

var tracer = otel.Tracer("cluster/http/search")

// Mount registers the search endpoint exposed by the coordinator.
func Mount(r chi.Router, coord *coordinator.Coordinator) {
	handler := &handler{
		coord: coord,
	}

	r.Post("/search", handler.handleSearch)
}

type handler struct {
	coord *coordinator.Coordinator
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

	definition, err := h.coord.GetIndex(ctx, req.IndexID)
	if err != nil {
		if errors.Is(err, coreindex.ErrIndexNotFound) {
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

	// TODO(bypasscash): derive shard routing from request filters once mapping-driven shard keys are in place.
	corehttputil.WriteJSON(w, http.StatusNotImplemented, map[string]any{
		"message":     "search execution not implemented yet",
		"index":       definition.ID,
		"query_boost": h.collectBoostInfo(req),
	})
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
