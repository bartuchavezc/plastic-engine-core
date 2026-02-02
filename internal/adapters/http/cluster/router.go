package clusterhttp

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	clusterindex "plastic-engine-core/internal/adapters/http/cluster/index"
	clustermanagement "plastic-engine-core/internal/adapters/http/cluster/management"
	clustersearchhttp "plastic-engine-core/internal/adapters/http/cluster/search"
	"plastic-engine-core/internal/core/cluster"
	"plastic-engine-core/internal/core/cluster/nodes"
)

// NewRouter exposes coordinator cluster operations via HTTP.
func NewRouter(coord *cluster.Coordinator, joinService *nodes.JoinService) http.Handler {
	r := chi.NewRouter()

	// Create HTTP client for communication with nodes
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	clusterindex.Mount(r, coord)
	clustermanagement.Mount(r, coord)
	r.Post("/cluster/join", func(w http.ResponseWriter, req *http.Request) {
		handleJoin(w, req, joinService)
	})
	r.Post("/cluster/heartbeat", func(w http.ResponseWriter, req *http.Request) {
		handleHeartbeat(w, req, joinService)
	})
	clustersearchhttp.Mount(r, coord, httpClient)
	r.Mount("/debug", middleware.Profiler())

	return r
}

var membershipTracer = otel.Tracer("cluster/http/membership")

func handleJoin(w http.ResponseWriter, r *http.Request, joinService *nodes.JoinService) {
	ctx, span := membershipTracer.Start(r.Context(), "cluster.membership.join")
	defer span.End()

	var req nodes.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid join payload", http.StatusBadRequest)
		return
	}

	span.SetAttributes(
		attribute.String("cluster.role", req.Role),
	)

	resp, err := joinService.Join(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request, joinService *nodes.JoinService) {
	ctx, span := membershipTracer.Start(r.Context(), "cluster.membership.heartbeat")
	defer span.End()

	var req nodes.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid heartbeat payload", http.StatusBadRequest)
		return
	}

	span.SetAttributes(
		attribute.String("cluster.node_id", req.NodeID),
	)

	resp, err := joinService.Heartbeat(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Return response with mapping updates if any
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}
