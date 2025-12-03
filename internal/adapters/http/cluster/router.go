package clusterhttp

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	clusterindex "plastic-engine-core/internal/adapters/http/cluster/index"
	clustermanagement "plastic-engine-core/internal/adapters/http/cluster/management"
	clustersearchhttp "plastic-engine-core/internal/adapters/http/cluster/search"
	coordinator "plastic-engine-core/internal/core/cluster/coordinator"
	clustersearchsvc "plastic-engine-core/internal/core/cluster/search"
)

// NewRouter exposes coordinator cluster operations via HTTP.
func NewRouter(coord *coordinator.Coordinator, joinService *clustersearchsvc.JoinService) http.Handler {
	r := chi.NewRouter()

	clusterindex.Mount(r, coord)
	clustermanagement.Mount(r, coord)
	r.Post("/cluster/join", func(w http.ResponseWriter, req *http.Request) {
		handleJoin(w, req, joinService)
	})
	r.Post("/cluster/heartbeat", func(w http.ResponseWriter, req *http.Request) {
		handleHeartbeat(w, req, joinService)
	})
	clustersearchhttp.Mount(r, coord)
	r.Mount("/debug", middleware.Profiler())

	return r
}

var membershipTracer = otel.Tracer("cluster/http/membership")

func handleJoin(w http.ResponseWriter, r *http.Request, joinService *clustersearchsvc.JoinService) {
	ctx, span := membershipTracer.Start(r.Context(), "cluster.membership.join")
	defer span.End()

	var req coordinator.JoinRequest
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

func handleHeartbeat(w http.ResponseWriter, r *http.Request, joinService *clustersearchsvc.JoinService) {
	ctx, span := membershipTracer.Start(r.Context(), "cluster.membership.heartbeat")
	defer span.End()

	var req coordinator.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid heartbeat payload", http.StatusBadRequest)
		return
	}

	span.SetAttributes(
		attribute.String("cluster.node_id", req.NodeID),
	)

	if err := joinService.Heartbeat(ctx, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
