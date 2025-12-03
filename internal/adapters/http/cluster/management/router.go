package clustermanagement

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"

	clusterhttputil "plastic-engine-core/internal/adapters/http/cluster/httputil"
	"plastic-engine-core/internal/core/cluster"
)

var tracer = otel.Tracer("cluster/http/management")

// Mount registers cluster management endpoints such as node and shard listings.
func Mount(r chi.Router, coord *cluster.Coordinator) {
	handler := &handler{
		coord: coord,
	}

	r.Get("/cluster/management/nodes", handler.handleListNodes)
	r.Get("/cluster/management/shards", handler.handleListShards)
	r.Get("/indexes/{id}/shards", handler.handleListShardsForIndex)
}

type handler struct {
	coord *cluster.Coordinator
}

func (h *handler) handleListNodes(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.management.listNodes")
	defer span.End()

	records, err := h.coord.ListNodes(ctx)
	if err != nil {
		http.Error(w, "failed to list nodes", http.StatusInternalServerError)
		return
	}

	response := make([]nodeResponse, 0, len(records))
	for _, node := range records {
		resp := nodeResponse{
			ID:            node.ID,
			Role:          node.Role,
			AdvertiseAddr: node.AdvertiseAddr,
			DataDir:       node.DataDir,
			Status:        node.Status,
		}
		if !node.LastHeartbeat.IsZero() {
			ts := node.LastHeartbeat
			resp.LastHeartbeat = &ts
		}

		response = append(response, resp)
	}

	clusterhttputil.WriteJSON(w, http.StatusOK, response)
}

func (h *handler) handleListShards(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.management.listShards")
	defer span.End()

	filter := cluster.ShardFilter{
		IndexID: strings.TrimSpace(r.URL.Query().Get("index_id")),
		NodeID:  strings.TrimSpace(r.URL.Query().Get("node_id")),
		State:   strings.TrimSpace(r.URL.Query().Get("state")),
	}

	h.respondWithShards(ctx, w, filter)
}

func (h *handler) handleListShardsForIndex(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.management.listShardsForIndex")
	defer span.End()

	indexID := strings.TrimSpace(chi.URLParam(r, "id"))
	if indexID == "" {
		http.Error(w, "missing index id in path", http.StatusBadRequest)
		return
	}

	filter := cluster.ShardFilter{
		IndexID: indexID,
		NodeID:  strings.TrimSpace(r.URL.Query().Get("node_id")),
		State:   strings.TrimSpace(r.URL.Query().Get("state")),
	}

	h.respondWithShards(ctx, w, filter)
}

func (h *handler) respondWithShards(ctx context.Context, w http.ResponseWriter, filter cluster.ShardFilter) {
	shards, err := h.coord.ListShards(ctx, filter)
	if err != nil {
		http.Error(w, "failed to list shards", http.StatusInternalServerError)
		return
	}

	response := make([]shardResponse, 0, len(shards))
	for _, shard := range shards {
		response = append(response, shardResponse{
			ID:          shard.ID,
			IndexID:     shard.IndexID,
			ShardKey:    shard.ShardKey,
			PrimaryNode: shard.PrimaryNode,
			State:       shard.State,
			Version:     shard.Version,
			CreatedAt:   shard.CreatedAt,
			UpdatedAt:   shard.UpdatedAt,
		})
	}

	clusterhttputil.WriteJSON(w, http.StatusOK, response)
}

type nodeResponse struct {
	ID            string     `json:"id"`
	Role          string     `json:"role"`
	AdvertiseAddr string     `json:"advertise_addr,omitempty"`
	DataDir       string     `json:"data_dir,omitempty"`
	Status        string     `json:"status"`
	LastHeartbeat *time.Time `json:"last_heartbeat,omitempty"`
}

type shardResponse struct {
	ID          string    `json:"id"`
	IndexID     string    `json:"index_id"`
	ShardKey    string    `json:"shard_key"`
	PrimaryNode string    `json:"primary_node,omitempty"`
	State       string    `json:"state"`
	Version     int64     `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
