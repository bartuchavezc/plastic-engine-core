package cluster

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"plastic-engine-core/internal/core/cluster/shards"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/indexes/sharding"
	"plastic-engine-core/internal/pkg/logger"

	"go.opentelemetry.io/otel"
)

// Request represents an ingestion command accepted by the cluster.
type Request struct {
	IndexID    string
	DocumentID string
	Routing    map[string]string
	Payload    json.RawMessage
}

// ErrShardNotFound signals the coordinator could not route the document.
var ErrShardNotFound = errors.New("shard not found for routing metadata")

// Handler encapsulates the dependencies required to execute the ingestion workflow.
type Handler struct {
	IndexRepo  IndexRepository
	DB         *sql.DB
	HTTPClient *http.Client
	Logger     logger.Logger
}

// IndexRepository resolves index definitions.
type IndexRepository interface {
	GetIndex(ctx context.Context, id string) (indexes.IndexDefinition, error)
}

// Handle executes the ingestion pipeline using the coordinator's collaborators.
func (h *Handler) Handle(ctx context.Context, req Request) error {
	ctx, span := otel.Tracer("cluster.ingest").Start(ctx, "Coordinator.IngestDocument")
	defer span.End()

	if h.IndexRepo == nil {
		return fmt.Errorf("ingest handler missing index repository")
	}
	if h.DB == nil {
		return fmt.Errorf("ingest handler missing database handle")
	}
	if h.HTTPClient == nil {
		return fmt.Errorf("ingest handler missing http client")
	}
	if h.Logger == nil {
		h.Logger = logger.DefaultLogger()
	}

	if err := validateRequest(req); err != nil {
		return err
	}

	def, err := h.IndexRepo.GetIndex(ctx, req.IndexID)
	if err != nil {
		return err
	}

	normalizedPayload, err := normalizeDocumentPayload(def, req.Payload)
	if err != nil {
		return err
	}

	shardKey, err := sharding.ComputeShardKey(def, req.DocumentID, normalizedPayload, sharding.RoutingMetadata(req.Routing))
	if err != nil {
		return err
	}

	shard, err := shards.LookupPrimaryShard(ctx, h.DB, req.IndexID, shardKey)
	if err != nil {
		if errors.Is(err, shards.ErrPrimaryNotFound) {
			return ErrShardNotFound
		}
		return err
	}

	nodeInfo, err := shards.LookupNode(ctx, h.DB, shard.PrimaryNode)
	if err != nil {
		return err
	}

	if strings.TrimSpace(nodeInfo.AdvertiseAddr) == "" {
		return fmt.Errorf("node %s missing advertise address", nodeInfo.ID)
	}

	forwardPayload := map[string]any{
		"index_id":    req.IndexID,
		"shard_id":    shard.ID,
		"document_id": req.DocumentID,
		"routing":     req.Routing,
		"payload":     normalizedPayload,
	}

	body, err := json.Marshal(forwardPayload)
	if err != nil {
		return fmt.Errorf("marshal forward payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(nodeInfo.AdvertiseAddr, "/")+"/documents", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build forward request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := h.HTTPClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("forward document to node %s: %w", nodeInfo.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("node %s returned %d: %s", nodeInfo.ID, resp.StatusCode, string(slurp))
	}

	h.Logger.Info("document forwarded",
		logger.Field{Key: "index_id", Value: req.IndexID},
		logger.Field{Key: "shard_id", Value: shard.ID},
		logger.Field{Key: "node_id", Value: nodeInfo.ID},
	)

	return nil
}

func validateRequest(req Request) error {
	if strings.TrimSpace(req.IndexID) == "" {
		return fmt.Errorf("index id is required")
	}
	if strings.TrimSpace(req.DocumentID) == "" {
		return fmt.Errorf("document id is required")
	}
	return nil
}
