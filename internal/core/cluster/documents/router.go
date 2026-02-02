package documents

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

	"go.opentelemetry.io/otel"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/indexes/sharding"
	"plastic-engine-core/internal/core/cluster/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// IndexRepository resolves index definitions.
type IndexRepository interface {
	GetIndex(ctx context.Context, id string) (indexes.IndexDefinition, error)
}

// Router encapsulates the dependencies required to execute the ingestion workflow.
// Note: Mapping validation has been moved to the search node for better performance.
// The coordinator only extracts routing information and forwards raw bytes.
type Router struct {
	IndexRepo  IndexRepository
	ShardRepo  *shards.Repository
	HTTPClient *http.Client
	Batcher    *DocumentBatcher
	Logger     logger.Logger
}

// RouterConfig holds configuration for creating a Router.
type RouterConfig struct {
	DB         *sql.DB
	IndexRepo  IndexRepository
	HTTPClient *http.Client
	Batcher    *DocumentBatcher
	Logger     logger.Logger
}

// NewRouter creates a document router.
func NewRouter(db *sql.DB, indexRepo IndexRepository, httpClient *http.Client, log logger.Logger) *Router {
	return NewRouterWithConfig(RouterConfig{
		DB:         db,
		IndexRepo:  indexRepo,
		HTTPClient: httpClient,
		Logger:     log,
	})
}

// NewRouterWithConfig creates a document router with full configuration.
func NewRouterWithConfig(cfg RouterConfig) *Router {
	log := cfg.Logger
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Router{
		IndexRepo:  cfg.IndexRepo,
		ShardRepo:  shards.NewRepository(cfg.DB),
		HTTPClient: cfg.HTTPClient,
		Batcher:    cfg.Batcher,
		Logger:     log,
	}
}

// Handle executes the ingestion pipeline by routing the document to the appropriate shard.
// Note: Mapping validation is performed at the search node, not here.
// The coordinator only extracts routing information and forwards raw bytes.
func (r *Router) Handle(ctx context.Context, req Request) error {
	ctx, span := otel.Tracer("cluster.documents").Start(ctx, "Router.Handle")
	defer span.End()

	if r.IndexRepo == nil {
		return fmt.Errorf("document router missing index repository")
	}
	if r.ShardRepo == nil {
		return fmt.Errorf("document router missing shard repository")
	}
	if r.HTTPClient == nil {
		return fmt.Errorf("document router missing http client")
	}

	if err := validateRequest(req); err != nil {
		return err
	}

	def, err := r.IndexRepo.GetIndex(ctx, req.IndexID)
	if err != nil {
		return err
	}

	// Compute shard key using raw payload (gjson extraction, no full parse)
	shardKey, err := sharding.ComputeShardKey(def, req.DocumentID, req.Payload, sharding.RoutingMetadata(req.Routing))
	if err != nil {
		return err
	}

	shardInfo, err := r.ShardRepo.LookupPrimaryShard(ctx, req.IndexID, shardKey)
	if err != nil {
		if errors.Is(err, shards.ErrPrimaryNotFound) {
			return ErrShardNotFound
		}
		return err
	}

	nodeInfo, err := r.ShardRepo.LookupNode(ctx, shardInfo.PrimaryNode)
	if err != nil {
		return err
	}

	if strings.TrimSpace(nodeInfo.AdvertiseAddr) == "" {
		return fmt.Errorf("node %s missing advertise address", nodeInfo.ID)
	}

	// Use batcher if available for improved throughput
	if r.Batcher != nil {
		doc := BatchDocument{
			IndexID:    req.IndexID,
			ShardID:    shardInfo.ID,
			DocumentID: req.DocumentID,
			Routing:    req.Routing,
			Payload:    req.Payload, // Forward raw bytes, validation happens at search node
		}

		if err := r.Batcher.Add(ctx, nodeInfo.ID, nodeInfo.AdvertiseAddr, doc); err != nil {
			return fmt.Errorf("batch document for node %s: %w", nodeInfo.ID, err)
		}

		r.Logger.Debug("document batched",
			logger.Field{Key: "index_id", Value: req.IndexID},
			logger.Field{Key: "shard_id", Value: shardInfo.ID},
			logger.Field{Key: "node_id", Value: nodeInfo.ID},
		)

		return nil
	}

	// Direct HTTP request (fallback when batcher is not configured)
	return r.sendDirectRequest(ctx, nodeInfo, req, shardInfo.ID)
}

func (r *Router) sendDirectRequest(ctx context.Context, nodeInfo shards.NodeInfo, req Request, shardID string) error {
	// Build forward payload with raw bytes (no re-serialization of payload content)
	forwardPayload := map[string]any{
		"index_id":    req.IndexID,
		"shard_id":    shardID,
		"document_id": req.DocumentID,
		"routing":     req.Routing,
		"payload":     req.Payload, // json.RawMessage is included directly
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

	resp, err := r.HTTPClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("forward document to node %s: %w", nodeInfo.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("node %s returned %d: %s", nodeInfo.ID, resp.StatusCode, string(slurp))
	}

	r.Logger.Debug("document forwarded",
		logger.Field{Key: "index_id", Value: req.IndexID},
		logger.Field{Key: "shard_id", Value: shardID},
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

