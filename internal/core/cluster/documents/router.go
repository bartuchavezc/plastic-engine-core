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
	"plastic-engine-core/internal/core/cluster/mappings"
	"plastic-engine-core/internal/core/cluster/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// IndexRepository resolves index definitions.
type IndexRepository interface {
	GetIndex(ctx context.Context, id string) (indexes.IndexDefinition, error)
}

// MappingsService handles dynamic mapping and field validation.
type MappingsService interface {
	ProcessDocument(ctx context.Context, indexID string, doc map[string]any) ([]mappings.Field, error)
	AddFields(ctx context.Context, indexID string, fields []mappings.Field) error
}

// Router encapsulates the dependencies required to execute the ingestion workflow.
type Router struct {
	IndexRepo   IndexRepository
	ShardRepo   *shards.Repository
	MappingsSvc MappingsService
	HTTPClient  *http.Client
	Logger      logger.Logger
}

// NewRouter creates a document router.
func NewRouter(db *sql.DB, indexRepo IndexRepository, mappingsSvc MappingsService, httpClient *http.Client, log logger.Logger) *Router {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Router{
		IndexRepo:   indexRepo,
		ShardRepo:   shards.NewRepository(db),
		MappingsSvc: mappingsSvc,
		HTTPClient:  httpClient,
		Logger:      log,
	}
}

// Handle executes the ingestion pipeline by routing the document to the appropriate shard.
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

	// Process document for dynamic mapping (infer new fields, validate types)
	if r.MappingsSvc != nil && len(req.Payload) > 0 {
		var doc map[string]any
		if err := json.Unmarshal(req.Payload, &doc); err == nil && doc != nil {
			newFields, err := r.MappingsSvc.ProcessDocument(ctx, req.IndexID, doc)
			if err != nil {
				return fmt.Errorf("mapping validation failed: %w", err)
			}

			// Persist new fields if dynamic mapping added them
			if len(newFields) > 0 {
				if err := r.MappingsSvc.AddFields(ctx, req.IndexID, newFields); err != nil {
					return fmt.Errorf("persist inferred fields: %w", err)
				}
				r.Logger.Info("dynamic mapping: added fields",
					logger.Field{Key: "index_id", Value: req.IndexID},
					logger.Field{Key: "count", Value: len(newFields)},
				)

				// Refresh index definition to include new fields
				def, err = r.IndexRepo.GetIndex(ctx, req.IndexID)
				if err != nil {
					return err
				}
			}
		}
	}

	normalizedPayload, err := NormalizePayload(def, req.Payload)
	if err != nil {
		return err
	}

	shardKey, err := sharding.ComputeShardKey(def, req.DocumentID, normalizedPayload, sharding.RoutingMetadata(req.Routing))
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

	forwardPayload := map[string]any{
		"index_id":        req.IndexID,
		"shard_id":        shardInfo.ID,
		"document_id":     req.DocumentID,
		"routing":         req.Routing,
		"payload":         normalizedPayload,
		"mapping_version": def.MappingVersion,
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

	r.Logger.Info("document forwarded",
		logger.Field{Key: "index_id", Value: req.IndexID},
		logger.Field{Key: "shard_id", Value: shardInfo.ID},
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

