package nodes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/indexstore"
	searchshards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// ShardAssigner handles shard assignment operations.
type ShardAssigner interface {
	AssignToNodeTx(ctx context.Context, tx *sql.Tx, nodeID string, limit int) ([]searchshards.Assignment, error)
	LoadAssignmentsForNodeTx(ctx context.Context, tx *sql.Tx, nodeID string) ([]searchshards.Assignment, error)
}

// IndexRepository provides access to index definitions.
type IndexRepository interface {
	GetIndex(ctx context.Context, id string) (indexes.IndexDefinition, error)
}

// Service handles node membership operations.
type Service struct {
	repo      *Repository
	shards    ShardAssigner
	indexRepo IndexRepository
	log       logger.Logger
}

// NewService creates a node service.
func NewService(db *sql.DB, shards ShardAssigner, indexRepo IndexRepository, log logger.Logger) *Service {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Service{
		repo:      NewRepository(db),
		shards:    shards,
		indexRepo: indexRepo,
		log:       log,
	}
}

const defaultShardAssignmentBatch = 32

// Join registers a node in the cluster and returns shard assignments.
func (s *Service) Join(ctx context.Context, req JoinRequest) (JoinResponse, error) {
	ctx, span := otel.Tracer("nodes").Start(ctx, "nodes.Service.Join")
	defer span.End()

	span.SetAttributes(
		attribute.String("node.role", req.Role),
		attribute.String("node.advertise_addr", req.AdvertiseAddr),
	)

	if req.Role == "" {
		return JoinResponse{}, errors.New("join request missing role")
	}

	nodeID := req.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	tx, err := s.repo.DB().BeginTx(ctx, nil)
	if err != nil {
		return JoinResponse{}, fmt.Errorf("begin join transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := s.repo.UpsertNode(ctx, tx, nodeID, req); err != nil {
		return JoinResponse{}, err
	}

	var newAssignments []searchshards.Assignment
	if s.shards != nil {
		newAssignments, err = s.shards.AssignToNodeTx(ctx, tx, nodeID, defaultShardAssignmentBatch)
		if err != nil {
			return JoinResponse{}, err
		}
	}

	var assignments []searchshards.Assignment
	if s.shards != nil {
		assignments, err = s.shards.LoadAssignmentsForNodeTx(ctx, tx, nodeID)
		if err != nil {
			return JoinResponse{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return JoinResponse{}, fmt.Errorf("commit join transaction: %w", err)
	}

	assignments, err = s.enrichAssignments(ctx, assignments)
	if err != nil {
		return JoinResponse{}, err
	}

	if len(newAssignments) > 0 {
		s.log.Info("assigned shards during join",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "count", Value: len(newAssignments)},
		)
	}

	return JoinResponse{
		NodeID: nodeID,
		Shards: append(assignments, newAssignments...),
	}, nil
}

// Heartbeat updates metadata for an existing node and returns mapping version updates.
func (s *Service) Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	ctx, span := otel.Tracer("nodes").Start(ctx, "nodes.Service.Heartbeat")
	defer span.End()

	if req.NodeID == "" {
		return HeartbeatResponse{}, errors.New("heartbeat missing node id")
	}

	tx, err := s.repo.DB().BeginTx(ctx, nil)
	if err != nil {
		return HeartbeatResponse{}, fmt.Errorf("begin heartbeat tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05")

	if err := s.repo.UpdateHeartbeat(ctx, tx, req.NodeID, timestamp); err != nil {
		return HeartbeatResponse{}, err
	}

	span.SetAttributes(
		attribute.String("node.id", req.NodeID),
		attribute.Int("node.shards.count", len(req.Shards)),
	)

	s.log.Info("heartbeat received",
		logger.Field{Key: "node_id", Value: req.NodeID},
		logger.Field{Key: "shards", Value: req.Shards},
	)

	if err := tx.Commit(); err != nil {
		return HeartbeatResponse{}, fmt.Errorf("commit heartbeat tx: %w", err)
	}

	// Calculate mapping versions for indexes that the node has shards for
	mappingUpdates, err := s.getMappingVersionsForShards(ctx, req.Shards)
	if err != nil {
		// Log but don't fail the heartbeat
		s.log.Error("failed to get mapping versions", logger.Field{Key: "error", Value: err})
		mappingUpdates = nil
	}

	return HeartbeatResponse{
		Status:         "ok",
		MappingUpdates: mappingUpdates,
	}, nil
}

// getMappingVersionsForShards returns current mapping versions for indexes related to the given shards.
func (s *Service) getMappingVersionsForShards(ctx context.Context, shardIDs []string) (map[string]int, error) {
	if len(shardIDs) == 0 || s.indexRepo == nil {
		return nil, nil
	}

	// Get unique index IDs from shard IDs
	indexIDs, err := s.repo.GetIndexIDsForShards(ctx, shardIDs)
	if err != nil {
		return nil, err
	}

	if len(indexIDs) == 0 {
		return nil, nil
	}

	// Get current mapping versions for each index
	versions := make(map[string]int, len(indexIDs))
	for _, indexID := range indexIDs {
		def, err := s.indexRepo.GetIndex(ctx, indexID)
		if err != nil {
			continue // Skip indexes that can't be fetched
		}
		versions[indexID] = def.MappingVersion
	}

	return versions, nil
}

// List returns all nodes registered in the cluster.
func (s *Service) List(ctx context.Context) ([]NodeRecord, error) {
	return s.repo.List(ctx)
}

// ListEligible returns nodes that can receive shard assignments.
func (s *Service) ListEligible(ctx context.Context) ([]NodeRecord, error) {
	return s.repo.ListEligible(ctx)
}

// StartHealthMonitor periodically marks nodes without fresh heartbeats as unreachable.
func (s *Service) StartHealthMonitor(ctx context.Context, interval time.Duration, timeout time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				s.log.Info("health monitor stopped")
				return
			case <-ticker.C:
				affected, err := s.repo.MarkStaleNodes(ctx, timeout)
				if err != nil {
					s.log.Error("health monitor error", logger.Field{Key: "error", Value: err})
					continue
				}
				if affected > 0 {
					s.log.Info("marked nodes unreachable", logger.Field{Key: "count", Value: affected})
				}
			}
		}
	}()
}

func (s *Service) enrichAssignments(ctx context.Context, assignments []searchshards.Assignment) ([]searchshards.Assignment, error) {
	if len(assignments) == 0 {
		return assignments, nil
	}

	if s.indexRepo == nil {
		return nil, errors.New("index repository not available")
	}

	cache := make(map[string]indexes.IndexDefinition)

	for i := range assignments {
		assignment := &assignments[i]

		def, ok := cache[assignment.IndexID]
		if !ok {
			var err error
			def, err = s.indexRepo.GetIndex(ctx, assignment.IndexID)
			if err != nil {
				return nil, err
			}
			cache[assignment.IndexID] = def
		}

		assignment.Analyzer = def.DefaultAnalyzer
		assignment.Tokenizer = def.DefaultTokenizer
		assignment.MappingVersion = def.MappingVersion
		if len(def.FieldMappings) > 0 {
			assignment.Fields = append([]indexes.FieldMapping(nil), def.FieldMappings...)
		}
		assignment.ShardStrategy = def.ShardStrategy

		// Decode per-index configs from raw JSON into typed fields
		if len(def.CooccurrenceConfigRaw) > 0 {
			var cc indexstore.CooccurrenceConfig
			if err := json.Unmarshal(def.CooccurrenceConfigRaw, &cc); err == nil {
				assignment.CooccurrenceConfig = cc
			}
		}
		if len(def.SearchPipelineRaw) > 0 {
			var sp indexstore.SearchPipeline
			if err := json.Unmarshal(def.SearchPipelineRaw, &sp); err == nil {
				assignment.SearchPipeline = sp
			}
		}
	}

	return assignments, nil
}
