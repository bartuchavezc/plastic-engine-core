package shards

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/indexes/sharding"
	searchshards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

const defaultShardAssignmentBatch = 32

// Service handles shard operations.
type Service struct {
	repo           *Repository
	plannerFactory *sharding.PlannerFactory
	log            logger.Logger
}

// NewService creates a shard service.
func NewService(db *sql.DB, log logger.Logger) *Service {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Service{
		repo:           NewRepository(db),
		plannerFactory: sharding.NewFactory(),
		log:            log,
	}
}

// PlanShardKeys computes the shard keys for an index definition without persisting them.
// This is useful when shard creation is handled by a StateApplier.
func (s *Service) PlanShardKeys(def indexes.IndexDefinition) []string {
	planner, err := s.plannerFactory.Get(def.ShardStrategy)
	if err != nil {
		s.log.Error("failed to get shard planner", logger.Field{Key: "error", Value: err})
		return nil
	}

	specs, err := planner.PlanInitialShards(context.Background(), def, nil)
	if err != nil {
		s.log.Error("failed to plan initial shards", logger.Field{Key: "error", Value: err})
		return nil
	}

	keys := make([]string, len(specs))
	for i, spec := range specs {
		keys[i] = spec.Key
	}
	return keys
}

// PlanInitial creates the initial shards for an index definition.
// Deprecated: Use PlanShardKeys with StateApplier for new code.
func (s *Service) PlanInitial(ctx context.Context, def indexes.IndexDefinition) error {
	planner, err := s.plannerFactory.Get(def.ShardStrategy)
	if err != nil {
		return err
	}

	specs, err := planner.PlanInitialShards(ctx, def, nil)
	if err != nil {
		return err
	}

	if len(specs) == 0 {
		return nil
	}

	tx, err := s.repo.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin shard planning tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC().Format(DefaultTimestampFormat)

	for _, spec := range specs {
		shardID := fmt.Sprintf("%s-%s", def.ID, spec.Key)
		if err := s.repo.InsertShard(ctx, tx, shardID, def.ID, spec.Key, now); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit shard planning tx: %w", err)
	}

	s.log.Info("planned initial shards",
		logger.Field{Key: "index_id", Value: def.ID},
		logger.Field{Key: "shards", Value: len(specs)},
	)

	return nil
}

// AssignToNode assigns pending shards to a node.
func (s *Service) AssignToNode(ctx context.Context, nodeID string, limit int) ([]searchshards.Assignment, error) {
	tx, err := s.repo.DB().BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin assign shards tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	assignments, err := s.repo.AssignToNodeTx(ctx, tx, nodeID, limit)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit assign shards tx: %w", err)
	}

	return assignments, nil
}

// AssignToNodeTx assigns shards within an existing transaction.
func (s *Service) AssignToNodeTx(ctx context.Context, tx *sql.Tx, nodeID string, limit int) ([]searchshards.Assignment, error) {
	return s.repo.AssignToNodeTx(ctx, tx, nodeID, limit)
}

// LoadAssignmentsForNodeTx loads shard assignments for a node within an existing transaction.
func (s *Service) LoadAssignmentsForNodeTx(ctx context.Context, tx *sql.Tx, nodeID string) ([]searchshards.Assignment, error) {
	return s.repo.LoadAssignmentsForNodeTx(ctx, tx, nodeID)
}

// List returns shards matching the filter.
func (s *Service) List(ctx context.Context, filter ShardFilter) ([]ShardRecord, error) {
	return s.repo.List(ctx, filter)
}

// LookupPrimary resolves the shard and node for routing.
func (s *Service) LookupPrimary(ctx context.Context, indexID, shardKey string) (Info, NodeInfo, error) {
	shardInfo, err := s.repo.LookupPrimaryShard(ctx, indexID, shardKey)
	if err != nil {
		return Info{}, NodeInfo{}, err
	}

	nodeInfo, err := s.repo.LookupNode(ctx, shardInfo.PrimaryNode)
	if err != nil {
		return Info{}, NodeInfo{}, err
	}

	return shardInfo, nodeInfo, nil
}

// LookupNode resolves a node by identifier and returns its information.
func (s *Service) LookupNode(ctx context.Context, nodeID string) (NodeInfo, error) {
	return s.repo.LookupNode(ctx, nodeID)
}
