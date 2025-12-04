package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"

	"plastic-engine-core/internal/core/cluster/documents"
	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/nodes"
	"plastic-engine-core/internal/core/cluster/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// Coordinator orchestrates cluster metadata and shard assignments.
// It uses a StateApplier to handle state mutations, allowing the same
// coordinator logic to work in both standalone and HA (Raft) modes.
type Coordinator struct {
	Role string
	Port string
	db   *sql.DB
	log  logger.Logger

	applier     StateApplier
	indexRepo   *indexes.Repository
	nodesSvc    *nodes.Service
	shardsSvc   *shards.Service
	documentsRouter *documents.Router
}

// CoordinatorConfig holds configuration for creating a Coordinator.
type CoordinatorConfig struct {
	Role    string
	Port    string
	DBPath  string
	Applier StateApplier // If nil, uses LocalApplier
	Logger  logger.Logger
}

// NewCoordinator creates a coordinator bound to a SQLite metadata store.
// Deprecated: Use NewCoordinatorWithConfig for more control.
func NewCoordinator(role string, port string, dbPath string) (*Coordinator, error) {
	return NewCoordinatorWithConfig(CoordinatorConfig{
		Role:   role,
		Port:   port,
		DBPath: dbPath,
	})
}

// NewCoordinatorWithConfig creates a coordinator with the provided configuration.
func NewCoordinatorWithConfig(cfg CoordinatorConfig) (*Coordinator, error) {
	db, err := OpenMetadataDB(cfg.DBPath)
	if err != nil {
		return nil, err
	}

	log := cfg.Logger
	if log == nil {
		log = logger.DefaultLogger()
	}

	// Use provided applier or create a local one
	applier := cfg.Applier
	if applier == nil {
		applier = NewLocalApplier(db, log)
	}

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	indexRepo := indexes.NewRepository(db)
	shardsSvc := shards.NewService(db, log)
	nodesSvc := nodes.NewService(db, shardsSvc, indexRepo, log)
	docRouter := documents.NewRouter(db, indexRepo, httpClient, log)

	return &Coordinator{
		Role:            cfg.Role,
		Port:            cfg.Port,
		db:              db,
		log:             log,
		applier:         applier,
		indexRepo:       indexRepo,
		nodesSvc:        nodesSvc,
		shardsSvc:       shardsSvc,
		documentsRouter: docRouter,
	}, nil
}

// NewCoordinatorWithDeps creates a coordinator with pre-initialized dependencies.
// This is useful when you want to manage the database and applier externally.
func NewCoordinatorWithDeps(db *sql.DB, applier StateApplier, log logger.Logger) (*Coordinator, error) {
	if db == nil {
		return nil, errors.New("database is required")
	}
	if applier == nil {
		return nil, errors.New("applier is required")
	}
	if log == nil {
		log = logger.DefaultLogger()
	}

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	indexRepo := indexes.NewRepository(db)
	shardsSvc := shards.NewService(db, log)
	nodesSvc := nodes.NewService(db, shardsSvc, indexRepo, log)
	docRouter := documents.NewRouter(db, indexRepo, httpClient, log)

	return &Coordinator{
		db:              db,
		log:             log,
		applier:         applier,
		indexRepo:       indexRepo,
		nodesSvc:        nodesSvc,
		shardsSvc:       shardsSvc,
		documentsRouter: docRouter,
	}, nil
}

// Close releases database resources held by the cluster.
func (c *Coordinator) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

// Logger returns the coordinator logger instance.
func (c *Coordinator) Logger() logger.Logger {
	return c.log
}

// DB returns the underlying SQL database handle (primarily for testing).
func (c *Coordinator) DB() *sql.DB {
	return c.db
}

// Applier returns the state applier for external use.
func (c *Coordinator) Applier() StateApplier {
	return c.applier
}

// IsLeader returns true if this coordinator can accept writes.
func (c *Coordinator) IsLeader() bool {
	if c.applier == nil {
		return false
	}
	return c.applier.IsLeader()
}

// LeaderAddr returns the address of the current leader.
func (c *Coordinator) LeaderAddr() string {
	if c.applier == nil {
		return ""
	}
	return c.applier.LeaderAddr()
}

// NodesService returns the nodes service for external use.
func (c *Coordinator) NodesService() *nodes.Service {
	return c.nodesSvc
}

// ShardsService returns the shards service for external use.
func (c *Coordinator) ShardsService() *shards.Service {
	return c.shardsSvc
}

// CreateIndex registers a new index definition in the metadata store.
// This operation goes through the StateApplier for consistency.
func (c *Coordinator) CreateIndex(ctx context.Context, req indexes.CreateIndexRequest) (indexes.CreateIndexResponse, error) {
	ctx, span := otel.Tracer("cluster.index").Start(ctx, "Coordinator.CreateIndex")
	defer span.End()

	if c == nil || c.applier == nil {
		return indexes.CreateIndexResponse{}, errors.New("coordinator not initialised")
	}

	// Normalize shard config to apply defaults before validation
	indexes.NormalizeShardConfig(&req.ShardConfig, req.ShardStrategy)
	req.ShardStrategy = req.ShardConfig.Strategy

	// Validate required fields before creating the command
	if err := indexes.ValidateCreateRequest(req); err != nil {
		return indexes.CreateIndexResponse{}, err
	}

	// Serialize field mappings
	var fieldMappingsJSON json.RawMessage
	if len(req.FieldMappings) > 0 {
		data, err := json.Marshal(req.FieldMappings)
		if err != nil {
			return indexes.CreateIndexResponse{}, err
		}
		fieldMappingsJSON = data
	}

	// Serialize shard config
	var shardConfigJSON json.RawMessage
	if req.ShardConfig.Strategy != "" {
		data, err := json.Marshal(req.ShardConfig)
		if err != nil {
			return indexes.CreateIndexResponse{}, err
		}
		shardConfigJSON = data
	}

	// Create the command payload
	payload := CreateIndexPayload{
		ID:               req.ID,
		Name:             req.Name,
		ShardStrategy:    string(req.ShardStrategy),
		ShardTemplate:    req.ShardTemplate,
		ShardConfig:      shardConfigJSON,
		DefaultAnalyzer:  req.DefaultAnalyzer,
		DefaultTokenizer: req.DefaultTokenizer,
		MappingVersion:   req.MappingVersion,
		FieldMappings:    fieldMappingsJSON,
		CreatedAt:        time.Now().UTC(),
	}

	cmd, err := NewCommand(CmdCreateIndex, payload)
	if err != nil {
		return indexes.CreateIndexResponse{}, err
	}

	// Apply through the state applier
	if err := c.applier.Apply(ctx, cmd); err != nil {
		c.log.Error("create index failed", logger.Field{Key: "error", Value: err}, logger.Field{Key: "index_id", Value: req.ID})
		return indexes.CreateIndexResponse{}, err
	}

	c.log.Info("index created", logger.Field{Key: "index_id", Value: req.ID})

	// Read back the created index
	def, err := c.indexRepo.GetIndex(ctx, req.ID)
	if err != nil {
		return indexes.CreateIndexResponse{}, err
	}

	// Plan initial shards
	shardKeys := c.shardsSvc.PlanShardKeys(def)
	if len(shardKeys) > 0 {
		shardCmd, err := NewCommand(CmdCreateShards, CreateShardsPayload{
			IndexID: req.ID,
			Keys:    shardKeys,
		})
		if err != nil {
			return indexes.CreateIndexResponse{}, err
		}
		if err := c.applier.Apply(ctx, shardCmd); err != nil {
			c.log.Error("planning initial shards failed", logger.Field{Key: "error", Value: err}, logger.Field{Key: "index_id", Value: req.ID})
			return indexes.CreateIndexResponse{}, err
		}
	}

	// Try to assign pending shards to ready nodes
	if err := c.assignPendingShardsToReadyNodes(ctx); err != nil {
		c.log.Error("assign pending shards failed", logger.Field{Key: "error", Value: err}, logger.Field{Key: "index_id", Value: req.ID})
		// Don't fail the whole operation, just log the error
	}

	return indexes.CreateIndexResponse{Definition: def}, nil
}

// GetIndex retrieves an index definition by identifier.
func (c *Coordinator) GetIndex(ctx context.Context, id string) (indexes.IndexDefinition, error) {
	ctx, span := otel.Tracer("cluster.index").Start(ctx, "Coordinator.GetIndex")
	defer span.End()

	if c == nil || c.indexRepo == nil {
		return indexes.IndexDefinition{}, errors.New("coordinator not initialised")
	}

	def, err := c.indexRepo.GetIndex(ctx, id)
	if err != nil {
		if errors.Is(err, indexes.ErrIndexNotFound) {
			c.log.Info("index lookup returned not found", logger.Field{Key: "index_id", Value: id})
		} else {
			c.log.Error("get index failed", logger.Field{Key: "error", Value: err}, logger.Field{Key: "index_id", Value: id})
		}
		return indexes.IndexDefinition{}, err
	}

	return def, nil
}

// StartHealthMonitor delegates to the nodes service health monitor.
func (c *Coordinator) StartHealthMonitor(ctx context.Context, interval time.Duration, timeout time.Duration) {
	if c == nil || c.nodesSvc == nil {
		return
	}
	c.nodesSvc.StartHealthMonitor(ctx, interval, timeout)
}

// IngestDocument routes a document to the appropriate search node.
func (c *Coordinator) IngestDocument(ctx context.Context, req documents.Request) error {
	if c == nil || c.documentsRouter == nil {
		return errors.New("coordinator not initialised")
	}
	return c.documentsRouter.Handle(ctx, req)
}

// ListNodes returns all nodes registered in the cluster.
func (c *Coordinator) ListNodes(ctx context.Context) ([]nodes.NodeRecord, error) {
	if c == nil || c.nodesSvc == nil {
		return nil, errors.New("coordinator not initialised")
	}
	return c.nodesSvc.List(ctx)
}

// ListShards returns shard metadata filtered by the provided filter.
func (c *Coordinator) ListShards(ctx context.Context, filter shards.ShardFilter) ([]shards.ShardRecord, error) {
	if c == nil || c.shardsSvc == nil {
		return nil, errors.New("coordinator not initialised")
	}
	return c.shardsSvc.List(ctx, filter)
}

// ListIndexes returns all index definitions registered in the cluster.
func (c *Coordinator) ListIndexes(ctx context.Context) ([]indexes.IndexDefinition, error) {
	if c == nil || c.indexRepo == nil {
		return nil, errors.New("coordinator not initialised")
	}

	defs, err := c.indexRepo.ListIndexes(ctx)
	if err != nil {
		c.log.Error("list indexes failed", logger.Field{Key: "error", Value: err})
		return nil, err
	}
	return defs, nil
}

const defaultShardAssignmentBatch = 32

func (c *Coordinator) assignPendingShardsToReadyNodes(ctx context.Context) error {
	eligibleNodes, err := c.nodesSvc.ListEligible(ctx)
	if err != nil {
		return err
	}

	if len(eligibleNodes) == 0 {
		return nil
	}

	for _, node := range eligibleNodes {
		assignments, err := c.shardsSvc.AssignToNode(ctx, node.ID, defaultShardAssignmentBatch)
		if err != nil {
			return err
		}

		if len(assignments) > 0 {
			c.log.Info("assigned shards to node",
				logger.Field{Key: "node_id", Value: node.ID},
				logger.Field{Key: "count", Value: len(assignments)},
			)
		}
	}

	return nil
}
