package cluster

import (
	"context"
	"database/sql"
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
type Coordinator struct {
	Role string
	Port string
	db   *sql.DB
	log  logger.Logger

	indexRepo   *indexes.Repository
	nodesSvc    *nodes.Service
	shardsSvc   *shards.Service
	documentsRouter *documents.Router
}

// NewCoordinator creates a coordinator bound to a SQLite metadata store.
func NewCoordinator(role string, port string, dbPath string) (*Coordinator, error) {
	db, err := OpenMetadataDB(dbPath)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	log := logger.DefaultLogger()
	indexRepo := indexes.NewRepository(db)
	shardsSvc := shards.NewService(db, log)
	nodesSvc := nodes.NewService(db, shardsSvc, indexRepo, log)
	docRouter := documents.NewRouter(db, indexRepo, httpClient, log)

	return &Coordinator{
		Role:           role,
		Port:           port,
		db:             db,
		log:            log,
		indexRepo:      indexRepo,
		nodesSvc:       nodesSvc,
		shardsSvc:      shardsSvc,
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

// NodesService returns the nodes service for external use.
func (c *Coordinator) NodesService() *nodes.Service {
	return c.nodesSvc
}

// ShardsService returns the shards service for external use.
func (c *Coordinator) ShardsService() *shards.Service {
	return c.shardsSvc
}

// CreateIndex registers a new index definition in the metadata store.
func (c *Coordinator) CreateIndex(ctx context.Context, req indexes.CreateIndexRequest) (indexes.CreateIndexResponse, error) {
	ctx, span := otel.Tracer("cluster.index").Start(ctx, "Coordinator.CreateIndex")
	defer span.End()

	if c == nil || c.indexRepo == nil {
		return indexes.CreateIndexResponse{}, errors.New("coordinator not initialised")
	}

	resp, err := c.indexRepo.CreateIndex(ctx, req)
	if err != nil {
		c.log.Error("create index failed", logger.Field{Key: "error", Value: err}, logger.Field{Key: "index_id", Value: req.ID})
		return indexes.CreateIndexResponse{}, err
	}

	c.log.Info("index created", logger.Field{Key: "index_id", Value: resp.Definition.ID})

	if err := c.shardsSvc.PlanInitial(ctx, resp.Definition); err != nil {
		c.log.Error("planning initial shards failed", logger.Field{Key: "error", Value: err}, logger.Field{Key: "index_id", Value: resp.Definition.ID})
		return indexes.CreateIndexResponse{}, err
	}

	if err := c.assignPendingShardsToReadyNodes(ctx); err != nil {
		c.log.Error("assign pending shards failed", logger.Field{Key: "error", Value: err}, logger.Field{Key: "index_id", Value: resp.Definition.ID})
		return indexes.CreateIndexResponse{}, err
	}

	return resp, nil
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
