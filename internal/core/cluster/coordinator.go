package cluster

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/indexes/sharding"
	"plastic-engine-core/internal/pkg/logger"

	"go.opentelemetry.io/otel"
)

// Coordinator orchestrates cluster metadata and shard assignments.
type Coordinator struct {
	Role string
	Port string
	db   *sql.DB
	log  logger.Logger

	indexRepo           *indexes.Repository
	shardPlannerFactory *sharding.PlannerFactory
	ingestHandler       Handler
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

	repo := indexes.NewRepository(db)
	logger := logger.DefaultLogger()
	return &Coordinator{
		Role: role,
		Port: port,
		db:   db,
		log:  logger,

		indexRepo:           repo,
		shardPlannerFactory: sharding.NewFactory(),
		ingestHandler: Handler{
			IndexRepo:  repo,
			DB:         db,
			HTTPClient: httpClient,
			Logger:     logger,
		},
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

	if err := c.planInitialShards(ctx, resp.Definition); err != nil {
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

// StartHealthMonitor periodically marks nodes without fresh heartbeats as unreachable.
func (c *Coordinator) StartHealthMonitor(ctx context.Context, interval time.Duration, timeout time.Duration) {
	if c == nil || c.db == nil {
		return
	}

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
				c.Logger().Info("health monitor stopped")
				return
			case <-ticker.C:
				if err := c.markStaleNodes(timeout); err != nil {
					c.Logger().Error("health monitor error", logger.Field{Key: "error", Value: err})
				}
			}
		}
	}()
}

func (c *Coordinator) markStaleNodes(timeout time.Duration) error {
	threshold := time.Now().UTC().Add(-timeout).Format("2006-01-02 15:04:05")

	res, err := c.db.Exec(
		`UPDATE nodes
		 SET status = 'unreachable'
		 WHERE status <> 'unreachable'
		   AND (last_heartbeat IS NULL OR last_heartbeat <= ?)`,
		threshold,
	)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if affected > 0 {
		c.Logger().Info("marked nodes unreachable", logger.Field{Key: "count", Value: affected})
	}

	return nil
}

func (c *Coordinator) IngestDocument(ctx context.Context, req Request) error {
	return c.ingestHandler.Handle(ctx, req)
}
