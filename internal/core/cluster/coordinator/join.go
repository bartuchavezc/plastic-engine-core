package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/search/shard"
	"plastic-engine-core/internal/helpers"
)

// JoinRequest captures the information a node provides when joining the cluster.
type JoinRequest struct {
	NodeID        string
	Role          string
	AdvertiseAddr string
	DataDir       string
}

// JoinResponse details the coordinator's response to a join request.
type JoinResponse struct {
	NodeID string
	Shards []shard.Assignment
}

// Join registers a node in the metadata store and returns shard assignments.
func (c *Coordinator) Join(ctx context.Context, req JoinRequest) (JoinResponse, error) {
	ctx, span := otel.Tracer("coordinator").Start(ctx, "Coordinator.Join")
	defer span.End()

	span.SetAttributes(
		attribute.String("node.role", req.Role),
		attribute.String("node.advertise_addr", req.AdvertiseAddr),
	)

	if c == nil || c.db == nil {
		return JoinResponse{}, errors.New("coordinator is not initialised")
	}

	if req.Role == "" {
		return JoinResponse{}, errors.New("join request missing role")
	}

	nodeID := req.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return JoinResponse{}, fmt.Errorf("begin join transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := upsertNode(tx, nodeID, req); err != nil {
		return JoinResponse{}, err
	}

	newAssignments, err := assignShardsToNodeTx(ctx, tx, nodeID, defaultShardAssignmentBatch)
	if err != nil {
		return JoinResponse{}, err
	}

	assignments, err := loadAssignmentsForNodeTx(ctx, tx, nodeID)
	if err != nil {
		return JoinResponse{}, err
	}

	if err := tx.Commit(); err != nil {
		return JoinResponse{}, fmt.Errorf("commit join transaction: %w", err)
	}

	assignments, err = c.enrichAssignments(ctx, assignments)
	if err != nil {
		return JoinResponse{}, err
	}

	if len(newAssignments) > 0 {
		c.Logger().Info("assigned shards during join",
			helpers.Field{Key: "node_id", Value: nodeID},
			helpers.Field{Key: "count", Value: len(newAssignments)},
		)
	}

	return JoinResponse{
		NodeID: nodeID,
		Shards: assignments,
	}, nil
}

func upsertNode(tx *sql.Tx, nodeID string, req JoinRequest) error {
	res, err := tx.Exec(
		`UPDATE nodes
		 SET role = ?, advertise_addr = ?, data_dir = ?, last_heartbeat = CURRENT_TIMESTAMP, status = 'joining'
		 WHERE id = ?`,
		req.Role,
		req.AdvertiseAddr,
		req.DataDir,
		nodeID,
	)
	if err != nil {
		return fmt.Errorf("update node metadata: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected update: %w", err)
	}

	if rows > 0 {
		return nil
	}

	_, err = tx.Exec(
		`INSERT INTO nodes (id, role, advertise_addr, data_dir, last_heartbeat, status)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 'joining')`,
		nodeID,
		req.Role,
		req.AdvertiseAddr,
		req.DataDir,
	)
	if err != nil {
		return fmt.Errorf("insert node metadata: %w", err)
	}
	return nil
}

func (c *Coordinator) enrichAssignments(ctx context.Context, assignments []shard.Assignment) ([]shard.Assignment, error) {
	if len(assignments) == 0 {
		return assignments, nil
	}

	if c.indexRepo == nil {
		return nil, errors.New("coordinator index repository not initialised")
	}

	cache := make(map[string]coreindex.IndexDefinition)

	for i := range assignments {
		assignment := &assignments[i]

		def, ok := cache[assignment.IndexID]
		if !ok {
			var err error
			def, err = c.indexRepo.GetIndex(ctx, assignment.IndexID)
			if err != nil {
				return nil, err
			}
			cache[assignment.IndexID] = def
		}

		assignment.Analyzer = def.DefaultAnalyzer
		assignment.Tokenizer = def.DefaultTokenizer
		assignment.MappingVersion = def.MappingVersion
		if len(def.FieldMappings) > 0 {
			assignment.Fields = append([]coreindex.FieldMapping(nil), def.FieldMappings...)
		}
		assignment.ShardStrategy = def.ShardStrategy
	}

	return assignments, nil
}
