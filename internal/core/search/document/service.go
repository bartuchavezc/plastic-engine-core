package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// Command encapsulates everything needed to index a document into a shard.
type Command struct {
	IndexID    string
	ShardID    string
	DocumentID string
	Routing    map[string]string
	RawPayload json.RawMessage // Raw JSON bytes - validated and parsed at worker level
	ReceivedAt time.Time
}

type WorkItem struct {
	Command Command
}

// WriterFactory creates a DocumentIndexWriter for a shard.
type WriterFactory func(shard *shards.Shard) DocumentIndexWriter

type Service struct {
	shards        *shards.Manager
	assignments   *AssignmentProvider
	planner       *FieldPlanner
	writerFactory WriterFactory
	config        ShardWorkerConfig
	log           logger.Logger

	mu      sync.RWMutex
	workers map[string]*ShardWorker
}

var (
	// ErrInvalidCommand is returned when the command lacks required metadata.
	ErrInvalidCommand = errors.New("invalid index command")
)

func NewService(shards *shards.Manager, assignments *AssignmentProvider, planner *FieldPlanner, writerFactory WriterFactory, cfg ShardWorkerConfig, log logger.Logger) *Service {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Service{
		shards:        shards,
		assignments:   assignments,
		planner:       planner,
		writerFactory: writerFactory,
		config:        cfg,
		log:           log,
		workers:       make(map[string]*ShardWorker),
	}
}

// Index validates and synchronously indexes a single document.
// Blocks until the document is indexed or an error occurs.
func (s *Service) Index(ctx context.Context, cmd Command) error {
	if err := validateCommand(cmd); err != nil {
		return err
	}

	if cmd.ReceivedAt.IsZero() {
		cmd.ReceivedAt = time.Now().UTC()
	}

	if err := s.validateIndexConfig(ctx, cmd.ShardID); err != nil {
		s.log.Error("index configuration validation failed",
			logger.Field{Key: "shard_id", Value: cmd.ShardID},
			logger.Field{Key: "document_id", Value: cmd.DocumentID},
			logger.Field{Key: "error", Value: err},
		)
		return fmt.Errorf("index configuration error: %w", err)
	}

	worker, err := s.ensureWorker(cmd.ShardID)
	if err != nil {
		s.log.Error("failed to ensure worker",
			logger.Field{Key: "shard_id", Value: cmd.ShardID},
			logger.Field{Key: "document_id", Value: cmd.DocumentID},
			logger.Field{Key: "error", Value: err},
		)
		return err
	}

	errs := worker.Process(ctx, []Command{cmd})
	if len(errs) > 0 && errs[0] != nil {
		return errs[0]
	}

	s.log.Debug("document indexed",
		logger.Field{Key: "shard_id", Value: cmd.ShardID},
		logger.Field{Key: "document_id", Value: cmd.DocumentID},
	)

	return nil
}

// IndexBulk synchronously indexes a batch of commands, potentially from multiple shards.
// Returns per-document errors (nil = success). Blocks until all documents are processed.
func (s *Service) IndexBulk(ctx context.Context, cmds []Command) []error {
	if len(cmds) == 0 {
		return nil
	}

	errs := make([]error, len(cmds))

	// Group commands by shard.
	type shardGroup struct {
		indices []int
		cmds    []Command
	}
	groups := make(map[string]*shardGroup)

	for i := range cmds {
		cmd := &cmds[i]
		if err := validateCommand(*cmd); err != nil {
			errs[i] = err
			continue
		}
		if cmd.ReceivedAt.IsZero() {
			cmd.ReceivedAt = time.Now().UTC()
		}
		g := groups[cmd.ShardID]
		if g == nil {
			g = &shardGroup{}
			groups[cmd.ShardID] = g
		}
		g.indices = append(g.indices, i)
		g.cmds = append(g.cmds, *cmd)
	}

	// Process shards in parallel — each shard's worker.Process is independent.
	// Previously shards were processed sequentially, serializing work that
	// could overlap (tokenization on shard A while shard B flushes to segment).
	var wg sync.WaitGroup
	var mu sync.Mutex // protects errs slice
	for shardID, group := range groups {
		worker, err := s.ensureWorker(shardID)
		if err != nil {
			for _, idx := range group.indices {
				errs[idx] = err
			}
			continue
		}

		wg.Add(1)
		go func(w *ShardWorker, g *shardGroup) {
			defer wg.Done()
			shardErrs := w.Process(ctx, g.cmds)
			mu.Lock()
			for j, shardErr := range shardErrs {
				if shardErr != nil {
					errs[g.indices[j]] = shardErr
				}
			}
			mu.Unlock()
		}(worker, group)
	}
	wg.Wait()

	return errs
}

// validateIndexConfig validates that the index configuration is valid for indexing.
func (s *Service) validateIndexConfig(ctx context.Context, shardID string) error {
	_, indexDef, err := s.assignments.AssignmentForShard(ctx, shardID)
	if err != nil {
		return fmt.Errorf("failed to resolve assignment: %w", err)
	}

	_, err = s.planner.BuildPlans(indexDef)
	if err != nil {
		return fmt.Errorf("failed to build field plans: %w", err)
	}

	return nil
}

func validateCommand(cmd Command) error {
	switch {
	case strings.TrimSpace(cmd.IndexID) == "":
		return ErrInvalidCommand
	case strings.TrimSpace(cmd.ShardID) == "":
		return ErrInvalidCommand
	case strings.TrimSpace(cmd.DocumentID) == "":
		return ErrInvalidCommand
	}

	if cmd.RawPayload == nil {
		cmd.RawPayload = []byte("{}")
	}

	return nil
}

// Close stops all shard workers.
func (s *Service) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, worker := range s.workers {
		worker.Close()
		delete(s.workers, id)
	}
}

func (s *Service) ensureWorker(shardID string) (*ShardWorker, error) {
	s.mu.RLock()
	if worker, ok := s.workers[shardID]; ok {
		s.mu.RUnlock()
		return worker, nil
	}
	s.mu.RUnlock()

	if s.writerFactory == nil {
		return nil, fmt.Errorf("writer factory not configured")
	}

	shard, ok := s.shards.GetShard(shardID)
	if !ok {
		return nil, ErrShardNotLoaded
	}

	writer := s.writerFactory(shard)
	if writer == nil {
		return nil, fmt.Errorf("writer factory returned nil for shard %s", shardID)
	}

	worker := NewShardWorker(shardID, shard, s.config, s.planner, writer, s.assignments, s.log)

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.workers[shardID]; ok {
		worker.Close()
		return existing, nil
	}

	s.workers[shardID] = worker
	return worker, nil
}
