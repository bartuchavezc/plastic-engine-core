package document

import (
	"context"
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
	Payload    map[string]any
	ReceivedAt time.Time
}

type WorkItem struct {
	Command Command
}

type WriterFactory func(shard *shards.Shard) *IndexWriter

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

// Index validates and enqueues the command for asynchronous processing.
func (s *Service) Index(ctx context.Context, cmd Command) error {
	if err := validateCommand(cmd); err != nil {
		return err
	}

	if cmd.ReceivedAt.IsZero() {
		cmd.ReceivedAt = time.Now().UTC()
	}

	worker, err := s.ensureWorker(cmd.ShardID)
	if err != nil {
		return err
	}

	return worker.Submit(ctx, WorkItem{Command: cmd})
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

	if cmd.Payload == nil {
		cmd.Payload = map[string]any{}
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
		// TODO dynamic shard creation: trigger coordinator to materialise missing shard and retry.
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
