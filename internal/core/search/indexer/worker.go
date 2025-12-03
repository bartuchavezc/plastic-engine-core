package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	searchshard "plastic-engine-core/internal/core/search/shard"
	"plastic-engine-core/internal/helpers"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var (
	tracer = otel.Tracer("search/indexer")

	// ErrBackpressure indicates the shard queue is full.
	ErrBackpressure = errors.New("indexer backpressure: shard queue full")
)

// ShardWorkerConfig controls concurrency and queueing for a shard.
type ShardWorkerConfig struct {
	MaxWorkers    int
	QueueCapacity int
}

func (c *ShardWorkerConfig) applyDefaults() {
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = 4
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 256
	}
}

// ShardWorker processes indexing commands for a particular shard.
type ShardWorker struct {
	shardID string
	store   *searchshard.Shard

	queue   chan WorkItem
	closing chan struct{}

	planBuilder *FieldPlanner
	writer      *IndexWriter
	assignments *AssignmentProvider

	log helpers.Logger

	wg sync.WaitGroup
}

// NewShardWorker spins up worker goroutines ready to process commands.
func NewShardWorker(shardID string, shard *searchshard.Shard, cfg ShardWorkerConfig, planner *FieldPlanner, writer *IndexWriter, assignments *AssignmentProvider, log helpers.Logger) *ShardWorker {
	cfg.applyDefaults()

	w := &ShardWorker{
		shardID: shardID,
		store:   shard,
		queue:   make(chan WorkItem, cfg.QueueCapacity),
		closing: make(chan struct{}),

		planBuilder: planner,
		writer:      writer,
		assignments: assignments,

		log: log,
	}

	for i := 0; i < cfg.MaxWorkers; i++ {
		w.wg.Add(1)
		go w.loop()
	}

	return w
}

// Submit enqueues a work item for processing.
func (w *ShardWorker) Submit(ctx context.Context, item WorkItem) error {
	select {
	case w.queue <- item:
		return nil
	case <-w.closing:
		return fmt.Errorf("shard worker shutting down")
	default:
		return ErrBackpressure
	}
}

// Close stops the worker and waits for in-flight operations.
func (w *ShardWorker) Close() {
	close(w.closing)
	w.wg.Wait()
	close(w.queue)
}

func (w *ShardWorker) loop() {
	defer w.wg.Done()

	for {
		select {
		case item := <-w.queue:
			w.process(item)
		case <-w.closing:
			return
		}
	}
}

func (w *ShardWorker) process(item WorkItem) {
	ctx, span := tracer.Start(context.Background(), "indexer.process")
	defer span.End()

	span.SetAttributes(
		attribute.String("shard.id", w.shardID),
		attribute.String("document.id", item.Command.DocumentID),
	)

	start := time.Now()

	assignment, indexDef, err := w.assignments.AssignmentForShard(ctx, w.shardID)
	if err != nil {
		w.log.Error("failed to resolve assignment",
			helpers.Field{Key: "shard_id", Value: w.shardID},
			helpers.Field{Key: "error", Value: err},
		)
		return
	}

	plans, err := w.planBuilder.BuildPlans(indexDef)
	if err != nil {
		w.log.Error("failed to build field plans",
			helpers.Field{Key: "shard_id", Value: w.shardID},
			helpers.Field{Key: "error", Value: err},
		)
		return
	}

	req := w.prepareWrite(item.Command, plans)
	if err := w.writer.Index(ctx, req); err != nil {
		w.log.Error("failed to persist document",
			helpers.Field{Key: "shard_id", Value: w.shardID},
			helpers.Field{Key: "error", Value: err},
		)
		return
	}

	latency := time.Since(start)
	span.SetAttributes(attribute.Float64("latency.ms", float64(latency.Milliseconds())))
	w.log.Debug("document indexed",
		helpers.Field{Key: "shard_id", Value: w.shardID},
		helpers.Field{Key: "document_id", Value: item.Command.DocumentID},
		helpers.Field{Key: "latency_ms", Value: latency.Milliseconds()},
	)

	_ = assignment // placeholder to avoid unused variable (future metrics)
}

func (w *ShardWorker) prepareWrite(cmd Command, plans []FieldPlan) DocumentWriteRequest {
	fieldResults := make([]FieldTerms, 0, len(plans))

	for _, plan := range plans {
		rawValue, ok := cmd.Payload[plan.Field.Name]
		if !ok {
			continue
		}

		valueStr := fmt.Sprintf("%v", rawValue)

		var tokens []Token
		if plan.Tokenizer != nil {
			tokens = plan.Tokenizer.Tokenize(valueStr)
		} else {
			tokens = []Token{{Term: valueStr}}
		}

		if plan.Analyzer != nil {
			tokens = plan.Analyzer.Analyze(tokens)
		}

		fieldResults = append(fieldResults, FieldTerms{
			Field:  plan.Field,
			Tokens: tokens,
		})
	}

	return DocumentWriteRequest{
		DocumentID: cmd.DocumentID,
		Fields:     fieldResults,
	}
}
