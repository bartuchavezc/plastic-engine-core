package document

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"

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
	RefreshTime   time.Duration // Time to wait before flushing batched documents
}

func (c *ShardWorkerConfig) applyDefaults() {
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = 4
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 256
	}
	// RefreshTime = 0 means immediate (no batching)
}

// DocumentIndexWriter is the interface for document indexing.
type DocumentIndexWriter interface {
	IndexBatch(ctx context.Context, requests []DocumentWriteRequest) error
}

// ShardWorker processes indexing commands for a particular shard.
type ShardWorker struct {
	shardID string
	store   *shards.Shard

	queue       chan WorkItem
	closing     chan struct{}
	flushSignal chan struct{} // Signal for immediate flush requests

	planBuilder *FieldPlanner
	writer      DocumentIndexWriter
	assignments *AssignmentProvider

	log logger.Logger

	wg sync.WaitGroup

	// Batching fields
	batchMu      sync.Mutex
	currentBatch []batchedItem
	refreshTime  time.Duration
}

type batchedItem struct {
	command  Command
	plans    []FieldPlan
	indexDef indexes.IndexDefinition
	parsed   map[string]any // Validated and parsed payload
}

// NewShardWorker spins up worker goroutines ready to process commands.
func NewShardWorker(shardID string, shard *shards.Shard, cfg ShardWorkerConfig, planner *FieldPlanner, writer DocumentIndexWriter, assignments *AssignmentProvider, log logger.Logger) *ShardWorker {
	cfg.applyDefaults()

	w := &ShardWorker{
		shardID:     shardID,
		store:       shard,
		queue:       make(chan WorkItem, cfg.QueueCapacity),
		closing:     make(chan struct{}),
		flushSignal: make(chan struct{}, 1), // Buffered to avoid blocking

		planBuilder: planner,
		writer:      writer,
		assignments: assignments,

		log:          log,
		currentBatch: make([]batchedItem, 0),
		refreshTime:  cfg.RefreshTime, // May be 0 for immediate processing
	}

	// Workers that prepare and add items to batch (CPU-bound, no I/O in lock)
	for i := 0; i < cfg.MaxWorkers; i++ {
		w.wg.Add(1)
		go w.loop()
	}

	// Single dedicated flush loop (handles all I/O, no race conditions on terms)
	w.wg.Add(1)
	go w.flushLoop()

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
			w.addToBatch(item)
		case <-w.closing:
			return
		}
	}
}

// flushLoop is a dedicated goroutine that handles all flush operations.
// Having a single flusher eliminates race conditions on term registry updates.
func (w *ShardWorker) flushLoop() {
	defer w.wg.Done()

	// Use refresh time for periodic flushes, or a reasonable default
	flushInterval := w.refreshTime
	if flushInterval == 0 {
		flushInterval = 100 * time.Millisecond // Default for immediate mode
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.doFlush()
		case <-w.flushSignal:
			w.doFlush()
		case <-w.closing:
			w.doFlush() // Final flush before shutdown
			return
		}
	}
}

// doFlush atomically swaps the batch and processes it outside the lock.
func (w *ShardWorker) doFlush() {
	// Atomic swap: acquire lock, swap batch, release lock immediately
	w.batchMu.Lock()
	if len(w.currentBatch) == 0 {
		w.batchMu.Unlock()
		return
	}
	batch := w.currentBatch
	w.currentBatch = make([]batchedItem, 0, cap(batch))
	w.batchMu.Unlock()

	// Process entirely outside the lock - no contention with addToBatch
	w.processSwappedBatch(batch)
}

// processSwappedBatch processes a batch that was atomically swapped from currentBatch.
func (w *ShardWorker) processSwappedBatch(batch []batchedItem) {
	if len(batch) == 0 {
		return
	}

	batchSize := len(batch)

	w.log.Info("processSwappedBatch: starting",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "batch_size", Value: batchSize},
	)

	ctx, span := tracer.Start(context.Background(), "batch.flush")
	defer span.End()

	span.SetAttributes(
		attribute.String("shard.id", w.shardID),
		attribute.Int("batch.size", batchSize),
	)

	start := time.Now()

	// Process the entire batch
	if err := w.processBatch(ctx, batch); err != nil {
		w.log.Error("processSwappedBatch: failed to process batch",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "batch_size", Value: batchSize},
			logger.Field{Key: "error", Value: err},
		)
	} else {
		w.log.Info("processSwappedBatch: batch processed successfully",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "batch_size", Value: batchSize},
		)
	}

	latency := time.Since(start)
	span.SetAttributes(attribute.Float64("latency.ms", float64(latency.Milliseconds())))

	// Record metrics
	if mp := metrics.Global(); mp != nil {
		mp.RecordIndexingBatch(ctx, w.shardID, batchSize, latency)
	}

	w.log.Debug("processSwappedBatch: completed",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "batch_size", Value: batchSize},
		logger.Field{Key: "latency_ms", Value: latency.Milliseconds()},
	)
}

func (w *ShardWorker) addToBatch(item WorkItem) {
	w.log.Debug("addToBatch: received item",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "document_id", Value: item.Command.DocumentID},
		logger.Field{Key: "payload_size", Value: len(item.Command.RawPayload)},
	)

	ctx := context.Background()

	// ═══════════════════════════════════════════════════════════════════════
	// OUTSIDE LOCK: All CPU/IO intensive operations happen here
	// This allows other goroutines to add to the batch while we prepare
	// ═══════════════════════════════════════════════════════════════════════

	// Resolve assignment and plans for this item
	_, indexDef, err := w.assignments.AssignmentForShard(ctx, w.shardID)
	if err != nil {
		w.log.Error("addToBatch: failed to resolve assignment",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "document_id", Value: item.Command.DocumentID},
			logger.Field{Key: "error", Value: err},
		)
		return
	}

	w.log.Debug("addToBatch: assignment resolved",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "index_id", Value: indexDef.ID},
		logger.Field{Key: "fields_count", Value: len(indexDef.FieldMappings)},
	)

	// Validate and parse the raw payload using sonic
	validator := NewValidator()
	validationResult := validator.Validate(item.Command.RawPayload, indexDef)
	if !validationResult.Valid {
		w.log.Error("addToBatch: document validation failed",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "document_id", Value: item.Command.DocumentID},
			logger.Field{Key: "error", Value: validationResult.Error},
		)
		return
	}

	w.log.Debug("addToBatch: document validated",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "document_id", Value: item.Command.DocumentID},
		logger.Field{Key: "parsed_fields", Value: len(validationResult.Parsed)},
	)

	plans, err := w.planBuilder.BuildPlans(indexDef)
	if err != nil {
		w.log.Error("addToBatch: failed to build field plans",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "document_id", Value: item.Command.DocumentID},
			logger.Field{Key: "error", Value: err},
		)
		return
	}

	w.log.Debug("addToBatch: plans built",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "plans_count", Value: len(plans)},
	)

	// Prepare the item to add (all data ready, no I/O needed)
	preparedItem := batchedItem{
		command:  item.Command,
		plans:    plans,
		indexDef: indexDef,
		parsed:   validationResult.Parsed,
	}

	// Use index-specific refresh time, fallback to worker config
	refreshTime := w.refreshTime
	if indexDef.RefreshTime > 0 {
		refreshTime = indexDef.RefreshTime
	}

	// ═══════════════════════════════════════════════════════════════════════
	// INSIDE LOCK: Only the append operation (nanoseconds)
	// ═══════════════════════════════════════════════════════════════════════

	w.batchMu.Lock()
	w.currentBatch = append(w.currentBatch, preparedItem)
	batchSize := len(w.currentBatch)
	w.batchMu.Unlock()

	w.log.Debug("addToBatch: item added to batch",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "document_id", Value: item.Command.DocumentID},
		logger.Field{Key: "batch_size", Value: batchSize},
	)

	// If refresh time is 0, signal immediate flush (handled by flush loop)
	if refreshTime == 0 {
		select {
		case w.flushSignal <- struct{}{}:
		default:
			// Signal already pending, flush loop will handle it
		}
	}
}

func (w *ShardWorker) processBatch(ctx context.Context, batch []batchedItem) error {
	if len(batch) == 0 {
		return nil
	}

	w.log.Debug("processBatch: starting",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "batch_size", Value: len(batch)},
	)

	// Use the first item's index definition (assuming all items in batch are for same index)
	indexDef := batch[0].indexDef

	// Get n-gram config from index definition
	ngramConfig := DefaultNgramConfig()
	if indexDef.NgramConfig.MaxLength > 0 {
		ngramConfig = NgramConfig{
			Enabled:   indexDef.NgramConfig.Enabled,
			MinLength: indexDef.NgramConfig.MinLength,
			MaxLength: indexDef.NgramConfig.MaxLength,
		}
	}

	w.log.Debug("processBatch: ngram config",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "ngram_enabled", Value: ngramConfig.Enabled},
		logger.Field{Key: "ngram_min", Value: ngramConfig.MinLength},
		logger.Field{Key: "ngram_max", Value: ngramConfig.MaxLength},
	)

	// Convert batch to DocumentWriteRequests
	requests := make([]DocumentWriteRequest, 0, len(batch))
	for _, item := range batch {
		req := w.prepareWrite(item.command.DocumentID, item.parsed, item.plans, ngramConfig)
		requests = append(requests, req)
	}

	w.log.Debug("processBatch: calling IndexBatch",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "requests_count", Value: len(requests)},
	)

	// Index the batch
	if err := w.writer.IndexBatch(ctx, requests); err != nil {
		w.log.Error("processBatch: IndexBatch failed",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "requests_count", Value: len(requests)},
			logger.Field{Key: "error", Value: err},
		)
		return err
	}

	w.log.Info("processBatch: IndexBatch completed successfully",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "documents_indexed", Value: len(requests)},
	)

	return nil
}

func (w *ShardWorker) prepareWrite(documentID string, payload map[string]any, plans []FieldPlan, ngramConfig NgramConfig) DocumentWriteRequest {
	w.log.Debug("prepareWrite: starting",
		logger.Field{Key: "document_id", Value: documentID},
		logger.Field{Key: "plans_count", Value: len(plans)},
		logger.Field{Key: "payload_fields", Value: len(payload)},
	)

	fieldResults := make([]FieldTerms, 0, len(plans))
	ctx := context.Background()

	for _, plan := range plans {
		rawValue, ok := payload[plan.Field.Name]
		if !ok {
			w.log.Debug("prepareWrite: field not in payload",
				logger.Field{Key: "document_id", Value: documentID},
				logger.Field{Key: "field_name", Value: plan.Field.Name},
			)
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

		// Record tokens metric
		if mp := metrics.Global(); mp != nil && len(tokens) > 0 {
			mp.RecordIndexingTokens(ctx, w.shardID, plan.Field.Name, int64(len(tokens)))
		}

		w.log.Debug("prepareWrite: field processed",
			logger.Field{Key: "document_id", Value: documentID},
			logger.Field{Key: "field_name", Value: plan.Field.Name},
			logger.Field{Key: "tokens_count", Value: len(tokens)},
		)

		fieldResults = append(fieldResults, FieldTerms{
			Field:  plan.Field,
			Tokens: tokens,
		})
	}

	w.log.Debug("prepareWrite: completed",
		logger.Field{Key: "document_id", Value: documentID},
		logger.Field{Key: "fields_with_tokens", Value: len(fieldResults)},
	)

	return DocumentWriteRequest{
		DocumentID:  documentID,
		Fields:      fieldResults,
		NgramConfig: ngramConfig,
	}
}
