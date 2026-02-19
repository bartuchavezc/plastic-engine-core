package document

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/segment"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var (
	tracer = otel.Tracer("search/indexer")

	// ErrBackpressure indicates the shard queue is full.
	ErrBackpressure = errors.New("indexer backpressure: shard queue full")

	// globalFlushSema limits concurrent flush operations across all shards.
	// Sized at init time by TuneForNode based on detected CPU/memory.
	// Override via PLASTIC_CPUS or PLASTIC_MEMORY_MB environment variables.
	globalFlushSema = initFlushSema()
)

func initFlushSema() chan struct{} {
	nodeCfg := segment.TuneForNode(segment.DetectResources())
	return make(chan struct{}, nodeCfg.FlushConcurrency)
}

// SetFlushConcurrency overrides the global flush semaphore capacity.
// Must be called before any ShardWorkers are created.
func SetFlushConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	globalFlushSema = make(chan struct{}, n)
}

// ShardWorkerConfig controls concurrency and queueing for a shard.
type ShardWorkerConfig struct {
	MaxWorkers    int
	QueueCapacity int
	MaxBatchSize  int           // Max items in a batch before forcing flush (0 = 512)
	RefreshTime   time.Duration // Time to wait before flushing batched documents
}

// workerDefaults is computed once at init from TuneForNode so applyDefaults()
// uses hardware-appropriate values instead of static constants.
var workerDefaults = func() segment.NodeConfig {
	return segment.TuneForNode(segment.DetectResources())
}()

func (c *ShardWorkerConfig) applyDefaults() {
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = workerDefaults.WorkerMaxWorkers
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = workerDefaults.WorkerQueueCapacity
	}
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = workerDefaults.WorkerMaxBatchSize
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
	maxBatchSize int
}

type batchedItem struct {
	// Pre-tokenized write request, ready for IndexBatch.
	// Tokenization happens in the worker pool (parallel) not the flush loop (serial).
	writeReq DocumentWriteRequest
	indexDef indexes.IndexDefinition
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
		currentBatch: make([]batchedItem, 0, 64),
		refreshTime:  cfg.RefreshTime, // May be 0 for immediate processing
		maxBatchSize: cfg.MaxBatchSize,
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
		if mp := metrics.Global(); mp != nil {
			mp.RecordIndexingQueueChange(ctx, w.shardID, 1)
		}
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
	w.currentBatch = make([]batchedItem, 0, 64) // fixed capacity to avoid high-water-mark leak
	w.batchMu.Unlock()

	// Process entirely outside the lock - no contention with addToBatch
	w.processSwappedBatch(batch)
}

// processSwappedBatch processes a batch that was atomically swapped from currentBatch.
func (w *ShardWorker) processSwappedBatch(batch []batchedItem) {
	if len(batch) == 0 {
		return
	}

	// Acquire global flush semaphore to limit Pebble contention
	globalFlushSema <- struct{}{}
	defer func() { <-globalFlushSema }()

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
	batchErr := w.processBatch(ctx, batch)
	if batchErr != nil {
		w.log.Error("processSwappedBatch: failed to process batch",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "batch_size", Value: batchSize},
			logger.Field{Key: "error", Value: batchErr},
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
		mp.RecordIndexingQueueChange(ctx, w.shardID, -int64(batchSize))
		if batchErr == nil && len(batch) > 0 {
			mp.RecordDocumentsIngestedBatch(ctx, batch[0].indexDef.ID, int64(batchSize))
		}
	}

	w.log.Debug("processSwappedBatch: completed",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "batch_size", Value: batchSize},
		logger.Field{Key: "latency_ms", Value: latency.Milliseconds()},
	)
}

func (w *ShardWorker) addToBatch(item WorkItem) {
	// ═══════════════════════════════════════════════════════════════════════
	// OUTSIDE LOCK: All CPU-intensive work happens here in the worker pool.
	// This runs across MaxWorkers goroutines in parallel.
	// ═══════════════════════════════════════════════════════════════════════

	ctx := context.Background()

	_, indexDef, err := w.assignments.AssignmentForShard(ctx, w.shardID)
	if err != nil {
		w.log.Error("addToBatch: failed to resolve assignment",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "document_id", Value: item.Command.DocumentID},
			logger.Field{Key: "error", Value: err},
		)
		return
	}

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

	// Release raw payload immediately after parsing.
	item.Command.RawPayload = nil

	plans, err := w.planBuilder.BuildPlans(indexDef)
	if err != nil {
		w.log.Error("addToBatch: failed to build field plans",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "document_id", Value: item.Command.DocumentID},
			logger.Field{Key: "error", Value: err},
		)
		return
	}

	// Tokenize and analyze all fields here in the worker pool (parallel).
	// Previously this happened in the flush loop (single goroutine) — that was
	// the main CPU bottleneck. Now all MaxWorkers goroutines tokenize concurrently.
	ngramConfig := DefaultNgramConfig()
	if indexDef.NgramConfig.MaxLength > 0 {
		ngramConfig = NgramConfig{
			Enabled:   indexDef.NgramConfig.Enabled,
			MinLength: indexDef.NgramConfig.MinLength,
			MaxLength: indexDef.NgramConfig.MaxLength,
		}
	}
	writeReq := w.tokenizeDocument(ctx, item.Command.DocumentID, validationResult.Parsed, plans, ngramConfig)

	// Use index-specific refresh time, fallback to worker config
	refreshTime := w.refreshTime
	if indexDef.RefreshTime > 0 {
		refreshTime = indexDef.RefreshTime
	}

	// ═══════════════════════════════════════════════════════════════════════
	// INSIDE LOCK: Only the slice append (nanoseconds)
	// ═══════════════════════════════════════════════════════════════════════

	w.batchMu.Lock()
	w.currentBatch = append(w.currentBatch, batchedItem{
		writeReq: writeReq,
		indexDef: indexDef,
	})
	batchSize := len(w.currentBatch)
	w.batchMu.Unlock()

	if refreshTime == 0 || batchSize >= w.maxBatchSize {
		select {
		case w.flushSignal <- struct{}{}:
		default:
		}
	}
}

// processBatch indexes a pre-tokenized batch. All CPU work (tokenization) was
// already done in the worker pool by addToBatch — this is now pure I/O.
func (w *ShardWorker) processBatch(ctx context.Context, batch []batchedItem) error {
	if len(batch) == 0 {
		return nil
	}

	requests := make([]DocumentWriteRequest, len(batch))
	for i, item := range batch {
		requests[i] = item.writeReq
	}

	if err := w.writer.IndexBatch(ctx, requests); err != nil {
		w.log.Error("processBatch: IndexBatch failed",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "requests_count", Value: len(requests)},
			logger.Field{Key: "error", Value: err},
		)
		return err
	}

	w.log.Info("processBatch: completed",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "documents_indexed", Value: len(requests)},
	)

	return nil
}

// tokenizeDocument applies tokenizers and analyzers to all fields of a document.
// Called from addToBatch (worker pool, parallel) instead of processBatch (flush loop, serial).
func (w *ShardWorker) tokenizeDocument(ctx context.Context, documentID string, payload map[string]any, plans []FieldPlan, ngramConfig NgramConfig) DocumentWriteRequest {
	fieldResults := make([]FieldTerms, 0, len(plans))

	for _, plan := range plans {
		rawValue, ok := payload[plan.Field.Name]
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

		if mp := metrics.Global(); mp != nil && len(tokens) > 0 {
			mp.RecordIndexingTokens(ctx, w.shardID, plan.Field.Name, int64(len(tokens)))
		}

		fieldResults = append(fieldResults, FieldTerms{
			Field:  plan.Field,
			Tokens: tokens,
		})
	}

	return DocumentWriteRequest{
		DocumentID:  documentID,
		Fields:      fieldResults,
		NgramConfig: ngramConfig,
	}
}
