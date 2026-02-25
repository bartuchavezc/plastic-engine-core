package document

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/core/search/segment"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var (
	tracer = otel.Tracer("search/indexer")

	// ErrBackpressure is kept for compatibility but no longer returned by the synchronous pipeline.
	ErrBackpressure = errors.New("indexer backpressure: shard queue full")
)

// ShardWorkerConfig controls concurrency for a shard.
type ShardWorkerConfig struct {
	MaxWorkers int
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
}

// DocumentIndexWriter is the interface for document indexing.
type DocumentIndexWriter interface {
	IndexBatch(ctx context.Context, requests []DocumentWriteRequest) error
}

// ShardWorker processes indexing commands for a particular shard.
// It is stateless (no goroutines) and processes each batch synchronously.
type ShardWorker struct {
	shardID string
	store   *shards.Shard

	planBuilder *FieldPlanner
	writer      DocumentIndexWriter
	assignments *AssignmentProvider

	log logger.Logger

	maxWorkers int
}

// NewShardWorker creates a ShardWorker. No goroutines are started.
func NewShardWorker(shardID string, shard *shards.Shard, cfg ShardWorkerConfig, planner *FieldPlanner, writer DocumentIndexWriter, assignments *AssignmentProvider, log logger.Logger) *ShardWorker {
	cfg.applyDefaults()

	return &ShardWorker{
		shardID:     shardID,
		store:       shard,
		planBuilder: planner,
		writer:      writer,
		assignments: assignments,
		log:         log,
		maxWorkers:  cfg.MaxWorkers,
	}
}

// Close is a no-op. Kept for interface compatibility.
func (w *ShardWorker) Close() {}

// Process tokenizes and indexes a batch of commands synchronously.
// Tokenization runs in parallel (up to MaxWorkers goroutines).
// Returns per-document errors; nil means success for that document.
func (w *ShardWorker) Process(ctx context.Context, cmds []Command) []error {
	if len(cmds) == 0 {
		return nil
	}

	errs := make([]error, len(cmds))

	// Resolve index definition once (all docs in a batch share the same shard/index).
	_, indexDef, err := w.assignments.AssignmentForShard(ctx, w.shardID)
	if err != nil {
		for i := range errs {
			errs[i] = fmt.Errorf("resolve assignment: %w", err)
		}
		return errs
	}

	// Build field plans once.
	plans, err := w.planBuilder.BuildPlans(indexDef)
	if err != nil {
		for i := range errs {
			errs[i] = fmt.Errorf("build field plans: %w", err)
		}
		return errs
	}

	ngramConfig := DefaultNgramConfig()
	if indexDef.NgramConfig.MaxLength > 0 {
		ngramConfig = NgramConfig{
			Enabled:   indexDef.NgramConfig.Enabled,
			MinLength: indexDef.NgramConfig.MinLength,
			MaxLength: indexDef.NgramConfig.MaxLength,
		}
	}

	// Tokenize all documents in parallel.
	ctx, tokenSpan := tracer.Start(ctx, "batch.tokenize")
	tokenSpan.SetAttributes(
		attribute.String("shard.id", w.shardID),
		attribute.Int("docs.count", len(cmds)),
	)

	type tokenResult struct {
		writeReq DocumentWriteRequest
		err      error
	}
	results := make([]tokenResult, len(cmds))

	sem := make(chan struct{}, w.maxWorkers)
	var wg sync.WaitGroup

	for i, cmd := range cmds {
		wg.Add(1)
		i, cmd := i, cmd
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			validator := NewValidator()
			validationResult := validator.Validate(cmd.RawPayload, indexDef)
			if !validationResult.Valid {
				results[i] = tokenResult{err: fmt.Errorf("document validation failed: %v", validationResult.Error)}
				return
			}

			writeReq := w.tokenizeDocument(ctx, cmd.DocumentID, validationResult.Parsed, plans, ngramConfig)
			results[i] = tokenResult{writeReq: writeReq}
		}()
	}
	wg.Wait()
	tokenSpan.End()

	// Collect valid requests and track their original indices.
	var validRequests []DocumentWriteRequest
	var validIndices []int
	for i, r := range results {
		if r.err != nil {
			errs[i] = r.err
			w.log.Error("Process: document validation failed",
				logger.Field{Key: "shard_id", Value: w.shardID},
				logger.Field{Key: "document_id", Value: cmds[i].DocumentID},
				logger.Field{Key: "error", Value: r.err},
			)
		} else {
			validRequests = append(validRequests, r.writeReq)
			validIndices = append(validIndices, i)
		}
	}

	if len(validRequests) == 0 {
		return errs
	}

	// NOTE: globalFlushSema removed — it was an artificial throttle that serialized
	// IndexBatch calls across shards. Natural backpressure comes from:
	// - FlushThresholdBytes triggers flush when MemSegment grows too large
	// - highPressure flag triggers emergency flush when heap exceeds 85% GOMEMLIMIT
	// - m.mu.Lock() in AddBatch serializes writes within a single shard (sufficient)

	batchSize := len(validRequests)

	ctx, span := tracer.Start(ctx, "batch.flush")
	defer span.End()

	span.SetAttributes(
		attribute.String("shard.id", w.shardID),
		attribute.Int("batch.size", batchSize),
	)

	start := time.Now()

	w.log.Debug("Process: indexing batch",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "batch_size", Value: batchSize},
	)

	if batchErr := w.writer.IndexBatch(ctx, validRequests); batchErr != nil {
		for _, idx := range validIndices {
			errs[idx] = batchErr
		}
		w.log.Error("Process: IndexBatch failed",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "batch_size", Value: batchSize},
			logger.Field{Key: "error", Value: batchErr},
		)
	} else {
		w.log.Debug("Process: batch indexed successfully",
			logger.Field{Key: "shard_id", Value: w.shardID},
			logger.Field{Key: "batch_size", Value: batchSize},
		)
	}

	latency := time.Since(start)
	span.SetAttributes(attribute.Float64("latency.ms", float64(latency.Milliseconds())))

	if mp := metrics.Global(); mp != nil {
		mp.RecordIndexingBatch(ctx, w.shardID, batchSize, latency)
		if len(validRequests) > 0 {
			mp.RecordDocumentsIngestedBatch(ctx, indexDef.ID, int64(batchSize))
		}
	}

	w.log.Debug("Process: completed",
		logger.Field{Key: "shard_id", Value: w.shardID},
		logger.Field{Key: "batch_size", Value: batchSize},
		logger.Field{Key: "latency_ms", Value: latency.Milliseconds()},
	)

	return errs
}

// tokenizeDocument applies tokenizers and analyzers to all fields of a document.
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
