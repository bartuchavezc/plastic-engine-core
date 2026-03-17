package document

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/indexstore"
	shards "plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// DocumentWriteRequest represents the terms generated for a document.
type DocumentWriteRequest struct {
	DocumentID string
	Fields     []FieldTerms
}

// FieldTerms groups the tokens produced for a field.
type FieldTerms struct {
	Field  indexes.FieldMapping
	Tokens []Token
}

var (
	tracer = otel.Tracer("search/indexer")

	// ErrBackpressure is kept for compatibility but no longer returned by the synchronous pipeline.
	ErrBackpressure = errors.New("indexer backpressure: shard queue full")
)

// termIndexPool reuses map[string]int across buildDocumentBatch calls.
// Avoids per-field heap allocations (~1-2KB each) that drive GC pressure
// during high-throughput ingestion (512 docs × N fields = hundreds of maps/chunk).
var termIndexPool = sync.Pool{
	New: func() any {
		return make(map[string]int, 32)
	},
}

// indexBatchChunkSize is the max docs per IndexDocumentBatch call.
const indexBatchChunkSize = 2048

// ShardWorkerConfig controls concurrency for a shard.
type ShardWorkerConfig struct {
	MaxWorkers int
}

// workerDefaults is computed once at init from TuneForNode so applyDefaults()
// uses hardware-appropriate values instead of static constants.
var workerDefaults = func() shards.NodeConfig {
	return shards.TuneForNode(shards.DetectResources())
}()

func (c *ShardWorkerConfig) applyDefaults() {
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = workerDefaults.WorkerMaxWorkers
	}
}

// ShardWorker processes indexing commands for a particular shard.
// It is stateless (no goroutines) and processes each batch synchronously.
// Talks directly to indexstore.Manager — no intermediate writer layer.
type ShardWorker struct {
	shardID    string
	store      *shards.Shard
	segmentMgr *indexstore.Manager

	planBuilder *FieldPlanner
	assignments *AssignmentProvider

	log logger.Logger

	maxWorkers int
}

// NewShardWorker creates a ShardWorker. No goroutines are started.
func NewShardWorker(shardID string, shard *shards.Shard, cfg ShardWorkerConfig, planner *FieldPlanner, segmentMgr *indexstore.Manager, assignments *AssignmentProvider, log logger.Logger) *ShardWorker {
	cfg.applyDefaults()

	return &ShardWorker{
		shardID:     shardID,
		store:       shard,
		segmentMgr:  segmentMgr,
		planBuilder: planner,
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

	// Tokenize all documents in parallel and build postings directly.
	ctx, tokenSpan := tracer.Start(ctx, "batch.tokenize")
	tokenSpan.SetAttributes(
		attribute.String("shard.id", w.shardID),
		attribute.Int("docs.count", len(cmds)),
	)

	type tokenResult struct {
		doc indexstore.DocumentBatch
		ok  bool
		err error
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

			writeReq := w.tokenizeDocument(ctx, cmd.DocumentID, validationResult.Parsed, plans)
			doc, ok := buildDocumentBatch(writeReq)
			results[i] = tokenResult{doc: doc, ok: ok}
		}()
	}
	wg.Wait()
	tokenSpan.End()

	// Collect valid docs and track their original indices.
	validDocs := make([]indexstore.DocumentBatch, 0, len(cmds))
	var validIndices []int
	for i, r := range results {
		if r.err != nil {
			errs[i] = r.err
			w.log.Error("Process: document validation failed",
				logger.Field{Key: "shard_id", Value: w.shardID},
				logger.Field{Key: "document_id", Value: cmds[i].DocumentID},
				logger.Field{Key: "error", Value: r.err},
			)
		} else if r.ok {
			validDocs = append(validDocs, r.doc)
			validIndices = append(validIndices, i)
		}
	}

	if len(validDocs) == 0 {
		return errs
	}

	batchSize := len(validDocs)

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

	// Write directly to segment manager in chunks.
	chunkSize := w.segmentMgr.EffectiveBatchSize(indexBatchChunkSize)
	var batchErr error
	for chunkStart := 0; chunkStart < len(validDocs); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(validDocs) {
			chunkEnd = len(validDocs)
		}
		if err := w.segmentMgr.IndexDocumentBatch(ctx, validDocs[chunkStart:chunkEnd]); err != nil {
			batchErr = err
			break
		}
	}

	if batchErr != nil {
		for _, idx := range validIndices {
			errs[idx] = batchErr
		}
		w.log.Error("Process: IndexDocumentBatch failed",
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
		if batchSize > 0 {
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

// buildDocumentBatch converts a tokenized DocumentWriteRequest into a indexstore.DocumentBatch.
// Returns (batch, false) if the document has no indexable terms.
func buildDocumentBatch(req DocumentWriteRequest) (indexstore.DocumentBatch, bool) {
	if req.DocumentID == "" {
		return indexstore.DocumentBatch{}, false
	}

	fieldTerms := make(map[string][]indexstore.TermPosting, len(req.Fields))

	for _, field := range req.Fields {
		name := field.Field.Name
		if name == "" {
			continue
		}
		tokens := field.Tokens
		if len(tokens) == 0 {
			continue
		}

		termIndex := termIndexPool.Get().(map[string]int)
		clear(termIndex)
		postings := make([]indexstore.TermPosting, 0, len(tokens)/2)

		for _, token := range tokens {
			if token.Term == "" {
				continue
			}
			if idx, exists := termIndex[token.Term]; exists {
				postings[idx].TF++
				postings[idx].Positions = append(postings[idx].Positions, token.Position)
				postings[idx].SentenceIDs = append(postings[idx].SentenceIDs, token.SentenceID)
			} else {
				termIndex[token.Term] = len(postings)
				postings = append(postings, indexstore.TermPosting{
					Term:        token.Term,
					TF:          1,
					Positions:   []int{token.Position},
					SentenceIDs: []int{token.SentenceID},
				})
			}
		}

		termIndexPool.Put(termIndex)

		if len(postings) > 0 {
			fieldTerms[name] = postings
		}
	}

	if len(fieldTerms) == 0 {
		return indexstore.DocumentBatch{}, false
	}

	return indexstore.DocumentBatch{
		DocID:      req.DocumentID,
		FieldTerms: fieldTerms,
	}, true
}

// tokenizeDocument applies tokenizers and analyzers to all fields of a document.
func (w *ShardWorker) tokenizeDocument(ctx context.Context, documentID string, payload map[string]any, plans []FieldPlan) DocumentWriteRequest {
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
		DocumentID: documentID,
		Fields:     fieldResults,
	}
}
