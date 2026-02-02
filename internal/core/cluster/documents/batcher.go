package documents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"plastic-engine-core/internal/pkg/logger"
)

// BatcherConfig holds configuration for the DocumentBatcher.
type BatcherConfig struct {
	MaxBatchSize int           // Maximum documents per batch before flush (default: 100)
	MaxWaitTime  time.Duration // Maximum wait time before flush (default: 10ms)
}

// DefaultBatcherConfig returns sensible defaults for batching.
func DefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		MaxBatchSize: 100,
		MaxWaitTime:  10 * time.Millisecond,
	}
}

// BatchDocument represents a document to be sent in a batch.
// The Payload is forwarded as raw bytes - validation happens at the search node.
type BatchDocument struct {
	IndexID    string            `json:"index_id"`
	ShardID    string            `json:"shard_id"`
	DocumentID string            `json:"document_id"`
	Routing    map[string]string `json:"routing,omitempty"`
	Payload    json.RawMessage   `json:"payload"` // Raw bytes, no parsing at coordinator
}

// BulkIngestRequest is the payload sent to the /documents/bulk endpoint.
type BulkIngestRequest struct {
	Documents []BatchDocument `json:"documents"`
}

// BulkIngestResponse is the response from the /documents/bulk endpoint.
type BulkIngestResponse struct {
	Indexed int            `json:"indexed"`
	Errors  []BulkDocError `json:"errors,omitempty"`
}

// BulkDocError represents an error for a specific document in a bulk request.
type BulkDocError struct {
	DocumentID string `json:"document_id"`
	Error      string `json:"error"`
}

// DocumentBatcher groups documents by destination node and flushes batches
// when they reach a size threshold or time limit.
type DocumentBatcher struct {
	config     BatcherConfig
	httpClient *http.Client
	log        logger.Logger

	mu      sync.Mutex
	batches map[string]*nodeBatch // key: nodeID
	closed  bool
}

type nodeBatch struct {
	nodeID    string
	nodeAddr  string
	documents []BatchDocument
	timer     *time.Timer
	errCh     chan error // Channel to communicate flush errors back to callers
	pending   int        // Number of callers waiting for flush
}

// NewDocumentBatcher creates a new batcher with the given configuration.
func NewDocumentBatcher(httpClient *http.Client, cfg BatcherConfig, log logger.Logger) *DocumentBatcher {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 100
	}
	if cfg.MaxWaitTime <= 0 {
		cfg.MaxWaitTime = 10 * time.Millisecond
	}
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &DocumentBatcher{
		config:     cfg,
		httpClient: httpClient,
		log:        log,
		batches:    make(map[string]*nodeBatch),
	}
}

// Add queues a document for batched delivery to the specified node.
// The method may block briefly if a flush is in progress.
func (b *DocumentBatcher) Add(ctx context.Context, nodeID, nodeAddr string, doc BatchDocument) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("batcher is closed")
	}

	batch := b.getOrCreateBatchLocked(nodeID, nodeAddr)
	batch.documents = append(batch.documents, doc)
	batch.pending++

	// Check if we should flush immediately due to size
	if len(batch.documents) >= b.config.MaxBatchSize {
		// Stop the timer since we're flushing now
		if batch.timer != nil {
			batch.timer.Stop()
			batch.timer = nil
		}
		b.flushBatchLocked(batch)
		b.mu.Unlock()
		return nil
	}

	// Ensure timer is running
	b.ensureTimerLocked(batch)
	b.mu.Unlock()

	return nil
}

// AddSync queues a document and waits for the batch to be flushed.
// Returns any error that occurred during the flush.
func (b *DocumentBatcher) AddSync(ctx context.Context, nodeID, nodeAddr string, doc BatchDocument) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("batcher is closed")
	}

	batch := b.getOrCreateBatchLocked(nodeID, nodeAddr)
	batch.documents = append(batch.documents, doc)
	batch.pending++

	// Get the error channel before potentially flushing
	errCh := batch.errCh

	// Check if we should flush immediately due to size
	if len(batch.documents) >= b.config.MaxBatchSize {
		if batch.timer != nil {
			batch.timer.Stop()
			batch.timer = nil
		}
		b.flushBatchLocked(batch)
		b.mu.Unlock()

		// Wait for flush result
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Ensure timer is running
	b.ensureTimerLocked(batch)
	b.mu.Unlock()

	// Wait for flush result
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *DocumentBatcher) getOrCreateBatchLocked(nodeID, nodeAddr string) *nodeBatch {
	batch, ok := b.batches[nodeID]
	if !ok {
		batch = &nodeBatch{
			nodeID:    nodeID,
			nodeAddr:  nodeAddr,
			documents: make([]BatchDocument, 0, b.config.MaxBatchSize),
			errCh:     make(chan error, b.config.MaxBatchSize),
		}
		b.batches[nodeID] = batch
	}
	return batch
}

func (b *DocumentBatcher) ensureTimerLocked(batch *nodeBatch) {
	if batch.timer != nil {
		return
	}

	batch.timer = time.AfterFunc(b.config.MaxWaitTime, func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		// Check if batch still exists and has documents
		if currentBatch, ok := b.batches[batch.nodeID]; ok && len(currentBatch.documents) > 0 {
			currentBatch.timer = nil
			b.flushBatchLocked(currentBatch)
		}
	})
}

func (b *DocumentBatcher) flushBatchLocked(batch *nodeBatch) {
	if len(batch.documents) == 0 {
		return
	}

	// Capture the current state
	docs := batch.documents
	errCh := batch.errCh
	pending := batch.pending
	nodeAddr := batch.nodeAddr
	nodeID := batch.nodeID

	// Reset the batch for new documents
	batch.documents = make([]BatchDocument, 0, b.config.MaxBatchSize)
	batch.errCh = make(chan error, b.config.MaxBatchSize)
	batch.pending = 0

	// Flush asynchronously
	go b.doFlush(nodeID, nodeAddr, docs, errCh, pending)
}

func (b *DocumentBatcher) doFlush(nodeID, nodeAddr string, docs []BatchDocument, errCh chan error, pending int) {
	defer close(errCh)

	err := b.sendBulkRequest(nodeAddr, docs)

	// Send result to all waiting callers
	for i := 0; i < pending; i++ {
		errCh <- err
	}

	if err != nil {
		b.log.Error("bulk flush failed",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "document_count", Value: len(docs)},
			logger.Field{Key: "error", Value: err},
		)
	} else {
		b.log.Debug("bulk flush completed",
			logger.Field{Key: "node_id", Value: nodeID},
			logger.Field{Key: "document_count", Value: len(docs)},
		)
	}
}

func (b *DocumentBatcher) sendBulkRequest(nodeAddr string, docs []BatchDocument) error {
	reqBody := BulkIngestRequest{Documents: docs}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal bulk request: %w", err)
	}

	url := strings.TrimRight(nodeAddr, "/") + "/documents/bulk"
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build bulk request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := b.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("send bulk request to %s: %w", nodeAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("bulk request to %s returned %d: %s", nodeAddr, resp.StatusCode, string(slurp))
	}

	return nil
}

// Flush forces all pending batches to be sent immediately.
func (b *DocumentBatcher) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, batch := range b.batches {
		if batch.timer != nil {
			batch.timer.Stop()
			batch.timer = nil
		}
		b.flushBatchLocked(batch)
	}
}

// Close stops the batcher and flushes any remaining documents.
func (b *DocumentBatcher) Close() {
	b.mu.Lock()
	b.closed = true

	for _, batch := range b.batches {
		if batch.timer != nil {
			batch.timer.Stop()
			batch.timer = nil
		}
		b.flushBatchLocked(batch)
	}
	b.mu.Unlock()
}

// Stats returns current batching statistics.
type BatcherStats struct {
	ActiveBatches  int
	PendingDocs    int
}

// Stats returns current batching statistics.
func (b *DocumentBatcher) Stats() BatcherStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	stats := BatcherStats{
		ActiveBatches: len(b.batches),
	}
	for _, batch := range b.batches {
		stats.PendingDocs += len(batch.documents)
	}
	return stats
}
