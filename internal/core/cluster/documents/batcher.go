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

type BatcherConfig struct {
	MaxBatchSize int
	MaxWaitTime  time.Duration
}

func DefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		MaxBatchSize: 100,
		MaxWaitTime:  1 * time.Millisecond,
	}
}

type BatchDocument struct {
	IndexID    string            `json:"index_id"`
	ShardID    string            `json:"shard_id"`
	DocumentID string            `json:"document_id"`
	Routing    map[string]string `json:"routing,omitempty"`
	Payload    json.RawMessage   `json:"payload"`
}

type BulkIngestRequest struct {
	Documents []BatchDocument `json:"documents"`
}

type BulkIngestResponse struct {
	Indexed int            `json:"indexed"`
	Errors  []BulkDocError `json:"errors,omitempty"`
}

type BulkDocError struct {
	DocumentID string `json:"document_id"`
	Error      string `json:"error"`
}

type DocumentBatcher struct {
	config     BatcherConfig
	httpClient *http.Client
	log        logger.Logger

	mu      sync.Mutex
	batches map[string]*nodeBatch
	closed  bool

	inflightWg sync.WaitGroup
	flushSema  chan struct{}
}

type nodeBatch struct {
	nodeID    string
	nodeAddr  string
	documents []BatchDocument
	timer     *time.Timer
	errCh     chan error
	pending   int
}

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
		flushSema:  make(chan struct{}, 16),
	}
}

// Add queues a document for batched delivery and blocks until the batch is
// flushed to the search node. Multiple concurrent callers sharing the same
// batch window are grouped into a single HTTP request.
func (b *DocumentBatcher) Add(ctx context.Context, nodeID, nodeAddr string, doc BatchDocument) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("batcher is closed")
	}

	batch := b.getOrCreateBatchLocked(nodeID, nodeAddr)
	batch.documents = append(batch.documents, doc)
	batch.pending++

	errCh := batch.errCh

	if len(batch.documents) >= b.config.MaxBatchSize {
		if batch.timer != nil {
			batch.timer.Stop()
			batch.timer = nil
		}
		b.flushBatchLocked(batch)
		b.mu.Unlock()

		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	b.ensureTimerLocked(batch)
	b.mu.Unlock()

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

	docs := batch.documents
	errCh := batch.errCh
	pending := batch.pending
	nodeAddr := batch.nodeAddr
	nodeID := batch.nodeID

	batch.documents = make([]BatchDocument, 0, b.config.MaxBatchSize)
	batch.errCh = make(chan error, b.config.MaxBatchSize)
	batch.pending = 0

	b.inflightWg.Add(1)
	go b.doFlush(nodeID, nodeAddr, docs, errCh, pending)
}

func (b *DocumentBatcher) doFlush(nodeID, nodeAddr string, docs []BatchDocument, errCh chan error, pending int) {
	defer b.inflightWg.Done()
	defer close(errCh)

	b.flushSema <- struct{}{}
	defer func() { <-b.flushSema }()

	err := b.sendBulkRequest(nodeAddr, docs)

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

	if resp.StatusCode != http.StatusOK {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("bulk request to %s returned %d: %s", nodeAddr, resp.StatusCode, string(slurp))
	}

	return nil
}

// Close flushes remaining pending batches and waits for all in-flight requests to finish.
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

	b.inflightWg.Wait()
}
