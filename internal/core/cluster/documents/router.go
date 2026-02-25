package documents

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/indexes/sharding"
	"plastic-engine-core/internal/core/cluster/shards"
	"plastic-engine-core/internal/pkg/logger"
)

// IndexRepository resolves index definitions.
type IndexRepository interface {
	GetIndex(ctx context.Context, id string) (indexes.IndexDefinition, error)
}

// routeCache caches the coordinator's three per-document DB lookups:
// index definition, primary shard info, and node advertise address.
// These are effectively static during ingestion (change only on cluster rebalance).
// TTL of 10s keeps the cache fresh without requiring explicit invalidation plumbing.
type routeCache struct {
	mu  sync.RWMutex
	m   map[string]routeCacheEntry
	ttl time.Duration
}

type routeCacheEntry struct {
	val    any
	expiry time.Time
}

func newRouteCache(ttl time.Duration) *routeCache {
	return &routeCache{m: make(map[string]routeCacheEntry), ttl: ttl}
}

func (c *routeCache) get(key string) (any, bool) {
	c.mu.RLock()
	e, ok := c.m[key]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expiry) {
		return nil, false
	}
	return e.val, true
}

func (c *routeCache) set(key string, val any) {
	c.mu.Lock()
	c.m[key] = routeCacheEntry{val: val, expiry: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}

// Router encapsulates the dependencies required to execute the ingestion workflow.
// Mapping validation is performed at the search node; the coordinator only extracts
// routing information and forwards raw bytes.
type Router struct {
	IndexRepo  IndexRepository
	ShardRepo  *shards.Repository
	HTTPClient *http.Client
	Batcher    *DocumentBatcher
	Logger     logger.Logger
	cache      *routeCache
}

// RouterConfig holds configuration for creating a Router.
type RouterConfig struct {
	DB         *sql.DB
	IndexRepo  IndexRepository
	HTTPClient *http.Client
	Batcher    *DocumentBatcher
	Logger     logger.Logger
}

// NewRouter creates a document router.
func NewRouter(db *sql.DB, indexRepo IndexRepository, httpClient *http.Client, log logger.Logger) *Router {
	return NewRouterWithConfig(RouterConfig{
		DB:         db,
		IndexRepo:  indexRepo,
		HTTPClient: httpClient,
		Logger:     log,
	})
}

// NewRouterWithConfig creates a document router with full configuration.
func NewRouterWithConfig(cfg RouterConfig) *Router {
	log := cfg.Logger
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &Router{
		IndexRepo:  cfg.IndexRepo,
		ShardRepo:  shards.NewRepository(cfg.DB),
		HTTPClient: cfg.HTTPClient,
		Batcher:    cfg.Batcher,
		Logger:     log,
		cache:      newRouteCache(10 * time.Second),
	}
}

// Handle executes the ingestion pipeline by routing the document to the appropriate shard.
func (r *Router) Handle(ctx context.Context, req Request) error {
	ctx, span := otel.Tracer("cluster.documents").Start(ctx, "Router.Handle")
	defer span.End()

	if r.IndexRepo == nil {
		return fmt.Errorf("document router missing index repository")
	}
	if r.ShardRepo == nil {
		return fmt.Errorf("document router missing shard repository")
	}
	if r.HTTPClient == nil {
		return fmt.Errorf("document router missing http client")
	}

	if err := validateRequest(req); err != nil {
		return err
	}

	def, err := r.getIndexCached(ctx, req.IndexID)
	if err != nil {
		return err
	}

	shardKey, err := sharding.ComputeShardKey(def, req.DocumentID, req.Payload, sharding.RoutingMetadata(req.Routing))
	if err != nil {
		return err
	}

	shardInfo, err := r.getPrimaryShardCached(ctx, req.IndexID, shardKey)
	if err != nil {
		return err
	}

	nodeInfo, err := r.getNodeCached(ctx, shardInfo.PrimaryNode)
	if err != nil {
		return err
	}

	if strings.TrimSpace(nodeInfo.AdvertiseAddr) == "" {
		return fmt.Errorf("node %s missing advertise address", nodeInfo.ID)
	}

	if r.Batcher != nil {
		doc := BatchDocument{
			IndexID:    req.IndexID,
			ShardID:    shardInfo.ID,
			DocumentID: req.DocumentID,
			Routing:    req.Routing,
			Payload:    req.Payload,
		}

		if err := r.Batcher.Add(ctx, nodeInfo.ID, nodeInfo.AdvertiseAddr, doc); err != nil {
			return fmt.Errorf("batch document for node %s: %w", nodeInfo.ID, err)
		}

		return nil
	}

	return r.sendDirectRequest(ctx, nodeInfo, req, shardInfo.ID)
}

func (r *Router) getIndexCached(ctx context.Context, indexID string) (indexes.IndexDefinition, error) {
	key := "idx:" + indexID
	if v, ok := r.cache.get(key); ok {
		return v.(indexes.IndexDefinition), nil
	}
	def, err := r.IndexRepo.GetIndex(ctx, indexID)
	if err != nil {
		return indexes.IndexDefinition{}, err
	}
	r.cache.set(key, def)
	return def, nil
}

func (r *Router) getPrimaryShardCached(ctx context.Context, indexID, shardKey string) (shards.Info, error) {
	key := "shard:" + indexID + ":" + shardKey
	if v, ok := r.cache.get(key); ok {
		return v.(shards.Info), nil
	}
	info, err := r.ShardRepo.LookupPrimaryShard(ctx, indexID, shardKey)
	if err != nil {
		if errors.Is(err, shards.ErrPrimaryNotFound) {
			return shards.Info{}, ErrShardNotFound
		}
		return shards.Info{}, err
	}
	r.cache.set(key, info)
	return info, nil
}

func (r *Router) getNodeCached(ctx context.Context, nodeID string) (shards.NodeInfo, error) {
	key := "node:" + nodeID
	if v, ok := r.cache.get(key); ok {
		return v.(shards.NodeInfo), nil
	}
	info, err := r.ShardRepo.LookupNode(ctx, nodeID)
	if err != nil {
		return shards.NodeInfo{}, err
	}
	r.cache.set(key, info)
	return info, nil
}

// BulkError describes a per-document error in a bulk operation.
type BulkError struct {
	Index int
	DocID string
	Err   error
}

// HandleBulk routes and forwards an entire bulk in one pass.
// All documents must belong to the same index.
//
// 1. Resolves shard/node for each doc (cache hits after first doc).
// 2. Groups by destination node.
// 3. Sends one HTTP POST /documents/bulk per node (parallel if multiple nodes).
// Returns per-document errors; nil entries mean success.
func (r *Router) HandleBulk(ctx context.Context, indexID string, docs []BatchDocument) []BulkError {
	if r.IndexRepo == nil || r.ShardRepo == nil || r.HTTPClient == nil {
		err := fmt.Errorf("document router not fully configured")
		out := make([]BulkError, len(docs))
		for i := range docs {
			out[i] = BulkError{Index: i, DocID: docs[i].DocumentID, Err: err}
		}
		return out
	}

	def, err := r.getIndexCached(ctx, indexID)
	if err != nil {
		out := make([]BulkError, len(docs))
		for i := range docs {
			out[i] = BulkError{Index: i, DocID: docs[i].DocumentID, Err: err}
		}
		return out
	}

	// Group docs by destination node.
	type nodeGroup struct {
		addr    string
		docs    []BatchDocument
		indices []int // original index in input slice
	}
	groups := make(map[string]*nodeGroup) // nodeID → group

	var routeErrors []BulkError

	for i := range docs {
		doc := &docs[i]
		doc.IndexID = indexID

		shardKey, err := sharding.ComputeShardKey(def, doc.DocumentID, doc.Payload, sharding.RoutingMetadata(doc.Routing))
		if err != nil {
			routeErrors = append(routeErrors, BulkError{Index: i, DocID: doc.DocumentID, Err: err})
			continue
		}

		shardInfo, err := r.getPrimaryShardCached(ctx, indexID, shardKey)
		if err != nil {
			routeErrors = append(routeErrors, BulkError{Index: i, DocID: doc.DocumentID, Err: err})
			continue
		}
		doc.ShardID = shardInfo.ID

		nodeInfo, err := r.getNodeCached(ctx, shardInfo.PrimaryNode)
		if err != nil {
			routeErrors = append(routeErrors, BulkError{Index: i, DocID: doc.DocumentID, Err: err})
			continue
		}

		g := groups[nodeInfo.ID]
		if g == nil {
			g = &nodeGroup{addr: nodeInfo.AdvertiseAddr}
			groups[nodeInfo.ID] = g
		}
		g.docs = append(g.docs, *doc)
		g.indices = append(g.indices, i)
	}

	// Send one bulk HTTP POST per node, in parallel if multiple nodes.
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, g := range groups {
		wg.Add(1)
		go func(ng *nodeGroup) {
			defer wg.Done()
			if err := r.sendBulkToNode(ctx, ng.addr, ng.docs); err != nil {
				mu.Lock()
				for j, idx := range ng.indices {
					routeErrors = append(routeErrors, BulkError{
						Index: idx,
						DocID: ng.docs[j].DocumentID,
						Err:   err,
					})
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	return routeErrors
}

// sendBulkToNode sends a batch of documents directly to a search node via HTTP POST.
func (r *Router) sendBulkToNode(ctx context.Context, nodeAddr string, docs []BatchDocument) error {
	reqBody := BulkIngestRequest{Documents: docs}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal bulk request: %w", err)
	}

	url := strings.TrimRight(nodeAddr, "/") + "/documents/bulk"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build bulk request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.HTTPClient.Do(httpReq)
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

func (r *Router) sendDirectRequest(ctx context.Context, nodeInfo shards.NodeInfo, req Request, shardID string) error {
	forwardPayload := map[string]any{
		"index_id":    req.IndexID,
		"shard_id":    shardID,
		"document_id": req.DocumentID,
		"routing":     req.Routing,
		"payload":     req.Payload,
	}

	body, err := json.Marshal(forwardPayload)
	if err != nil {
		return fmt.Errorf("marshal forward payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(nodeInfo.AdvertiseAddr, "/")+"/documents", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build forward request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.HTTPClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("forward document to node %s: %w", nodeInfo.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("node %s returned %d: %s", nodeInfo.ID, resp.StatusCode, string(slurp))
	}

	return nil
}

func validateRequest(req Request) error {
	if strings.TrimSpace(req.IndexID) == "" {
		return fmt.Errorf("index id is required")
	}
	if strings.TrimSpace(req.DocumentID) == "" {
		return fmt.Errorf("document id is required")
	}
	return nil
}
