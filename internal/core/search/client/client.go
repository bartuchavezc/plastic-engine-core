package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/cluster/nodes"
	node "plastic-engine-core/internal/core/search"
)

const (
	defaultJoinPath      = "/cluster/join"
	defaultHeartbeatPath = "/cluster/heartbeat"
)

// Client implements the node.ClusterClient interface using HTTP calls to the cluster.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

var tracer = otel.Tracer("cluster/client")

// NewClient builds a ClusterClient targeting the given coordinator base URL.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 5 * time.Second,
		}
	}

	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
	}
}

// Join registers the node in the coordinator and returns shard assignments.
func (c *Client) Join(ctx context.Context, info node.NodeInfo) (node.JoinResponse, error) {
	ctx, span := tracer.Start(ctx, "ClusterClient.Join")
	defer span.End()

	payload := nodes.JoinRequest{
		NodeID:        info.ID,
		Role:          info.Role,
		AdvertiseAddr: info.AdvertiseAddr,
		DataDir:       info.DataDir,
	}

	var response nodes.JoinResponse
	if err := c.postJSON(ctx, defaultJoinPath, payload, &response); err != nil {
		return node.JoinResponse{}, fmt.Errorf("join coordinator: %w", err)
	}

	span.SetAttributes(
		attribute.String("node.id", response.NodeID),
		attribute.Int("shards.assigned", len(response.Shards)),
	)

	return node.JoinResponse{
		NodeID: response.NodeID,
		Shards: response.Shards,
	}, nil
}

// Heartbeat sends the current node status to the cluster and returns mapping updates.
func (c *Client) Heartbeat(ctx context.Context, report node.HeartbeatReport) (node.HeartbeatResponse, error) {
	ctx, span := tracer.Start(ctx, "ClusterClient.Heartbeat")
	defer span.End()

	payload := nodes.HeartbeatRequest{
		NodeID: report.NodeID,
		Shards: report.Shards,
	}

	span.SetAttributes(
		attribute.String("node.id", report.NodeID),
		attribute.Int("shards.count", len(report.Shards)),
	)

	var response nodes.HeartbeatResponse
	if err := c.postJSON(ctx, defaultHeartbeatPath, payload, &response); err != nil {
		return node.HeartbeatResponse{}, err
	}

	return node.HeartbeatResponse{
		Status:         response.Status,
		MappingUpdates: response.MappingUpdates,
	}, nil
}

func (c *Client) postJSON(ctx context.Context, path string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("coordinator responded with %d: %s", resp.StatusCode, string(slurp))
	}

	if out == nil {
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

// DefinitionFetcher fetches index definitions from the coordinator.
type DefinitionFetcher struct {
	client *Client
}

// NewDefinitionFetcher creates a fetcher that uses the cluster client.
func NewDefinitionFetcher(client *Client) *DefinitionFetcher {
	return &DefinitionFetcher{
		client: client,
	}
}

// FetchIndexDefinition retrieves an index definition from the coordinator.
func (f *DefinitionFetcher) FetchIndexDefinition(ctx context.Context, indexID string) (indexes.IndexDefinition, error) {
	ctx, span := tracer.Start(ctx, "DefinitionFetcher.FetchIndexDefinition")
	defer span.End()

	path := fmt.Sprintf("/indexes/%s", indexID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.client.baseURL+path, nil)
	if err != nil {
		return indexes.IndexDefinition{}, fmt.Errorf("build request: %w", err)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := f.client.httpClient.Do(req)
	if err != nil {
		return indexes.IndexDefinition{}, fmt.Errorf("fetch index definition: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return indexes.IndexDefinition{}, fmt.Errorf("index %s not found", indexID)
	}
	if resp.StatusCode >= 400 {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return indexes.IndexDefinition{}, fmt.Errorf("coordinator responded with %d: %s", resp.StatusCode, string(slurp))
	}

	var indexResp struct {
		ID               string `json:"id"`
		Name             string `json:"name"`
		ShardStrategy    string `json:"shard_strategy"`
		ShardTemplate    string `json:"shard_template"`
		ShardConfig      map[string]any `json:"shard_config"`
		DefaultAnalyzer  string `json:"default_analyzer"`
		DefaultTokenizer string `json:"default_tokenizer"`
		MappingVersion   int `json:"mapping_version"`
		FieldMappings    []struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			Analyzer  string `json:"analyzer,omitempty"`
			Tokenizer string `json:"tokenizer,omitempty"`
			Stored    bool `json:"stored"`
			Required  bool `json:"required"`
			Index     bool `json:"index"`
		} `json:"field_mappings"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&indexResp); err != nil {
		return indexes.IndexDefinition{}, fmt.Errorf("decode response: %w", err)
	}

	// Convert to IndexDefinition
	def := indexes.IndexDefinition{
		ID:               indexResp.ID,
		Name:             indexResp.Name,
		ShardStrategy:    indexes.ShardStrategy(indexResp.ShardStrategy),
		ShardTemplate:    indexResp.ShardTemplate,
		DefaultAnalyzer:  indexResp.DefaultAnalyzer,
		DefaultTokenizer: indexResp.DefaultTokenizer,
		MappingVersion:   indexResp.MappingVersion,
		CreatedAt:        indexResp.CreatedAt,
		UpdatedAt:        indexResp.UpdatedAt,
	}

	// Convert field mappings
	def.FieldMappings = make([]indexes.FieldMapping, 0, len(indexResp.FieldMappings))
	for _, fm := range indexResp.FieldMappings {
		def.FieldMappings = append(def.FieldMappings, indexes.FieldMapping{
			Name:      fm.Name,
			Type:      indexes.FieldType(fm.Type),
			Analyzer:  fm.Analyzer,
			Tokenizer: fm.Tokenizer,
			Stored:    fm.Stored,
			Required:  fm.Required,
			Indexed:   fm.Index,
		})
	}

	span.SetAttributes(
		attribute.String("index.id", def.ID),
		attribute.Int("mapping.version", def.MappingVersion),
		attribute.Int("fields.count", len(def.FieldMappings)),
	)

	return def, nil
}
