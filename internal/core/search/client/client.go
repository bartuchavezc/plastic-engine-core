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

	"plastic-engine-core/internal/core/cluster"
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

	payload := cluster.JoinRequest{
		NodeID:        info.ID,
		Role:          info.Role,
		AdvertiseAddr: info.AdvertiseAddr,
		DataDir:       info.DataDir,
	}

	var response cluster.JoinResponse
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

// Heartbeat sends the current node status to the cluster.
func (c *Client) Heartbeat(ctx context.Context, report node.HeartbeatReport) error {
	ctx, span := tracer.Start(ctx, "ClusterClient.Heartbeat")
	defer span.End()

	payload := cluster.HeartbeatRequest{
		NodeID: report.NodeID,
		Shards: report.Shards,
	}

	span.SetAttributes(
		attribute.String("node.id", report.NodeID),
		attribute.Int("shards.count", len(report.Shards)),
	)

	return c.postJSON(ctx, defaultHeartbeatPath, payload, nil)
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
