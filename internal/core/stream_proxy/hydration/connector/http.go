package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"plastic-engine-core/internal/core/stream_proxy/hydration"
)

const (
	// HTTPConnectorName is the identifier for the HTTP connector.
	HTTPConnectorName = "http"

	// Default settings
	defaultTimeout = 10 * time.Second
	defaultMethod  = http.MethodGet
)

// HTTPConfig holds configuration for the HTTP connector.
type HTTPConfig struct {
	BaseURL string            // Base URL with {key} placeholder
	Headers map[string]string // Additional headers
	Method  string            // HTTP method (default: GET)
	Timeout time.Duration     // Request timeout
}

// ParseHTTPConfig extracts HTTP config from settings map.
func ParseHTTPConfig(settings map[string]string) HTTPConfig {
	cfg := HTTPConfig{
		BaseURL: settings["base_url"],
		Method:  settings["method"],
		Headers: make(map[string]string),
	}

	if cfg.Method == "" {
		cfg.Method = defaultMethod
	}

	// Parse timeout
	if t := settings["timeout"]; t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			cfg.Timeout = d
		}
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	// Parse headers (format: "Header-Name:value")
	if h := settings["headers"]; h != "" {
		for _, pair := range strings.Split(h, ";") {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) == 2 {
				cfg.Headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	return cfg
}

// HTTPConnector fetches documents from external HTTP APIs.
type HTTPConnector struct {
	config HTTPConfig
	client *http.Client
}

// NewHTTPConnector creates an HTTP connector with the given config.
func NewHTTPConnector(config HTTPConfig) *HTTPConnector {
	return &HTTPConnector{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// NewHTTPConnectorFromSettings creates an HTTP connector from settings map.
func NewHTTPConnectorFromSettings(settings map[string]string) *HTTPConnector {
	return NewHTTPConnector(ParseHTTPConfig(settings))
}

// Name returns the connector type.
func (c *HTTPConnector) Name() string {
	return HTTPConnectorName
}

// Fetch retrieves a document by key from the external HTTP API.
func (c *HTTPConnector) Fetch(ctx context.Context, key string) (hydration.Document, error) {
	url := c.buildURL(key)

	req, err := http.NewRequestWithContext(ctx, c.config.Method, url, nil)
	if err != nil {
		return hydration.Document{}, fmt.Errorf("create request: %w", err)
	}

	// Add headers
	for k, v := range c.config.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return hydration.Document{}, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	// Handle 404 as not found
	if resp.StatusCode == http.StatusNotFound {
		return hydration.Document{
			ID:    key,
			Found: false,
		}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return hydration.Document{}, fmt.Errorf("http status %d for key %s", resp.StatusCode, key)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return hydration.Document{}, fmt.Errorf("read response: %w", err)
	}

	var source map[string]any
	if err := json.Unmarshal(body, &source); err != nil {
		return hydration.Document{}, fmt.Errorf("decode response: %w", err)
	}

	return hydration.Document{
		ID:     key,
		Source: source,
		Metadata: hydration.DocumentMeta{
			Size:        int64(len(body)),
			ContentType: resp.Header.Get("Content-Type"),
		},
		Found: true,
	}, nil
}

// FetchBatch retrieves multiple documents. HTTP connector does this sequentially.
// For better performance, consider implementing batch endpoints in your API.
func (c *HTTPConnector) FetchBatch(ctx context.Context, keys []string) (map[string]hydration.Document, error) {
	results := make(map[string]hydration.Document, len(keys))

	for _, key := range keys {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		doc, err := c.Fetch(ctx, key)
		if err != nil {
			// Mark as not found on error, continue with others
			results[key] = hydration.Document{ID: key, Found: false}
			continue
		}
		results[key] = doc
	}

	return results, nil
}

// Store is not supported by HTTP connector.
func (c *HTTPConnector) Store(ctx context.Context, key string, source map[string]any) error {
	return hydration.ErrStoreNotSupported
}

// Delete is not supported by HTTP connector.
func (c *HTTPConnector) Delete(ctx context.Context, key string) error {
	return hydration.ErrDeleteNotSupported
}

// Close releases resources.
func (c *HTTPConnector) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

// buildURL replaces {key} placeholder with the actual key.
func (c *HTTPConnector) buildURL(key string) string {
	url := c.config.BaseURL
	url = strings.ReplaceAll(url, "{key}", key)
	url = strings.ReplaceAll(url, "{id}", key)
	return url
}

// Ensure HTTPConnector implements Connector.
var _ hydration.Connector = (*HTTPConnector)(nil)

