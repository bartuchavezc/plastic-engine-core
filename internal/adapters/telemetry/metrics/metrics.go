package metrics

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Config holds metrics configuration.
type Config struct {
	// ServiceName identifies the service in metrics
	ServiceName string

	// OTLPEndpoint for OTLP gRPC exporter (optional)
	OTLPEndpoint string

	// EnablePrometheus exposes a /metrics endpoint
	EnablePrometheus bool

	// PrometheusPort for the metrics server (default 9090)
	PrometheusPort string
}

// Provider wraps the OTEL meter provider and HTTP instruments.
type Provider struct {
	meterProvider *sdkmetric.MeterProvider
	meter         metric.Meter

	// HTTP metrics
	RequestsTotal   metric.Int64Counter
	RequestDuration metric.Float64Histogram
	RequestSize     metric.Int64Counter
	ResponseSize    metric.Int64Counter
	ActiveRequests  metric.Int64UpDownCounter

	// Business metrics
	DocumentsIngested metric.Int64Counter
	ShardsAssigned    metric.Int64Counter
	NodesJoined       metric.Int64Counter
	SearchQueries     metric.Int64Counter

	// Prometheus registry if enabled
	promRegistry *promclient.Registry
}

// Init initializes the metrics provider with OTEL.
func Init(ctx context.Context, cfg Config) (*Provider, func(context.Context) error, error) {
	if cfg.ServiceName == "" {
		return nil, nil, fmt.Errorf("metrics: service name is required")
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			resource.Default().SchemaURL(),
			attribute.String("service.name", cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("metrics: build resource: %w", err)
	}

	var readers []sdkmetric.Reader
	var promRegistry *promclient.Registry

	// Prometheus exporter (for /metrics endpoint)
	if cfg.EnablePrometheus {
		promRegistry = promclient.NewRegistry()
		promExporter, err := prometheus.New(
			prometheus.WithRegisterer(promRegistry),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("metrics: create prometheus exporter: %w", err)
		}
		readers = append(readers, promExporter)
	}

	// OTLP exporter (for Grafana/Tempo/etc.)
	if cfg.OTLPEndpoint != "" {
		otlpExporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlpmetricgrpc.WithInsecure(),
			otlpmetricgrpc.WithTimeout(5*time.Second),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("metrics: create OTLP exporter: %w", err)
		}
		readers = append(readers, sdkmetric.NewPeriodicReader(otlpExporter,
			sdkmetric.WithInterval(15*time.Second),
		))
	}

	// Create meter provider with all readers
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, reader := range readers {
		opts = append(opts, sdkmetric.WithReader(reader))
	}

	mp := sdkmetric.NewMeterProvider(opts...)
	otel.SetMeterProvider(mp)

	meter := mp.Meter(cfg.ServiceName)

	provider := &Provider{
		meterProvider: mp,
		meter:         meter,
		promRegistry:  promRegistry,
	}

	// Initialize HTTP metrics
	if err := provider.initHTTPMetrics(); err != nil {
		return nil, nil, err
	}

	// Initialize business metrics
	if err := provider.initBusinessMetrics(); err != nil {
		return nil, nil, err
	}

	// Start Prometheus server if enabled
	if cfg.EnablePrometheus {
		port := cfg.PrometheusPort
		if port == "" {
			port = os.Getenv("METRICS_PORT")
			if port == "" {
				port = "9091"
			}
		}
		go provider.startPrometheusServer(port)
	}

	return provider, mp.Shutdown, nil
}

func (p *Provider) initHTTPMetrics() error {
	var err error

	p.RequestsTotal, err = p.meter.Int64Counter(
		"http_requests_total",
		metric.WithDescription("Total number of HTTP requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return fmt.Errorf("create requests_total: %w", err)
	}

	p.RequestDuration, err = p.meter.Float64Histogram(
		"http_request_duration_seconds",
		metric.WithDescription("HTTP request duration in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return fmt.Errorf("create request_duration: %w", err)
	}

	p.RequestSize, err = p.meter.Int64Counter(
		"http_request_size_bytes_total",
		metric.WithDescription("Total bytes received in requests"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("create request_size: %w", err)
	}

	p.ResponseSize, err = p.meter.Int64Counter(
		"http_response_size_bytes_total",
		metric.WithDescription("Total bytes sent in responses"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("create response_size: %w", err)
	}

	p.ActiveRequests, err = p.meter.Int64UpDownCounter(
		"http_active_requests",
		metric.WithDescription("Number of active HTTP requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return fmt.Errorf("create active_requests: %w", err)
	}

	return nil
}

func (p *Provider) initBusinessMetrics() error {
	var err error

	p.DocumentsIngested, err = p.meter.Int64Counter(
		"plastic_documents_ingested_total",
		metric.WithDescription("Total number of documents ingested"),
		metric.WithUnit("{document}"),
	)
	if err != nil {
		return fmt.Errorf("create documents_ingested: %w", err)
	}

	p.ShardsAssigned, err = p.meter.Int64Counter(
		"plastic_shards_assigned_total",
		metric.WithDescription("Total number of shards assigned to nodes"),
		metric.WithUnit("{shard}"),
	)
	if err != nil {
		return fmt.Errorf("create shards_assigned: %w", err)
	}

	p.NodesJoined, err = p.meter.Int64Counter(
		"plastic_nodes_joined_total",
		metric.WithDescription("Total number of nodes that joined the cluster"),
		metric.WithUnit("{node}"),
	)
	if err != nil {
		return fmt.Errorf("create nodes_joined: %w", err)
	}

	p.SearchQueries, err = p.meter.Int64Counter(
		"plastic_search_queries_total",
		metric.WithDescription("Total number of search queries executed"),
		metric.WithUnit("{query}"),
	)
	if err != nil {
		return fmt.Errorf("create search_queries: %w", err)
	}

	return nil
}

func (p *Provider) startPrometheusServer(port string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(p.promRegistry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	_ = server.ListenAndServe()
}

// RecordRequest records metrics for an HTTP request.
func (p *Provider) RecordRequest(ctx context.Context, method, path string, statusCode int, duration time.Duration, requestSize, responseSize int64) {
	attrs := []attribute.KeyValue{
		attribute.String("method", method),
		attribute.String("path", normalizePath(path)),
		attribute.Int("status_code", statusCode),
		attribute.String("status_class", statusClass(statusCode)),
	}

	p.RequestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	p.RequestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))

	if requestSize > 0 {
		p.RequestSize.Add(ctx, requestSize, metric.WithAttributes(attrs[:2]...))
	}
	if responseSize > 0 {
		p.ResponseSize.Add(ctx, responseSize, metric.WithAttributes(attrs...))
	}
}

// IncrementActiveRequests increments the active requests counter.
func (p *Provider) IncrementActiveRequests(ctx context.Context, method, path string) {
	p.ActiveRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("path", normalizePath(path)),
	))
}

// DecrementActiveRequests decrements the active requests counter.
func (p *Provider) DecrementActiveRequests(ctx context.Context, method, path string) {
	p.ActiveRequests.Add(ctx, -1, metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("path", normalizePath(path)),
	))
}

// RecordDocumentIngested records a document ingestion event.
func (p *Provider) RecordDocumentIngested(ctx context.Context, indexID string) {
	p.DocumentsIngested.Add(ctx, 1, metric.WithAttributes(
		attribute.String("index_id", indexID),
	))
}

// RecordShardAssigned records a shard assignment event.
func (p *Provider) RecordShardAssigned(ctx context.Context, nodeID, indexID string) {
	p.ShardsAssigned.Add(ctx, 1, metric.WithAttributes(
		attribute.String("node_id", nodeID),
		attribute.String("index_id", indexID),
	))
}

// RecordNodeJoined records a node join event.
func (p *Provider) RecordNodeJoined(ctx context.Context, role string) {
	p.NodesJoined.Add(ctx, 1, metric.WithAttributes(
		attribute.String("role", role),
	))
}

// RecordSearchQuery records a search query event.
func (p *Provider) RecordSearchQuery(ctx context.Context, indexID string) {
	p.SearchQueries.Add(ctx, 1, metric.WithAttributes(
		attribute.String("index_id", indexID),
	))
}

func normalizePath(path string) string {
	// Normalize paths with IDs to reduce cardinality
	// e.g., /indexes/abc123/documents -> /indexes/{id}/documents
	// This is a simple implementation; can be enhanced
	return path
}

func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

