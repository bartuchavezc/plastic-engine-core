package tracing

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/credentials"
)

// Config contains the settings for tracer initialisation.
type Config struct {
	// ServiceName identifies the service in traces (required)
	ServiceName string

	// ServiceVersion is the version of the service
	ServiceVersion string

	// Environment (e.g., "production", "staging", "development")
	Environment string

	// Endpoint for OTLP gRPC exporter
	Endpoint string

	// Insecure disables TLS for the exporter connection
	Insecure bool

	// SampleRate is the probability of sampling (0.0 to 1.0, default 1.0)
	SampleRate float64

	// Enabled controls whether tracing is active (default true)
	Enabled bool

	// BatchTimeout is the maximum time to wait before exporting a batch
	BatchTimeout time.Duration

	// MaxExportBatchSize is the maximum number of spans to export in a batch
	MaxExportBatchSize int

	// MaxQueueSize is the maximum number of spans to queue before dropping
	MaxQueueSize int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(serviceName string) Config {
	cfg := Config{
		ServiceName:        serviceName,
		ServiceVersion:     getEnvOrDefault("SERVICE_VERSION", "0.0.0"),
		Environment:        getEnvOrDefault("ENVIRONMENT", "development"),
		Endpoint:           getEnvOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		Insecure:           getEnvBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		SampleRate:         getEnvFloat("OTEL_TRACES_SAMPLER_ARG", 1.0),
		Enabled:            getEnvBool("OTEL_SDK_DISABLED", false) == false,
		BatchTimeout:       5 * time.Second,
		MaxExportBatchSize: 512,
		MaxQueueSize:       2048,
	}
	return cfg
}

// Init configures the global tracer provider and returns a shutdown function.
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	// If tracing is disabled, return a no-op shutdown
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("tracing: service name is required")
	}

	// Apply defaults for optional fields
	if cfg.Endpoint == "" {
		cfg.Endpoint = getEnvOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	}
	if cfg.SampleRate <= 0 || cfg.SampleRate > 1 {
		cfg.SampleRate = 1.0
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 5 * time.Second
	}
	if cfg.MaxExportBatchSize <= 0 {
		cfg.MaxExportBatchSize = 512
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 2048
	}

	// Configure gRPC client options
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithTimeout(cfg.BatchTimeout),
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}

	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	} else {
		// Use TLS with system CA pool (skip verification for self-signed certs)
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true, // Allow self-signed certificates
		}
		opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsConfig)))
	}

	client := otlptracegrpc.NewClient(opts...)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("tracing: create exporter: %w", err)
	}

	// Build resource with service metadata
	attrs := []attribute.KeyValue{
		attribute.String("service.name", cfg.ServiceName),
	}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, attribute.String("service.version", cfg.ServiceVersion))
	}
	if cfg.Environment != "" {
		attrs = append(attrs, attribute.String("deployment.environment", cfg.Environment))
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(resource.Default().SchemaURL(), attrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: build resource: %w", err)
	}

	// Configure sampler
	var sampler sdktrace.Sampler
	if cfg.SampleRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if cfg.SampleRate <= 0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRate)
	}

	// Create tracer provider with batcher
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(cfg.BatchTimeout),
			sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatchSize),
			sdktrace.WithMaxQueueSize(cfg.MaxQueueSize),
		),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return defaultValue
	}
	return b
}

func getEnvFloat(key string, defaultValue float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultValue
	}
	return f
}
