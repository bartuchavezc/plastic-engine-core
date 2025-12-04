package middleware

import (
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"

	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/pkg/logger"
)

// Config holds middleware configuration options.
type Config struct {
	// ServiceName identifies the service in traces
	ServiceName string

	// Logger for request logging (optional, uses default if nil)
	Logger logger.Logger

	// Metrics provider for recording HTTP metrics (optional)
	Metrics *metrics.Provider

	// SkipPaths are paths that should not be instrumented (e.g., health checks)
	SkipPaths []string

	// RecordRequestBody enables recording request body size
	RecordRequestBody bool

	// RecordResponseBody enables recording response body size
	RecordResponseBody bool
}

// Observability returns an HTTP middleware that provides:
// - Distributed tracing with OpenTelemetry
// - Request/response logging with trace correlation
// - Request timing
func Observability(cfg Config) func(http.Handler) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = logger.DefaultLogger()
	}

	tracerName := cfg.ServiceName
	if tracerName == "" {
		tracerName = "http-server"
	}

	tracer := otel.Tracer(tracerName)
	propagator := otel.GetTextMapPropagator()

	skipSet := make(map[string]struct{}, len(cfg.SkipPaths))
	for _, p := range cfg.SkipPaths {
		skipSet[p] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip instrumentation for configured paths
			if _, skip := skipSet[r.URL.Path]; skip {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()

			// Extract trace context from incoming request
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			// Create span for this request
			spanName := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPMethodKey.String(r.Method),
					semconv.HTTPURLKey.String(r.URL.String()),
					semconv.HTTPTargetKey.String(r.URL.Path),
					semconv.HTTPSchemeKey.String(schemeFromRequest(r)),
					semconv.NetHostNameKey.String(r.Host),
					semconv.UserAgentOriginalKey.String(r.UserAgent()),
				),
			)
			defer span.End()

			// Track active requests
			if cfg.Metrics != nil {
				cfg.Metrics.IncrementActiveRequests(ctx, r.Method, r.URL.Path)
				defer cfg.Metrics.DecrementActiveRequests(ctx, r.Method, r.URL.Path)
			}

			// Wrap response writer to capture status code and size
			wrapped := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Serve request with traced context
			next.ServeHTTP(wrapped, r.WithContext(ctx))

			duration := time.Since(start)

			// Add response attributes to span
			span.SetAttributes(
				semconv.HTTPStatusCodeKey.Int(wrapped.statusCode),
				attribute.Int64("http.response_size", wrapped.bytesWritten),
				attribute.Int64("http.duration_ms", duration.Milliseconds()),
			)

			// Set span status based on HTTP status code
			if wrapped.statusCode >= 400 {
				span.SetStatus(codes.Error, http.StatusText(wrapped.statusCode))
			} else {
				span.SetStatus(codes.Ok, "")
			}

			// Record metrics
			if cfg.Metrics != nil {
				cfg.Metrics.RecordRequest(ctx, r.Method, r.URL.Path, wrapped.statusCode, duration, r.ContentLength, wrapped.bytesWritten)
			}

			// Log request with trace correlation
			logLevel := "info"
			if wrapped.statusCode >= 500 {
				logLevel = "error"
			} else if wrapped.statusCode >= 400 {
				logLevel = "warn"
			}

			fields := []logger.Field{
				{Key: "method", Value: r.Method},
				{Key: "path", Value: r.URL.Path},
				{Key: "status", Value: wrapped.statusCode},
				{Key: "duration_ms", Value: duration.Milliseconds()},
				{Key: "size", Value: wrapped.bytesWritten},
			}

			if r.URL.RawQuery != "" {
				fields = append(fields, logger.Field{Key: "query", Value: r.URL.RawQuery})
			}

			switch logLevel {
			case "error":
				cfg.Logger.ErrorCtx(ctx, "HTTP request completed", fields...)
			case "warn":
				cfg.Logger.WarnCtx(ctx, "HTTP request completed", fields...)
			default:
				cfg.Logger.InfoCtx(ctx, "HTTP request completed", fields...)
			}
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code and bytes written.
type responseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.statusCode = code
		rw.wroteHeader = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

// Flush implements http.Flusher if the underlying ResponseWriter supports it.
func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func schemeFromRequest(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if scheme := r.Header.Get("X-Forwarded-Proto"); scheme != "" {
		return scheme
	}
	return "http"
}

// Recovery returns a middleware that recovers from panics and logs them with trace context.
func Recovery(log logger.Logger) func(http.Handler) http.Handler {
	if log == nil {
		log = logger.DefaultLogger()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					ctx := r.Context()
					span := trace.SpanFromContext(ctx)
					span.SetStatus(codes.Error, fmt.Sprintf("panic: %v", err))
					span.RecordError(fmt.Errorf("panic: %v", err))

					log.ErrorCtx(ctx, "panic recovered",
						logger.Field{Key: "error", Value: err},
						logger.Field{Key: "path", Value: r.URL.Path},
						logger.Field{Key: "method", Value: r.Method},
					)

					http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RequestID extracts or generates a request ID and adds it to the context.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		spanCtx := trace.SpanContextFromContext(ctx)

		var requestID string
		if spanCtx.IsValid() {
			requestID = spanCtx.TraceID().String()
		} else {
			// Fallback to X-Request-ID header if no trace
			requestID = r.Header.Get("X-Request-ID")
		}

		if requestID != "" {
			w.Header().Set("X-Request-ID", requestID)
		}

		next.ServeHTTP(w, r)
	})
}

