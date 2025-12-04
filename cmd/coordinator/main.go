package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	clusterhttp "plastic-engine-core/internal/adapters/http/cluster"
	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/adapters/telemetry/middleware"
	"plastic-engine-core/internal/adapters/telemetry/tracing"
	"plastic-engine-core/internal/core/cluster"
	"plastic-engine-core/internal/core/cluster/nodes"
	"plastic-engine-core/internal/pkg/logger"
)

const serviceName = "coordinator"

func main() {
	log := logger.DefaultLogger()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dbPath := os.Getenv("DB_PATH")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize tracing
	tracingCfg := tracing.DefaultConfig(serviceName)
	tracerShutdown, err := tracing.Init(ctx, tracingCfg)
	if err != nil {
		log.Error("failed to initialise tracing", logger.Field{Key: "error", Value: err})
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			log.Error("error shutting down tracer", logger.Field{Key: "error", Value: err})
		}
	}()

	// Initialize metrics
	metricsProvider, metricsShutdown, err := metrics.Init(ctx, metrics.Config{
		ServiceName:      serviceName,
		OTLPEndpoint:     os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		EnablePrometheus: os.Getenv("ENABLE_PROMETHEUS") != "false",
		PrometheusPort:   os.Getenv("METRICS_PORT"),
	})
	if err != nil {
		log.Error("failed to initialise metrics", logger.Field{Key: "error", Value: err})
		os.Exit(1)
	}
	defer func() {
		if metricsShutdown != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := metricsShutdown(shutdownCtx); err != nil {
				log.Error("error shutting down metrics", logger.Field{Key: "error", Value: err})
			}
		}
	}()

	// Initialize coordinator
	coord, err := cluster.NewCoordinator("coordinator", port, dbPath)
	if err != nil {
		log.Error("failed to initialise coordinator", logger.Field{Key: "error", Value: err})
		os.Exit(1)
	}
	defer func() {
		if err := coord.Close(); err != nil {
			log.Error("error closing coordinator", logger.Field{Key: "error", Value: err})
		}
	}()

	// Build router with observability middleware
	joinService := nodes.NewJoinService(coord.NodesService())
	baseRouter := clusterhttp.NewRouter(coord, joinService)

	// Wrap with observability middleware
	r := chi.NewRouter()
	r.Use(middleware.Recovery(log))
	r.Use(middleware.RequestID)
	r.Use(middleware.Observability(middleware.Config{
		ServiceName: serviceName,
		Logger:      log,
		Metrics:     metricsProvider,
		SkipPaths:   []string{"/health", "/ready", "/metrics"},
	}))
	r.Mount("/", baseRouter)

	// Add health endpoints
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	coord.StartHealthMonitor(signalCtx, 5*time.Second, 15*time.Second)

	go func() {
		log.Info("coordinator listening",
			logger.Field{Key: "addr", Value: server.Addr},
			logger.Field{Key: "metrics_enabled", Value: os.Getenv("ENABLE_PROMETHEUS") != "false"},
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("coordinator server error", logger.Field{Key: "error", Value: err})
			os.Exit(1)
		}
	}()

	<-signalCtx.Done()
	log.Info("shutting down coordinator")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("error shutting down server", logger.Field{Key: "error", Value: err})
	}
}
