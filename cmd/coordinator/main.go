package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	clusterhttp "plastic-engine-core/internal/adapters/http/cluster"
	"plastic-engine-core/internal/adapters/telemetry/tracing"
	"plastic-engine-core/internal/core/cluster"
	"plastic-engine-core/internal/core/cluster/nodes"
	"plastic-engine-core/internal/pkg/logger"
)

func main() {
	log := logger.DefaultLogger()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dbPath := os.Getenv("DB_PATH")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracerShutdown, err := tracing.Init(ctx, tracing.Config{
		ServiceName: "coordinator",
		Endpoint:    os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	})
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

	joinService := nodes.NewJoinService(coord.NodesService())
	router := clusterhttp.NewRouter(coord, joinService)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: router,
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	coord.StartHealthMonitor(signalCtx, 5*time.Second, 15*time.Second)

	go func() {
		log.Info("coordinator listening", logger.Field{Key: "addr", Value: server.Addr})
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("coordinator server error", logger.Field{Key: "error", Value: err})
			os.Exit(1)
		}
	}()

	<-signalCtx.Done()
	log.Info("shutting down coordinator")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("error shutting down server", logger.Field{Key: "error", Value: err})
	}
}
