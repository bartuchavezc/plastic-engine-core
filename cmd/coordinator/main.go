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
	coordinator "plastic-engine-core/internal/core/cluster/coordinator"
	clustersearch "plastic-engine-core/internal/core/cluster/search"
	"plastic-engine-core/internal/helpers"
	"plastic-engine-core/internal/telemetry/tracing"
)

func main() {
	logger := helpers.DefaultLogger()

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
		logger.Error("failed to initialise tracing", helpers.Field{Key: "error", Value: err})
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			logger.Error("error shutting down tracer", helpers.Field{Key: "error", Value: err})
		}
	}()

	coord, err := coordinator.NewCoordinator("coordinator", port, dbPath)
	if err != nil {
		logger.Error("failed to initialise coordinator", helpers.Field{Key: "error", Value: err})
		os.Exit(1)
	}
	defer func() {
		if err := coord.Close(); err != nil {
			logger.Error("error closing coordinator", helpers.Field{Key: "error", Value: err})
		}
	}()

	joinService := clustersearch.NewJoinService(coord)
	router := clusterhttp.NewRouter(coord, joinService)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: router,
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	coord.StartHealthMonitor(signalCtx, 5*time.Second, 15*time.Second)

	go func() {
		logger.Info("coordinator listening", helpers.Field{Key: "addr", Value: server.Addr})
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("coordinator server error", helpers.Field{Key: "error", Value: err})
			os.Exit(1)
		}
	}()

	<-signalCtx.Done()
	logger.Info("shutting down coordinator")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("error shutting down server", helpers.Field{Key: "error", Value: err})
	}
}
