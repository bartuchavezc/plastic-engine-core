package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	searchhttp "plastic-engine-core/internal/adapters/http/search"
	client "plastic-engine-core/internal/core/search/client"
	"plastic-engine-core/internal/core/search/document"
	searchnode "plastic-engine-core/internal/core/search"
	"plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
	"plastic-engine-core/internal/adapters/telemetry/tracing"
)

func main() {
	logger := logger.DefaultLogger()

	role := os.Getenv("ROLE")
	if role == "" {
		role = "search"
	}

	if role != "search" {
		logger.Error("search binary must run with ROLE=search", logger.Field{Key: "role", Value: role})
		os.Exit(1)
	}

	joinAddr := os.Getenv("JOIN_ADDR")
	if joinAddr == "" {
		joinAddr = "http://localhost:8080"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}

	listenAddr := fmt.Sprintf(":%s", port)

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}

	advertiseAddr := os.Getenv("ADVERTISE_ADDR")
	if advertiseAddr == "" {
		advertiseAddr = fmt.Sprintf("http://localhost:%s", port)
	}

	nodeInfo := search.NodeInfo{
		Role:          role,
		AdvertiseAddr: advertiseAddr,
		DataDir:       dataDir,
	}

	client := client.NewClient(joinAddr, &http.Client{})
	manager := shard.NewManager(dataDir)

	tracerShutdown, err := tracing.Init(context.Background(), tracing.Config{
		ServiceName: "search",
		Endpoint:    os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	})
	if err != nil {
		logger.Error("failed to initialise tracing", logger.Field{Key: "error", Value: err})
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			logger.Error("error shutting down tracer", logger.Field{Key: "error", Value: err})
		}
	}()

	node := search.New(nodeInfo, joinAddr, client, manager, logger)

	if err := node.Initialize(); err != nil {
		logger.Error("failed to initialise search node", logger.Field{Key: "error", Value: err})
		os.Exit(1)
	}

	logger.Info("search node initialised", logger.Field{Key: "node_id", Value: node.Info.ID})

	resolver := document.NewMetadataResolver(nil)
	assignmentProvider := document.NewAssignmentProvider(manager, resolver)
	planner := document.NewFieldPlanner(document.TokenizerFactory{}, document.AnalyzerFactory{})
	writerFactory := func(sh *shard.Shard) *document.IndexWriter {
		return document.NewIndexWriter(sh.Store)
	}

	workerCfg := document.ShardWorkerConfig{
		MaxWorkers:    4,
		QueueCapacity: 256,
	}

	indexService := document.NewService(manager, assignmentProvider, planner, writerFactory, workerCfg, logger)
	defer indexService.Close()

	router := searchhttp.NewRouter(indexService, logger)

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: router,
	}

	serverErrCh := make(chan error, 1)

	go func() {
		logger.Info("search HTTP server listening",
			logger.Field{Key: "listen_addr", Value: listenAddr},
			logger.Field{Key: "advertise_addr", Value: advertiseAddr},
		)

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	node.StartHeartbeat(ctx, 5*time.Second)

	var serverErr error

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrCh:
		serverErr = err
		stop()
	}

	if serverErr == nil {
		select {
		case err := <-serverErrCh:
			serverErr = err
		default:
		}
	}

	if serverErr != nil {
		logger.Error("search HTTP server stopped unexpectedly", logger.Field{Key: "error", Value: serverErr})
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("error shutting down HTTP server", logger.Field{Key: "error", Value: err})
	}

	if serverErr != nil {
		os.Exit(1)
	}

	logger.Info("search node shutting down")
}
