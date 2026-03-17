package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	searchhttp "plastic-engine-core/internal/adapters/http/search"
	"plastic-engine-core/internal/adapters/telemetry/metrics"
	"plastic-engine-core/internal/adapters/telemetry/middleware"
	"plastic-engine-core/internal/adapters/telemetry/tracing"
	searchnode "plastic-engine-core/internal/core/search"
	client "plastic-engine-core/internal/core/search/client"
	"plastic-engine-core/internal/core/search/document"
	searchquery "plastic-engine-core/internal/core/search/query"
	"plastic-engine-core/internal/core/search/shards"
	"plastic-engine-core/internal/pkg/logger"
)

const serviceName = "search"

func main() {
	log := logger.DefaultLogger()

	role := os.Getenv("ROLE")
	if role == "" {
		role = "search"
	}

	if role != "search" {
		log.Error("search binary must run with ROLE=search", logger.Field{Key: "role", Value: role})
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
	metrics.SetGlobalProvider(metricsProvider)
	defer func() {
		if metricsShutdown != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := metricsShutdown(shutdownCtx); err != nil {
				log.Error("error shutting down metrics", logger.Field{Key: "error", Value: err})
			}
		}
	}()

	nodeInfo := searchnode.NodeInfo{
		Role:          role,
		AdvertiseAddr: advertiseAddr,
		DataDir:       dataDir,
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	clusterClient := client.NewClient(joinAddr, httpClient)
	manager := shards.NewManager(dataDir)

	// Create definition fetcher for mapping refreshes
	definitionFetcher := client.NewDefinitionFetcher(clusterClient)

	// Create metadata resolver with definition fetcher for mapping refreshes
	resolver := document.NewMetadataResolver(definitionFetcher)

	node := searchnode.NewWithMappingRefresher(nodeInfo, joinAddr, clusterClient, manager, resolver, log)

	if err := node.Initialize(); err != nil {
		log.Error("failed to initialise search node", logger.Field{Key: "error", Value: err})
		os.Exit(1)
	}

	log.Info("search node initialised", logger.Field{Key: "node_id", Value: node.Info.ID})

	assignmentProvider := document.NewAssignmentProvider(manager, resolver)
	planner := document.NewFieldPlanner(document.TokenizerFactory{}, document.AnalyzerFactory{})

	workerCfg := document.ShardWorkerConfig{
		// MaxWorkers: 0 → applyDefaults() uses TuneForNode(cpus/2, [2,32])
	}

	indexService := document.NewService(manager, assignmentProvider, planner, workerCfg, log)
	defer indexService.Close()

	// Create search service with segment manager getter
	getSegmentManager := func(shardID string) (searchquery.SegmentManager, bool) {
		shard, ok := manager.GetShard(shardID)
		if !ok {
			return nil, false
		}
		return shard.Segments, true
	}
	queryAnalyzer := searchquery.NewStandardQueryAnalyzer("english")
	searchService := searchquery.NewSearchService(getSegmentManager, queryAnalyzer, log)

	// Create term lookup service for term index APIs
	termLookupService := searchquery.NewTermLookupService(manager, log)

	// Build router with observability middleware
	baseRouter := searchhttp.NewRouterWithConfig(searchhttp.RouterConfig{
		Indexer:     indexService,
		Searcher:    searchService,
		ShardSyncer: manager,
		TermLookup:  termLookupService,
		Logger:      log,
	})

	r := chi.NewRouter()
	r.Use(middleware.Recovery(log))
	r.Use(middleware.RequestID)
	r.Use(middleware.Observability(middleware.Config{
		ServiceName: serviceName,
		Logger:      log,
		Metrics:     metricsProvider,
		SkipPaths:   []string{"/health", "/ready", "/metrics", "/debug/pprof/"},
	}))
	r.Mount("/", baseRouter)

	// Add health endpoints
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		// Check if node has shards loaded
		if len(manager.ListShardIDs()) > 0 || node.Info.ID != "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
		}
	})

	// pprof endpoints for profiling
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))

	httpServer := &http.Server{
		Addr:         listenAddr,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	serverErrCh := make(chan error, 1)

	go func() {
		log.Info("search HTTP server listening",
			logger.Field{Key: "listen_addr", Value: listenAddr},
			logger.Field{Key: "advertise_addr", Value: advertiseAddr},
			logger.Field{Key: "node_id", Value: node.Info.ID},
		)

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	node.StartHeartbeat(signalCtx, 5*time.Second)

	var serverErr error

	select {
	case <-signalCtx.Done():
		log.Info("shutdown signal received")
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
		log.Error("search HTTP server stopped unexpectedly", logger.Field{Key: "error", Value: serverErr})
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("error shutting down HTTP server", logger.Field{Key: "error", Value: err})
	}

	if serverErr != nil {
		os.Exit(1)
	}

	log.Info("search node shutting down")
}
