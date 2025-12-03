package clusterindex

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"

	clusterhttputil "plastic-engine-core/internal/adapters/http/cluster/httputil"
	coordinator "plastic-engine-core/internal/core/cluster/coordinator"
	coreindex "plastic-engine-core/internal/core/index"
)

var tracer = otel.Tracer("cluster/http/index")

// Mount registers index-related routes on the provided router.
func Mount(r chi.Router, coord *coordinator.Coordinator) {
	handler := &handler{
		coord: coord,
	}

	r.Get("/indexes", handler.handleListIndexes)
	r.Get("/indexes/{id}", handler.handleGetIndex)
	r.Post("/indexes", handler.handleCreateIndex)
	r.Post("/indexes/{id}/documents", handler.handleIngestDocument)
}

type handler struct {
	coord *coordinator.Coordinator
}

type createIndexRequest struct {
	ID               string                   `json:"id"`
	Name             string                   `json:"name"`
	ShardConfig      *shardConfigBody         `json:"shard_config"`
	DefaultAnalyzer  string                   `json:"default_analyzer"`
	DefaultTokenizer string                   `json:"default_tokenizer"`
	MappingVersion   int                      `json:"mapping_version"`
	FieldMappings    []createFieldMappingBody `json:"field_mappings"`
	InitialShardKeys []string                 `json:"initial_shard_keys"`
}

type createFieldMappingBody struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Analyzer  string `json:"analyzer"`
	Tokenizer string `json:"tokenizer"`
	Stored    *bool  `json:"stored"`
	Required  *bool  `json:"required"`
	Index     *bool  `json:"index"`
}

type ingestDocumentRequest struct {
	DocumentID string            `json:"document_id"`
	Routing    map[string]string `json:"routing"`
	Payload    json.RawMessage   `json:"payload"`
}

type shardConfigBody struct {
	Strategy  string                    `json:"strategy"`
	Automatic *automaticShardConfigBody `json:"automatic"`
	Date      *dateShardConfigBody      `json:"date"`
	Computed  *computedShardConfigBody  `json:"computed"`
}

type automaticShardConfigBody struct {
	ShardCount int    `json:"shard_count"`
	Field      string `json:"field"`
}

type dateShardConfigBody struct {
	Field       string `json:"field"`
	Granularity string `json:"granularity"`
}

type computedShardConfigBody struct {
	Components []computedComponentBody `json:"components"`
}

type computedComponentBody struct {
	Field     string `json:"field"`
	Transform string `json:"transform"`
}

func convertShardConfig(body *shardConfigBody) coreindex.ShardConfig {
	cfg := coreindex.ShardConfig{}
	if body == nil {
		cfg.Strategy = coreindex.ShardStrategyAutomatic
		cfg.Automatic = &coreindex.AutomaticShardConfig{ShardCount: 1}
		return cfg
	}

	strategy := strings.TrimSpace(body.Strategy)
	if strategy == "" {
		switch {
		case body.Date != nil:
			strategy = string(coreindex.ShardStrategyDate)
		case body.Computed != nil:
			strategy = string(coreindex.ShardStrategyComputed)
		default:
			strategy = string(coreindex.ShardStrategyAutomatic)
		}
	}

	cfg.Strategy = coreindex.ShardStrategy(strategy)

	switch cfg.Strategy {
	case coreindex.ShardStrategyAutomatic:
		cfg.Automatic = &coreindex.AutomaticShardConfig{ShardCount: 1}
		if body.Automatic != nil {
			if body.Automatic.ShardCount > 0 {
				cfg.Automatic.ShardCount = body.Automatic.ShardCount
			}
			cfg.Automatic.Field = strings.TrimSpace(body.Automatic.Field)
		}
	case coreindex.ShardStrategyDate:
		cfg.Date = &coreindex.DateShardConfig{
			Granularity: coreindex.DateGranularityMonth,
		}
		if body.Date != nil {
			cfg.Date.Field = strings.TrimSpace(body.Date.Field)
			if g := strings.TrimSpace(body.Date.Granularity); g != "" {
				cfg.Date.Granularity = coreindex.DateGranularity(g)
			}
		}
	case coreindex.ShardStrategyComputed:
		cfg.Computed = &coreindex.ComputedShardConfig{}
		if body.Computed != nil {
			cfg.Computed.Components = make([]coreindex.ComputedComponent, 0, len(body.Computed.Components))
			for _, comp := range body.Computed.Components {
				cfg.Computed.Components = append(cfg.Computed.Components, coreindex.ComputedComponent{
					Field:     strings.TrimSpace(comp.Field),
					Transform: coreindex.ComputedTransform(strings.TrimSpace(comp.Transform)),
				})
			}
		}
	default:
		cfg.Strategy = coreindex.ShardStrategyAutomatic
		cfg.Automatic = &coreindex.AutomaticShardConfig{ShardCount: 1}
	}

	return cfg
}

func (h *handler) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.createIndex")
	defer span.End()

	var payload createIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid index payload", http.StatusBadRequest)
		return
	}

	req := coreindex.CreateIndexRequest{
		ID:               payload.ID,
		Name:             payload.Name,
		DefaultAnalyzer:  payload.DefaultAnalyzer,
		DefaultTokenizer: payload.DefaultTokenizer,
		MappingVersion:   payload.MappingVersion,
		InitialShardKeys: append([]string(nil), payload.InitialShardKeys...),
		FieldMappings:    make([]coreindex.FieldMapping, 0, len(payload.FieldMappings)),
		ShardConfig:      convertShardConfig(payload.ShardConfig),
	}

	for _, fm := range payload.FieldMappings {
		field := coreindex.FieldMapping{
			Name:      fm.Name,
			Type:      coreindex.FieldType(fm.Type),
			Analyzer:  fm.Analyzer,
			Tokenizer: fm.Tokenizer,
			Stored:    fm.Stored != nil && *fm.Stored,
			Required:  fm.Required != nil && *fm.Required,
		}
		if fm.Index != nil {
			field.Indexed = *fm.Index
		} else {
			field.Indexed = true
		}
		req.FieldMappings = append(req.FieldMappings, field)
	}

	if req.ShardConfig.Strategy == "" {
		req.ShardConfig.Strategy = coreindex.ShardStrategyAutomatic
	}
	req.ShardStrategy = req.ShardConfig.Strategy

	resp, err := h.coord.CreateIndex(ctx, req)
	if err != nil {
		switch {
		case errors.Is(err, coreindex.ErrIndexIDExists),
			errors.Is(err, coreindex.ErrIndexNameExists):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			var validationErr *coreindex.ValidationError
			if errors.As(err, &validationErr) {
				http.Error(w, validationErr.Error(), http.StatusBadRequest)
				return
			}
			http.Error(w, "failed to create index", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	response := map[string]any{
		"id":                resp.Definition.ID,
		"name":              resp.Definition.Name,
		"shard_strategy":    resp.Definition.ShardStrategy,
		"shard_template":    resp.Definition.ShardTemplate,
		"shard_config":      toShardConfigResponse(resp.Definition.ShardConfig),
		"default_analyzer":  resp.Definition.DefaultAnalyzer,
		"default_tokenizer": resp.Definition.DefaultTokenizer,
		"mapping_version":   resp.Definition.MappingVersion,
		"field_mappings":    resp.Definition.FieldMappings,
		"created_at":        resp.Definition.CreatedAt,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *handler) handleIngestDocument(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.ingestDocument")
	defer span.End()

	indexID := chi.URLParam(r, "id")
	if indexID == "" {
		http.Error(w, "missing index id in path", http.StatusBadRequest)
		return
	}

	var req ingestDocumentRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid ingest payload", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.DocumentID) == "" {
		http.Error(w, "document_id is required", http.StatusBadRequest)
		return
	}

	ingestReq := coordinator.Request{
		IndexID:    indexID,
		DocumentID: req.DocumentID,
		Routing:    req.Routing,
		Payload:    req.Payload,
	}

	if err := h.coord.IngestDocument(ctx, ingestReq); err != nil {
		var validationErr *coreindex.ValidationError
		switch {
		case errors.Is(err, fmt.Errorf("shard not found for routing metadata")):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.As(err, &validationErr):
			http.Error(w, validationErr.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *handler) handleListIndexes(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.listIndexes")
	defer span.End()

	definitions, err := h.coord.ListIndexes(ctx)
	if err != nil {
		http.Error(w, "failed to list indexes", http.StatusInternalServerError)
		return
	}

	response := make([]indexResponse, 0, len(definitions))
	for _, def := range definitions {
		response = append(response, toIndexResponse(def))
	}

	clusterhttputil.WriteJSON(w, http.StatusOK, response)
}

func (h *handler) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "cluster.getIndex")
	defer span.End()

	indexID := chi.URLParam(r, "id")
	if strings.TrimSpace(indexID) == "" {
		http.Error(w, "missing index id in path", http.StatusBadRequest)
		return
	}

	definition, err := h.coord.GetIndex(ctx, indexID)
	if err != nil {
		if errors.Is(err, coreindex.ErrIndexNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, "failed to fetch index", http.StatusInternalServerError)
		return
	}

	clusterhttputil.WriteJSON(w, http.StatusOK, toIndexResponse(definition))
}

type indexResponse struct {
	ID               string                 `json:"id"`
	Name             string                 `json:"name"`
	ShardStrategy    string                 `json:"shard_strategy"`
	ShardTemplate    string                 `json:"shard_template,omitempty"`
	ShardConfig      shardConfigResponse    `json:"shard_config"`
	DefaultAnalyzer  string                 `json:"default_analyzer"`
	DefaultTokenizer string                 `json:"default_tokenizer"`
	MappingVersion   int                    `json:"mapping_version"`
	FieldMappings    []fieldMappingResponse `json:"field_mappings"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
}

type fieldMappingResponse struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Analyzer  string `json:"analyzer,omitempty"`
	Tokenizer string `json:"tokenizer,omitempty"`
	Stored    bool   `json:"stored"`
	Required  bool   `json:"required"`
	Index     bool   `json:"index"`
}

type shardConfigResponse struct {
	Strategy  string                        `json:"strategy"`
	Automatic *automaticShardConfigResponse `json:"automatic,omitempty"`
	Date      *dateShardConfigResponse      `json:"date,omitempty"`
	Computed  *computedShardConfigResponse  `json:"computed,omitempty"`
}

type automaticShardConfigResponse struct {
	ShardCount int    `json:"shard_count"`
	Field      string `json:"field,omitempty"`
}

type dateShardConfigResponse struct {
	Field       string `json:"field"`
	Granularity string `json:"granularity"`
}

type computedShardConfigResponse struct {
	Components []computedComponentResponse `json:"components"`
}

type computedComponentResponse struct {
	Field     string `json:"field"`
	Transform string `json:"transform"`
}

func toIndexResponse(def coreindex.IndexDefinition) indexResponse {
	resp := indexResponse{
		ID:               def.ID,
		Name:             def.Name,
		ShardStrategy:    string(def.ShardStrategy),
		ShardTemplate:    def.ShardTemplate,
		ShardConfig:      toShardConfigResponse(def.ShardConfig),
		DefaultAnalyzer:  def.DefaultAnalyzer,
		DefaultTokenizer: def.DefaultTokenizer,
		MappingVersion:   def.MappingVersion,
		CreatedAt:        def.CreatedAt,
		UpdatedAt:        def.UpdatedAt,
		FieldMappings:    make([]fieldMappingResponse, 0, len(def.FieldMappings)),
	}

	for _, field := range def.FieldMappings {
		resp.FieldMappings = append(resp.FieldMappings, fieldMappingResponse{
			Name:      field.Name,
			Type:      string(field.Type),
			Analyzer:  field.Analyzer,
			Tokenizer: field.Tokenizer,
			Stored:    field.Stored,
			Required:  field.Required,
			Index:     field.Indexed,
		})
	}

	return resp
}

func toShardConfigResponse(cfg coreindex.ShardConfig) shardConfigResponse {
	resp := shardConfigResponse{
		Strategy: string(cfg.Strategy),
	}
	switch cfg.Strategy {
	case coreindex.ShardStrategyAutomatic:
		if cfg.Automatic != nil {
			resp.Automatic = &automaticShardConfigResponse{
				ShardCount: cfg.Automatic.ShardCount,
				Field:      cfg.Automatic.Field,
			}
		}
	case coreindex.ShardStrategyDate:
		if cfg.Date != nil {
			resp.Date = &dateShardConfigResponse{
				Field:       cfg.Date.Field,
				Granularity: string(cfg.Date.Granularity),
			}
		}
	case coreindex.ShardStrategyComputed:
		if cfg.Computed != nil {
			components := make([]computedComponentResponse, 0, len(cfg.Computed.Components))
			for _, comp := range cfg.Computed.Components {
				components = append(components, computedComponentResponse{
					Field:     comp.Field,
					Transform: string(comp.Transform),
				})
			}
			resp.Computed = &computedShardConfigResponse{Components: components}
		}
	}
	return resp
}
