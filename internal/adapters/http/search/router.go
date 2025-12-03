package searchhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"plastic-engine-core/internal/core/search/document"
	"plastic-engine-core/internal/pkg/logger"
)

// Indexer defines the contract required to index documents.
type Indexer interface {
	Index(ctx context.Context, cmd document.Command) error
}

// NewRouter exposes the HTTP surface of a search node.
func NewRouter(idx Indexer, log logger.Logger) http.Handler {
	if log == nil {
		log = logger.DefaultLogger()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/documents", func(w http.ResponseWriter, r *http.Request) {
		handleDocumentIngest(w, r, idx, log)
	})

	return mux
}

type ingestRequest struct {
	IndexID    string            `json:"index_id"`
	ShardID    string            `json:"shard_id"`
	DocumentID string            `json:"document_id"`
	Routing    map[string]string `json:"routing"`
	Payload    json.RawMessage   `json:"payload"`
}

func handleDocumentIngest(w http.ResponseWriter, r *http.Request, idx Indexer, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if idx == nil {
		http.Error(w, "indexer unavailable", http.StatusServiceUnavailable)
		return
	}

	defer r.Body.Close()
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	var payload map[string]any
	if len(req.Payload) > 0 {
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			http.Error(w, "invalid payload body", http.StatusBadRequest)
			return
		}
	}

	cmd := document.Command{
		IndexID:    req.IndexID,
		ShardID:    req.ShardID,
		DocumentID: req.DocumentID,
		Routing:    req.Routing,
		Payload:    payload,
	}

	if err := idx.Index(r.Context(), cmd); err != nil {
		switch {
		case errors.Is(err, document.ErrInvalidCommand):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, document.ErrShardNotLoaded):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, document.ErrBackpressure):
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			http.Error(w, "failed to index document", http.StatusInternalServerError)
		}
		return
	}

	log.Info("document ingested",
		logger.Field{Key: "index_id", Value: req.IndexID},
		logger.Field{Key: "shard_id", Value: req.ShardID},
		logger.Field{Key: "document_id", Value: req.DocumentID},
	)

	w.WriteHeader(http.StatusAccepted)
}
