package searchhttp

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"plastic-engine-core/internal/core/ml/export"
	"plastic-engine-core/internal/core/search/indexstore"
	"plastic-engine-core/internal/core/search/knowledge"
	"plastic-engine-core/internal/pkg/logger"
)

// MLExporter provides access to shard internals needed for ML training data export.
type MLExporter interface {
	// GetShardTermMatrix returns the term co-occurrence matrix for a shard.
	GetShardTermMatrix(shardID string) (*knowledge.AdjacencyMatrix, bool)
	// GetShardDFSnapshot returns a DF snapshot for a shard.
	GetShardDFSnapshot(shardID string) (map[string]int64, bool)
	// GetShardTotalDocs returns the total document count for a shard.
	GetShardTotalDocs(shardID string) (int64, bool)
	// GetShardManager returns the indexstore.Manager for a shard (for expansion export).
	GetShardManager(shardID string) (*indexstore.Manager, bool)
}

// handleMLExportEdges streams edge feature CSV for a shard.
// GET /ml/export/edges?shard_id=X
func handleMLExportEdges(w http.ResponseWriter, r *http.Request, exporter MLExporter, log logger.Logger) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if exporter == nil {
		http.Error(w, "ml export unavailable", http.StatusServiceUnavailable)
		return
	}

	shardID := r.URL.Query().Get("shard_id")
	if shardID == "" {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}

	termMatrix, ok := exporter.GetShardTermMatrix(shardID)
	if !ok || termMatrix == nil {
		http.Error(w, "shard not found or no term matrix", http.StatusNotFound)
		return
	}

	dfSnapshot, ok := exporter.GetShardDFSnapshot(shardID)
	if !ok {
		http.Error(w, "shard DF snapshot unavailable", http.StatusNotFound)
		return
	}

	totalDocs, ok := exporter.GetShardTotalDocs(shardID)
	if !ok {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=edges_%s.csv", shardID))

	count, err := export.ExportEdgeFeaturesTo(termMatrix, dfSnapshot, totalDocs, w)
	if err != nil {
		log.Error("edge export error",
			logger.Field{Key: "shard_id", Value: shardID},
			logger.Field{Key: "error", Value: err},
		)
		return
	}

	log.Info("edge export complete",
		logger.Field{Key: "shard_id", Value: shardID},
		logger.Field{Key: "rows", Value: count},
	)
}

// handleMLExportGraph streams graph TSV for Node2Vec training.
// GET /ml/export/graph?shard_id=X
func handleMLExportGraph(w http.ResponseWriter, r *http.Request, exporter MLExporter, log logger.Logger) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if exporter == nil {
		http.Error(w, "ml export unavailable", http.StatusServiceUnavailable)
		return
	}

	shardID := r.URL.Query().Get("shard_id")
	if shardID == "" {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}

	termMatrix, ok := exporter.GetShardTermMatrix(shardID)
	if !ok || termMatrix == nil {
		http.Error(w, "shard not found or no term matrix", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/tab-separated-values")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=graph_%s.tsv", shardID))

	count, err := export.ExportGraphTSVTo(termMatrix, w)
	if err != nil {
		log.Error("graph export error",
			logger.Field{Key: "shard_id", Value: shardID},
			logger.Field{Key: "error", Value: err},
		)
		return
	}

	log.Info("graph export complete",
		logger.Field{Key: "shard_id", Value: shardID},
		logger.Field{Key: "edges", Value: count},
	)
}

// handleMLExportExpansion streams expansion features CSV for LTR training.
// POST /ml/export/expansion?shard_id=X&field=content&max_candidates=50
// Body: TSV with relevance judgments (query_id\tquery_text\tdoc_id\trelevance_grade)
func handleMLExportExpansion(w http.ResponseWriter, r *http.Request, exporter MLExporter, log logger.Logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed (use POST with judgments TSV body)", http.StatusMethodNotAllowed)
		return
	}
	if exporter == nil {
		http.Error(w, "ml export unavailable", http.StatusServiceUnavailable)
		return
	}

	shardID := r.URL.Query().Get("shard_id")
	if shardID == "" {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}

	field := r.URL.Query().Get("field")
	if field == "" {
		field = "content"
	}

	maxCandidates := 50
	if mc := r.URL.Query().Get("max_candidates"); mc != "" {
		if v, err := strconv.Atoi(mc); err == nil && v > 0 {
			maxCandidates = v
		}
	}

	termMatrix, ok := exporter.GetShardTermMatrix(shardID)
	if !ok || termMatrix == nil {
		http.Error(w, "shard not found or no term matrix", http.StatusNotFound)
		return
	}

	mgr, ok := exporter.GetShardManager(shardID)
	if !ok {
		http.Error(w, "shard manager not found", http.StatusNotFound)
		return
	}

	// Parse judgments from request body (TSV)
	defer r.Body.Close()
	judgments, err := parseJudgmentsFromBody(r)
	if err != nil {
		http.Error(w, "invalid judgments: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(judgments) == 0 {
		http.Error(w, "no judgments provided", http.StatusBadRequest)
		return
	}

	analyzeQuery := func(text string) []string {
		tokens := strings.Fields(strings.ToLower(text))
		var result []string
		for _, t := range tokens {
			t = strings.TrimSpace(t)
			if len(t) >= 2 {
				result = append(result, t)
			}
		}
		return result
	}

	expCfg := export.ExpansionFeatureConfig{
		Field:                 field,
		Pipeline:              indexstore.DefaultSearchPipeline(),
		AnalyzeQuery:          analyzeQuery,
		MaxCandidatesPerQuery: maxCandidates,
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=expansion_%s.csv", shardID))

	count, err := export.ExportExpansionFeaturesTo(termMatrix, mgr, judgments, expCfg, w)
	if err != nil {
		log.Error("expansion export error",
			logger.Field{Key: "shard_id", Value: shardID},
			logger.Field{Key: "error", Value: err},
		)
		return
	}

	log.Info("expansion export complete",
		logger.Field{Key: "shard_id", Value: shardID},
		logger.Field{Key: "rows", Value: count},
	)
}

// parseJudgmentsFromBody reads relevance judgments TSV from the request body.
// Format: query_id\tquery_text\tdoc_id\trelevance_grade
func parseJudgmentsFromBody(r *http.Request) ([]export.RelevanceJudgment, error) {
	reader := csv.NewReader(r.Body)
	reader.Comma = '\t'
	reader.Comment = '#'

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read TSV body: %w", err)
	}

	grouped := make(map[string]*export.RelevanceJudgment)
	for i, rec := range records {
		if i == 0 && (rec[0] == "query_id" || rec[0] == "qid") {
			continue
		}
		if len(rec) < 4 {
			continue
		}

		qid := rec[0]
		queryText := rec[1]
		docID := rec[2]
		grade, _ := strconv.Atoi(rec[3])

		j, ok := grouped[qid]
		if !ok {
			j = &export.RelevanceJudgment{
				QueryID:     qid,
				QueryText:   queryText,
				RelevantIDs: make(map[string]int),
			}
			grouped[qid] = j
		}
		j.RelevantIDs[docID] = grade
	}

	result := make([]export.RelevanceJudgment, 0, len(grouped))
	for _, j := range grouped {
		result = append(result, *j)
	}
	return result, nil
}
