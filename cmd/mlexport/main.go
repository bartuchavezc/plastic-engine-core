// mlexport exports training data for ML models from an existing index.
//
// Usage:
//
//	mlexport -data <shard_dir> -export <type> -out <output_path> [-judgments <judgments.tsv>] [-field content]
//
// Export types:
//
//	edges     — Edge features CSV for training edge weight prediction (notebook 01)
//	graph     — Graph TSV for Node2Vec embedding training (notebook 03)
//	expansion — Expansion features CSV for LTR training (notebook 02, requires -judgments)
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"plastic-engine-core/internal/core/ml/export"
	"plastic-engine-core/internal/core/search/indexstore"
	"plastic-engine-core/internal/core/search/knowledge"
)

func main() {
	dataDir := flag.String("data", "", "Path to shard data directory (contains term_cooccurrence/, registry/, etc.)")
	exportType := flag.String("export", "", "Export type: edges, graph, expansion")
	outputPath := flag.String("out", "", "Output file path")
	judgmentsPath := flag.String("judgments", "", "Path to relevance judgments TSV (for expansion export)")
	field := flag.String("field", "content", "Search field name")
	flag.Parse()

	if *dataDir == "" || *exportType == "" || *outputPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	switch *exportType {
	case "edges":
		exportEdges(*dataDir, *outputPath)
	case "graph":
		exportGraph(*dataDir, *outputPath)
	case "expansion":
		if *judgmentsPath == "" {
			log.Fatal("-judgments is required for expansion export")
		}
		exportExpansion(*dataDir, *outputPath, *judgmentsPath, *field)
	default:
		log.Fatalf("unknown export type: %s (must be: edges, graph, expansion)", *exportType)
	}
}

func exportEdges(dataDir, outputPath string) {
	termMatrix, cleanup := openTermMatrix(dataDir)
	defer cleanup()

	// Open posting store for DF snapshot
	storeCfg := indexstore.PostingStoreConfig{DataDir: dataDir}
	store, err := indexstore.NewPebblePostingStore(storeCfg)
	if err != nil {
		log.Fatalf("open posting store: %v", err)
	}
	defer store.Close()

	dfSnapshot := store.SnapshotDF()
	totalDocs := store.GetTotalDocs()

	log.Printf("Loaded DF snapshot: %d terms, %d total docs", len(dfSnapshot), totalDocs)

	count, err := export.ExportEdgeFeatures(termMatrix, dfSnapshot, totalDocs, outputPath)
	if err != nil {
		log.Fatalf("export edge features: %v", err)
	}
	log.Printf("Exported %d edge feature rows to %s", count, outputPath)
}

func exportGraph(dataDir, outputPath string) {
	termMatrix, cleanup := openTermMatrix(dataDir)
	defer cleanup()

	count, err := export.ExportGraphTSV(termMatrix, outputPath)
	if err != nil {
		log.Fatalf("export graph: %v", err)
	}
	log.Printf("Exported %d edges to %s", count, outputPath)
}

func exportExpansion(dataDir, outputPath, judgmentsPath, field string) {
	termMatrix, cleanup := openTermMatrix(dataDir)
	defer cleanup()

	// Open Manager for DF lookups and search
	cfg := indexstore.DefaultConfig()
	cfg.DataDir = dataDir
	cfg.CooccurrenceConfig.Disabled = true // don't start background workers

	mgr, err := indexstore.NewManager(cfg)
	if err != nil {
		log.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	// Load judgments
	judgments, err := loadJudgments(judgmentsPath)
	if err != nil {
		log.Fatalf("load judgments: %v", err)
	}
	log.Printf("Loaded %d queries with relevance judgments", len(judgments))

	// Simple whitespace tokenizer (good enough for export — in production, use the analyzer)
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
		MaxCandidatesPerQuery: 50,
	}

	count, err := export.ExportExpansionFeatures(termMatrix, mgr, judgments, expCfg, outputPath)
	if err != nil {
		log.Fatalf("export expansion features: %v", err)
	}
	log.Printf("Exported %d expansion feature rows to %s", count, outputPath)
}

func openTermMatrix(dataDir string) (*knowledge.AdjacencyMatrix, func()) {
	tmDir := dataDir + "/term_cooccurrence"
	tm, err := knowledge.NewAdjacencyMatrix(knowledge.AdjacencyMatrixConfig{
		DataDir:       tmDir,
		EdgeCacheSize: 0, // no cache needed for batch export
	})
	if err != nil {
		log.Fatalf("open term matrix at %s: %v", tmDir, err)
	}

	stats := tm.Stats()
	log.Printf("Term matrix: %d terms, %d edges, %d nodes", stats.TermCount, stats.EdgeCount, stats.NodeCount)

	return tm, func() { tm.Close() }
}

// loadJudgments loads relevance judgments from a TSV file.
// Format: query_id\tquery_text\tdoc_id\trelevance_grade
// Multiple rows per query_id are grouped.
func loadJudgments(path string) ([]export.RelevanceJudgment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open judgments file: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '\t'
	r.Comment = '#'

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read judgments: %w", err)
	}

	grouped := make(map[string]*export.RelevanceJudgment)
	for i, rec := range records {
		if i == 0 && (rec[0] == "query_id" || rec[0] == "qid") {
			continue // skip header
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
