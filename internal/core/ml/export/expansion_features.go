package export

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"plastic-engine-core/internal/core/ml"
	"plastic-engine-core/internal/core/search/indexstore"
	"plastic-engine-core/internal/core/search/knowledge"
)

// RelevanceJudgment maps a query to its relevant document IDs.
type RelevanceJudgment struct {
	QueryID     string
	QueryText   string
	RelevantIDs map[string]int // docID → relevance grade (0-3)
}

// ExpansionFeatureConfig configures the expansion feature export.
type ExpansionFeatureConfig struct {
	// Field is the search field for terms (e.g. "content").
	Field string

	// Pipeline provides graph traversal parameters.
	Pipeline indexstore.SearchPipeline

	// EmbeddingLookup is optional — if non-nil, cosine similarity is computed.
	EmbeddingLookup ml.EmbeddingLookup

	// AnalyzeQuery tokenizes query text into terms.
	AnalyzeQuery func(text string) []string

	// MaxCandidatesPerQuery caps the number of expansion candidates per query.
	MaxCandidatesPerQuery int
}

// ExportExpansionFeatures runs queries against the term matrix, collects expansion
// candidates with features, and labels them based on relevance judgments.
//
// For each query:
//  1. Tokenize → spread activation → collect candidate expansions
//  2. For each candidate: compute features (energy, DF, cosineSim, degree, avgNeighborWeight)
//  3. Label: 1 if candidate's DF-weighted postings overlap with relevant docs, else 0
//
// CSV columns: query_id,term_id,energy,df,cosine_sim,node_degree,avg_neighbor_weight,label
func ExportExpansionFeatures(
	termMatrix *knowledge.AdjacencyMatrix,
	manager SegmentManagerForExport,
	judgments []RelevanceJudgment,
	cfg ExpansionFeatureConfig,
	outputPath string,
) (int, error) {
	f, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()
	return ExportExpansionFeaturesTo(termMatrix, manager, judgments, cfg, f)
}

// ExportExpansionFeaturesTo writes expansion features CSV to any io.Writer.
func ExportExpansionFeaturesTo(
	termMatrix *knowledge.AdjacencyMatrix,
	manager SegmentManagerForExport,
	judgments []RelevanceJudgment,
	cfg ExpansionFeatureConfig,
	out io.Writer,
) (int, error) {
	w := csv.NewWriter(out)
	defer w.Flush()

	if err := w.Write([]string{
		"query_id", "term_id", "energy", "df", "cosine_sim",
		"node_degree", "avg_neighbor_weight", "label",
	}); err != nil {
		return 0, err
	}

	graphCfg := cfg.Pipeline.Graph
	maxCandidates := cfg.MaxCandidatesPerQuery
	if maxCandidates <= 0 {
		maxCandidates = 50
	}

	count := 0

	for _, j := range judgments {
		tokens := cfg.AnalyzeQuery(j.QueryText)
		if len(tokens) == 0 {
			continue
		}

		// Build original token set
		originalSet := make(map[string]struct{}, len(tokens))
		for _, t := range tokens {
			originalSet[cfg.Field+"\x00"+t] = struct{}{}
		}

		// Spread activation from each token, aggregate
		allExpanded := make(map[string]float64)
		entryEnergy := 1.0 / float64(len(tokens))

		for _, token := range tokens {
			termID := cfg.Field + "\x00" + token
			spread := termMatrix.SpreadActivation(
				termID, entryEnergy,
				graphCfg.Hops, graphCfg.Decay,
				graphCfg.MaxFanOut, graphCfg.Epsilon, graphCfg.EnergyThreshold,
			)
			for id, w := range spread {
				if _, orig := originalSet[id]; orig {
					continue
				}
				allExpanded[id] += w
			}
		}

		// Sort by energy and limit
		type candidate struct {
			termID string
			energy float64
		}
		candidates := make([]candidate, 0, len(allExpanded))
		for id, e := range allExpanded {
			candidates = append(candidates, candidate{id, e})
		}
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].energy > candidates[j].energy
		})
		if len(candidates) > maxCandidates {
			candidates = candidates[:maxCandidates]
		}

		// Get posting hits for relevant docs (for labeling)
		relevantDocs := j.RelevantIDs

		for _, c := range candidates {
			df := manager.GetDF(c.termID)

			// Cosine similarity to query centroid
			cosineSim := 0.0
			if cfg.EmbeddingLookup != nil {
				if sim, ok := cfg.EmbeddingLookup.CentroidSimilarity(tokens, c.termID); ok {
					cosineSim = sim
				}
			}

			// Node degree and average neighbor weight
			edges := termMatrix.GetEdges(c.termID, 0)
			degree := len(edges)
			avgNeighborWeight := 0.0
			if degree > 0 {
				sum := 0.0
				for _, e := range edges {
					sum += e.Data.Weight
				}
				avgNeighborWeight = sum / float64(degree)
			}

			// Label: does this expansion term appear in relevant docs?
			label := 0
			if len(relevantDocs) > 0 {
				hits, err := manager.SearchByTermID(c.termID)
				if err == nil {
					for _, hit := range hits {
						if grade, ok := relevantDocs[hit.DocID]; ok && grade > 0 {
							label = 1
							break
						}
					}
				}
			}

			row := []string{
				j.QueryID,
				c.termID,
				strconv.FormatFloat(c.energy, 'f', 6, 64),
				strconv.FormatInt(df, 10),
				strconv.FormatFloat(cosineSim, 'f', 6, 64),
				strconv.Itoa(degree),
				strconv.FormatFloat(avgNeighborWeight, 'f', 6, 64),
				strconv.Itoa(label),
			}
			if err := w.Write(row); err != nil {
				return count, err
			}
			count++
		}
	}

	return count, nil
}

// SegmentManagerForExport is a minimal interface for the export functions,
// avoiding a hard dependency on the full SegmentManager.
type SegmentManagerForExport interface {
	GetDF(termID string) int64
	SearchByTermID(termID string) ([]indexstore.Hit, error)
	GetTotalDocs() int64
	Search(ctx context.Context, field, term string) ([]indexstore.Hit, error)
}
