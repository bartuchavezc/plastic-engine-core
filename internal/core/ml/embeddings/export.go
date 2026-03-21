package embeddings

import (
	"fmt"
	"os"
	"strings"

	"plastic-engine-core/internal/core/search/knowledge"
)

// ExportGraphForTraining exports the co-occurrence graph as a TSV file for Node2Vec training.
// Format: termA\ttermB\tweight (one edge per line, deduplicated by From < To).
func ExportGraphForTraining(am *knowledge.AdjacencyMatrix, outputPath string) (int, error) {
	f, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	if _, err := fmt.Fprintln(f, "termA\ttermB\tweight"); err != nil {
		return 0, err
	}

	count := 0
	err = am.IterateEdges(func(from, to string, data knowledge.EdgeData) bool {
		// Only export one direction to avoid duplicates
		if from > to {
			return true
		}

		termA := stripFieldPrefix(from)
		termB := stripFieldPrefix(to)
		if _, werr := fmt.Fprintf(f, "%s\t%s\t%.6f\n", termA, termB, data.Weight); werr != nil {
			return false
		}
		count++
		return true
	})

	return count, err
}

// stripFieldPrefix removes the "field\x00" prefix from a term ID if present.
func stripFieldPrefix(termID string) string {
	if idx := strings.IndexByte(termID, 0); idx >= 0 {
		return termID[idx+1:]
	}
	return termID
}
