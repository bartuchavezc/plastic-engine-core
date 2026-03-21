package export

import (
	"fmt"
	"io"
	"os"
	"strings"

	"plastic-engine-core/internal/core/search/knowledge"
)

// ExportGraphTSV exports the co-occurrence graph as a TSV for Node2Vec training.
// Format: termA\ttermB\tweight (deduplicated, outgoing only where from < to).
// Returns the number of edges exported.
func ExportGraphTSV(termMatrix *knowledge.AdjacencyMatrix, outputPath string) (int, error) {
	f, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()
	return ExportGraphTSVTo(termMatrix, f)
}

// ExportGraphTSVTo writes graph TSV to any io.Writer.
func ExportGraphTSVTo(termMatrix *knowledge.AdjacencyMatrix, out io.Writer) (int, error) {
	if _, err := fmt.Fprintln(out, "termA\ttermB\tweight"); err != nil {
		return 0, err
	}

	count := 0
	err := termMatrix.IterateEdges(func(from, to string, data knowledge.EdgeData) bool {
		if from > to {
			return true
		}
		termA := stripFieldPrefix(from)
		termB := stripFieldPrefix(to)
		if _, werr := fmt.Fprintf(out, "%s\t%s\t%.6f\n", termA, termB, data.Weight); werr != nil {
			return false
		}
		count++
		return true
	})

	return count, err
}

// stripFieldPrefix removes the "field\x00" prefix from a term ID.
func stripFieldPrefix(termID string) string {
	if idx := strings.IndexByte(termID, 0); idx >= 0 {
		return termID[idx+1:]
	}
	return termID
}
