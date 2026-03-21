package export

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"plastic-engine-core/internal/core/search/knowledge"
)

// EdgeFeatureRow is a single row in the edge features CSV.
type EdgeFeatureRow struct {
	TermA       string
	TermB       string
	CooccCount  int64 // approximated from weight if not tracked directly
	DFA         int64
	DFB         int64
	TotalDocs   int64
	AvgWeight   float64
	NPMI        float64
	LLR         float64
	Dice        float64
	LogCooccCnt float64
	DFRatio     float64
	Weight      float64 // current edge weight (target for supervised training)
	Source      string
}

// ExportEdgeFeatures scans all co-occurrence edges from the term matrix and
// enriches them with DF from the posting store snapshot, producing a CSV
// suitable for training an edge weight prediction model (notebook 01).
//
// The posting DF snapshot should come from Manager.store.SnapshotDF().
// The term matrix DFs come from AdjacencyMatrix.ListTermDFs().
//
// CSV columns: termA,termB,cooccCount,dfA,dfB,totalDocs,avgWeight,npmi,llr,dice,logCooccCount,dfRatio,target_weight,source
func ExportEdgeFeatures(
	termMatrix *knowledge.AdjacencyMatrix,
	postingDFSnapshot map[string]int64,
	totalDocs int64,
	outputPath string,
) (int, error) {
	f, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()
	return ExportEdgeFeaturesTo(termMatrix, postingDFSnapshot, totalDocs, f)
}

// ExportEdgeFeaturesTo writes edge features CSV to any io.Writer.
func ExportEdgeFeaturesTo(
	termMatrix *knowledge.AdjacencyMatrix,
	postingDFSnapshot map[string]int64,
	totalDocs int64,
	out io.Writer,
) (int, error) {
	w := csv.NewWriter(out)
	defer w.Flush()

	// Header
	if err := w.Write([]string{
		"termA", "termB", "cooccCount", "dfA", "dfB", "totalDocs",
		"avgWeight", "npmi", "llr", "dice", "logCooccCount", "dfRatio", "target_weight", "source",
	}); err != nil {
		return 0, err
	}

	// Get term DFs from the matrix itself (used for NPMI/LLR/Dice computation)
	matrixDFs := termMatrix.ListTermDFs()

	count := 0
	err := termMatrix.IterateEdges(func(from, to string, data knowledge.EdgeData) bool {
		// Only export one direction to avoid duplicates
		if from > to {
			return true
		}

		// Lookup DFs — prefer posting store snapshot, fallback to matrix DFs
		dfA := lookupDF(from, postingDFSnapshot, matrixDFs)
		dfB := lookupDF(to, postingDFSnapshot, matrixDFs)

		if dfA <= 0 || dfB <= 0 || totalDocs <= 0 {
			return true
		}

		// Estimate cooccCount from edge weight.
		// For NPMI edges: weight = npmi × (1 + avgWeight), so
		// we can approximate cooccCount from the data we have.
		// Since we don't store raw cooccCount in EdgeData, we use the weight
		// and DF to back-calculate an estimate.
		estimatedCooccCount := estimateCooccCount(data.Weight, dfA, dfB, totalDocs)
		if estimatedCooccCount <= 0 {
			estimatedCooccCount = 1
		}

		n := float64(totalDocs)
		pA := float64(dfA) / n
		pB := float64(dfB) / n
		pAB := float64(estimatedCooccCount) / n

		// NPMI
		npmi := 0.0
		if pAB > 0 && pA > 0 && pB > 0 {
			pmi := math.Log(pAB / (pA * pB))
			negLogPAB := -math.Log(pAB)
			if negLogPAB != 0 {
				npmi = pmi / negLogPAB
			}
		}

		// LLR
		llr := computeLLR(estimatedCooccCount, dfA, dfB, totalDocs)

		// Dice
		dice := 0.0
		if dfA+dfB > 0 {
			dice = 2.0 * float64(estimatedCooccCount) / float64(dfA+dfB)
		}

		logCooccCnt := math.Log1p(float64(estimatedCooccCount))

		dfRatio := 0.0
		if dfB > 0 {
			dfRatio = float64(dfA) / float64(dfB)
			if dfRatio > 1 {
				dfRatio = 1.0 / dfRatio // normalize to [0,1]
			}
		}

		row := []string{
			from,
			to,
			strconv.FormatInt(estimatedCooccCount, 10),
			strconv.FormatInt(dfA, 10),
			strconv.FormatInt(dfB, 10),
			strconv.FormatInt(totalDocs, 10),
			strconv.FormatFloat(data.Weight, 'f', 6, 64),
			strconv.FormatFloat(npmi, 'f', 6, 64),
			strconv.FormatFloat(llr, 'f', 6, 64),
			strconv.FormatFloat(dice, 'f', 6, 64),
			strconv.FormatFloat(logCooccCnt, 'f', 6, 64),
			strconv.FormatFloat(dfRatio, 'f', 6, 64),
			strconv.FormatFloat(data.Weight, 'f', 6, 64), // target_weight = current weight
			data.Source,
		}
		if err := w.Write(row); err != nil {
			return false
		}
		count++
		return true
	})

	return count, err
}

// lookupDF checks the posting snapshot first, then the matrix DFs.
func lookupDF(termID string, postingDFs, matrixDFs map[string]int64) int64 {
	if df, ok := postingDFs[termID]; ok && df > 0 {
		return df
	}
	if df, ok := matrixDFs[termID]; ok && df > 0 {
		return df
	}
	return 0
}

// estimateCooccCount back-calculates co-occurrence count from NPMI weight.
// Since weight = clamp(npmi × (1 + avgWeight), 0, 1), we assume avgWeight ≈ 0.5
// and invert the NPMI formula to get an approximate cooccurrence count.
func estimateCooccCount(weight float64, dfA, dfB, totalDocs int64) int64 {
	if weight <= 0 || totalDocs <= 0 || dfA <= 0 || dfB <= 0 {
		return 0
	}

	// Approximate: npmi ≈ weight / 1.5 (assuming avgWeight ≈ 0.5)
	npmi := weight / 1.5
	if npmi > 1 {
		npmi = 1
	}
	if npmi <= 0 {
		return 1
	}

	n := float64(totalDocs)
	pA := float64(dfA) / n
	pB := float64(dfB) / n

	// npmi = log(pAB / (pA×pB)) / -log(pAB)
	// This is transcendental — use Newton's method to find pAB.
	// Start with pAB = pA × pB (independence) and iterate.
	pAB := pA * pB * 2 // initial guess above independence
	for iter := 0; iter < 20; iter++ {
		if pAB <= 0 || pAB >= 1 {
			break
		}
		logPAB := math.Log(pAB)
		if logPAB == 0 {
			break
		}
		pmi := math.Log(pAB / (pA * pB))
		currentNPMI := pmi / (-logPAB)
		err := currentNPMI - npmi

		// Derivative of NPMI w.r.t. pAB (chain rule)
		dPMI := 1.0 / pAB
		dNegLog := -1.0 / pAB
		dNPMI := (dPMI*(-logPAB) - pmi*dNegLog) / (logPAB * logPAB)
		if math.Abs(dNPMI) < 1e-12 {
			break
		}
		pAB -= err / dNPMI
		if pAB < 0 {
			pAB = 1.0 / n
		}
	}

	count := int64(pAB * n)
	if count < 1 {
		count = 1
	}
	return count
}

// computeLLR computes log-likelihood ratio for a 2×2 contingency table.
func computeLLR(cooccCount, dfA, dfB, totalDocs int64) float64 {
	n := float64(totalDocs)
	k11 := float64(cooccCount)
	k12 := float64(dfA) - k11
	k21 := float64(dfB) - k11
	k22 := n - float64(dfA) - float64(dfB) + k11

	if k12 < 0 {
		k12 = 0
	}
	if k21 < 0 {
		k21 = 0
	}
	if k22 < 0 {
		k22 = 0
	}

	llr := 0.0
	cells := [4]struct{ obs, row, col float64 }{
		{k11, k11 + k12, k11 + k21},
		{k12, k11 + k12, k12 + k22},
		{k21, k21 + k22, k11 + k21},
		{k22, k21 + k22, k12 + k22},
	}
	for _, c := range cells {
		if c.obs > 0 && c.row > 0 && c.col > 0 {
			expected := c.row * c.col / n
			if expected > 0 {
				llr += c.obs * math.Log(c.obs/expected)
			}
		}
	}
	return llr * 2
}
