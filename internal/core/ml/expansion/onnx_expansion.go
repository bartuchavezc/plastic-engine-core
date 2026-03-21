package expansion

import (
	"fmt"
	"sort"

	"plastic-engine-core/internal/core/ml"
	"plastic-engine-core/internal/core/models"
)

// ONNXExpansionScorer implements ml.ExpansionScorer using an ONNX LTR model.
type ONNXExpansionScorer struct {
	store     *models.ModelStore
	modelName string
}

var _ ml.ExpansionScorer = (*ONNXExpansionScorer)(nil)

// NewONNXExpansionScorer creates a new ONNX-based expansion scorer.
func NewONNXExpansionScorer(store *models.ModelStore, modelName string) *ONNXExpansionScorer {
	return &ONNXExpansionScorer{
		store:     store,
		modelName: modelName,
	}
}

// ScoreExpansions ranks expansion candidates using the ONNX LTR model.
// Features per candidate: [GraphEnergy, DF, CosineSim, queryLen]
func (s *ONNXExpansionScorer) ScoreExpansions(queryTokens []string, candidates []ml.ExpansionCandidate) ([]ml.ExpansionCandidate, error) {
	if len(candidates) == 0 {
		return candidates, nil
	}

	session, err := s.store.Get(s.modelName)
	if err != nil {
		return nil, fmt.Errorf("get model %s: %w", s.modelName, err)
	}

	const featureSize = 4
	batchInput := make([]float32, len(candidates)*featureSize)

	for i, c := range candidates {
		offset := i * featureSize
		batchInput[offset+0] = float32(c.GraphEnergy)
		batchInput[offset+1] = float32(c.DF)
		batchInput[offset+2] = float32(c.CosineSim)
		batchInput[offset+3] = float32(len(queryTokens))
	}

	scores, err := session.RunBatch(batchInput, len(candidates), featureSize)
	if err != nil {
		return nil, fmt.Errorf("run model %s: %w", s.modelName, err)
	}

	// Assign scores back to candidates as GraphEnergy (reused for sorting)
	type scored struct {
		candidate ml.ExpansionCandidate
		score     float32
	}
	scoredCandidates := make([]scored, len(candidates))
	for i, c := range candidates {
		s := float32(0)
		if i < len(scores) {
			s = scores[i]
		}
		scoredCandidates[i] = scored{candidate: c, score: s}
	}

	// Sort by score descending
	sort.Slice(scoredCandidates, func(i, j int) bool {
		return scoredCandidates[i].score > scoredCandidates[j].score
	})

	result := make([]ml.ExpansionCandidate, len(scoredCandidates))
	for i, sc := range scoredCandidates {
		sc.candidate.GraphEnergy = float64(sc.score)
		result[i] = sc.candidate
	}

	return result, nil
}
