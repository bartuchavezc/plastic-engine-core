package weights

import (
	"fmt"
	"math"

	"plastic-engine-core/internal/core/ml"
	"plastic-engine-core/internal/core/models"
)

// ONNXWeighter implements ml.EdgeWeighter using an ONNX model for edge weight prediction.
type ONNXWeighter struct {
	store     *models.ModelStore
	modelName string
}

var _ ml.EdgeWeighter = (*ONNXWeighter)(nil)

// NewONNXWeighter creates a new ONNX-based edge weighter.
func NewONNXWeighter(store *models.ModelStore, modelName string) *ONNXWeighter {
	return &ONNXWeighter{
		store:     store,
		modelName: modelName,
	}
}

// WeightEdge runs the ONNX model to predict an edge weight.
// Input features: [cooccCount, dfA, dfB, totalDocs, avgWeight, currentNPMI]
// Output: clamped weight in [0, 1].
func (w *ONNXWeighter) WeightEdge(input ml.EdgeWeightInput) (float64, error) {
	session, err := w.store.Get(w.modelName)
	if err != nil {
		return 0, fmt.Errorf("get model %s: %w", w.modelName, err)
	}

	// Build input tensor with engineered features
	features := []float32{
		float32(input.CooccCount),
		float32(input.DFA),
		float32(input.DFB),
		float32(input.TotalDocs),
		float32(input.AvgWeight),
		float32(input.CurrentNPMI),
	}

	output, err := session.Run(features)
	if err != nil {
		return 0, fmt.Errorf("run model %s: %w", w.modelName, err)
	}

	if len(output) == 0 {
		return 0, nil
	}

	// Clamp to [0, 1]
	weight := float64(output[0])
	weight = math.Max(0, math.Min(1, weight))
	return weight, nil
}
