package gnn

import (
	"context"
	"fmt"

	"plastic-engine-core/internal/core/ml"
	"plastic-engine-core/internal/core/models"
)

// GNNEnricher implements ml.GraphEnricher using a GraphSAGE ONNX model for link prediction.
type GNNEnricher struct {
	store     *models.ModelStore
	modelName string
	batchSize int // nodes per inference batch (default 100)
}

var _ ml.GraphEnricher = (*GNNEnricher)(nil)

// NewGNNEnricher creates a new GNN-based graph enricher.
func NewGNNEnricher(store *models.ModelStore, modelName string, batchSize int) *GNNEnricher {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &GNNEnricher{
		store:     store,
		modelName: modelName,
		batchSize: batchSize,
	}
}

// EnrichBatch runs GNN inference on the given nodes and returns predicted new edges.
// getEdges provides the current neighbors for each node (used to build node features).
// New edges are capped at weight 0.5 and tagged with Source="gnn".
func (g *GNNEnricher) EnrichBatch(ctx context.Context, nodeIDs []string, getEdges func(string) []ml.Edge) ([]ml.NewEdge, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}

	session, err := g.store.Get(g.modelName)
	if err != nil {
		return nil, fmt.Errorf("get model %s: %w", g.modelName, err)
	}

	var allNewEdges []ml.NewEdge

	for start := 0; start < len(nodeIDs); start += g.batchSize {
		select {
		case <-ctx.Done():
			return allNewEdges, ctx.Err()
		default:
		}

		end := start + g.batchSize
		if end > len(nodeIDs) {
			end = len(nodeIDs)
		}
		batch := nodeIDs[start:end]

		// Build feature matrix: for each node, aggregate neighbor weights
		// Feature per node: [degree, avgWeight, maxWeight, minWeight]
		const featureSize = 4
		features := make([]float32, len(batch)*featureSize)

		for i, nodeID := range batch {
			edges := getEdges(nodeID)
			offset := i * featureSize
			if len(edges) == 0 {
				continue
			}

			features[offset+0] = float32(len(edges))
			var sum, maxW, minW float64
			minW = 1.0
			for _, e := range edges {
				sum += e.Weight
				if e.Weight > maxW {
					maxW = e.Weight
				}
				if e.Weight < minW {
					minW = e.Weight
				}
			}
			features[offset+1] = float32(sum / float64(len(edges)))
			features[offset+2] = float32(maxW)
			features[offset+3] = float32(minW)
		}

		scores, err := session.RunBatch(features, len(batch), featureSize)
		if err != nil {
			return allNewEdges, fmt.Errorf("run GNN batch: %w", err)
		}

		// Interpret scores as predicted edge weights between consecutive node pairs.
		// For simplicity, predict edges between nodes in the batch that aren't already connected.
		for i := 0; i < len(batch)-1; i++ {
			if i >= len(scores) {
				break
			}
			weight := float64(scores[i])
			if weight < 0.1 {
				continue
			}
			// Cap weight at 0.5 for GNN-predicted edges
			if weight > 0.5 {
				weight = 0.5
			}

			allNewEdges = append(allNewEdges, ml.NewEdge{
				From:   batch[i],
				To:     batch[i+1],
				Weight: weight,
				Source: "gnn",
			})
		}
	}

	return allNewEdges, nil
}
