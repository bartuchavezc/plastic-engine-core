package ml

import "context"

// EdgeWeightInput contains features for ML-based edge weight prediction.
type EdgeWeightInput struct {
	CooccCount  int64
	DFA, DFB    int64
	TotalDocs   int64
	AvgWeight   float64
	CurrentNPMI float64
}

// EdgeWeighter computes edge weights using a trained model.
type EdgeWeighter interface {
	WeightEdge(input EdgeWeightInput) (float64, error)
}

// ExpansionCandidate represents a candidate term for query expansion.
type ExpansionCandidate struct {
	TermID      string
	GraphEnergy float64
	DF          int64
	CosineSim   float64 // 0 if no embeddings available
}

// ExpansionScorer ranks expansion candidates using a trained LTR model.
type ExpansionScorer interface {
	ScoreExpansions(queryTokens []string, candidates []ExpansionCandidate) ([]ExpansionCandidate, error)
}

// EmbeddingLookup provides cosine similarity between term embeddings.
type EmbeddingLookup interface {
	CosineSimilarity(termA, termB string) (float64, bool)
	CentroidSimilarity(queryTerms []string, candidate string) (float64, bool)
}

// Edge is a lightweight edge representation for GNN enrichment callbacks.
type Edge struct {
	To     string
	Weight float64
}

// NewEdge represents a predicted edge from GNN enrichment.
type NewEdge struct {
	From   string
	To     string
	Weight float64
	Source string
}

// GraphEnricher discovers new edges via GNN link prediction.
type GraphEnricher interface {
	EnrichBatch(ctx context.Context, nodeIDs []string, getEdges func(string) []Edge) ([]NewEdge, error)
}
