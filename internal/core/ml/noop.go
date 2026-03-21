package ml

import "context"

// NoopEdgeWeighter always returns 0, signaling the caller to use statistical fallback.
type NoopEdgeWeighter struct{}

func (NoopEdgeWeighter) WeightEdge(_ EdgeWeightInput) (float64, error) { return 0, nil }

// NoopExpansionScorer returns candidates unchanged (no reranking).
type NoopExpansionScorer struct{}

func (NoopExpansionScorer) ScoreExpansions(_ []string, candidates []ExpansionCandidate) ([]ExpansionCandidate, error) {
	return candidates, nil
}

// NoopEmbeddingLookup always returns false (no embeddings available).
type NoopEmbeddingLookup struct{}

func (NoopEmbeddingLookup) CosineSimilarity(_, _ string) (float64, bool)             { return 0, false }
func (NoopEmbeddingLookup) CentroidSimilarity(_ []string, _ string) (float64, bool) { return 0, false }

// NoopGraphEnricher returns no new edges.
type NoopGraphEnricher struct{}

func (NoopGraphEnricher) EnrichBatch(_ context.Context, _ []string, _ func(string) []Edge) ([]NewEdge, error) {
	return nil, nil
}
