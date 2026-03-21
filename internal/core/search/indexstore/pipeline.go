package indexstore

// SearchPipeline defines how a search index traverses its co-occurrence graph
// and scores results. Persisted in IndexDefinition and synced to search nodes
// via Assignment. Per-query overrides take precedence.
type SearchPipeline struct {
	Name    string               `json:"name,omitempty"`
	Graph   GraphTraversalConfig `json:"graph"`
	Scoring ScoringConfig        `json:"scoring"`
}

// GraphTraversalConfig controls spread-activation traversal on the co-occurrence graph.
type GraphTraversalConfig struct {
	Hops            int     `json:"hops,omitempty"`
	Decay           float64 `json:"decay,omitempty"`
	MaxFanOut       int     `json:"max_fan_out,omitempty"`
	Epsilon         float64 `json:"epsilon,omitempty"`
	EnergyThreshold float64 `json:"energy_threshold,omitempty"`
	MaxExpansions   int     `json:"max_expansions,omitempty"`
	MaxDF           int64   `json:"max_df,omitempty"`
	ExpansionCap    float64 `json:"expansion_cap,omitempty"`
}

// ScoringConfig holds BM25 and ranking parameters.
type ScoringConfig struct {
	K1             float64 `json:"k1,omitempty"`
	B              float64 `json:"b,omitempty"`
	CoordWeight    float64 `json:"coord_weight,omitempty"`
	OrderingFactor float64 `json:"ordering_factor,omitempty"`
	ExpansionBlend float64 `json:"expansion_blend,omitempty"`
}

// DefaultSearchPipeline is the single source of truth for scoring and traversal defaults.
func DefaultSearchPipeline() SearchPipeline {
	return SearchPipeline{
		Graph:   DefaultGraphTraversalConfig(),
		Scoring: DefaultScoringConfig(),
	}
}

// DefaultGraphTraversalConfig returns default graph traversal parameters.
func DefaultGraphTraversalConfig() GraphTraversalConfig {
	return GraphTraversalConfig{
		Hops:            3,
		Decay:           0.7,
		MaxFanOut:       15,
		Epsilon:         0.05,
		EnergyThreshold: 0.01,
		MaxExpansions:   10,
		MaxDF:           5000,
		ExpansionCap:    0.3,
	}
}

// DefaultScoringConfig returns default scoring parameters.
func DefaultScoringConfig() ScoringConfig {
	return ScoringConfig{
		K1:             1.2,
		B:              0.75,
		CoordWeight:    2.0,
		OrderingFactor: 0.5,
		ExpansionBlend: 0.3,
	}
}

// Merge applies non-zero overrides from over onto the receiver, returning a new pipeline.
func (p SearchPipeline) Merge(over SearchPipeline) SearchPipeline {
	out := p
	if over.Name != "" {
		out.Name = over.Name
	}
	out.Graph = out.Graph.merge(over.Graph)
	out.Scoring = out.Scoring.merge(over.Scoring)
	return out
}

func (g GraphTraversalConfig) merge(over GraphTraversalConfig) GraphTraversalConfig {
	out := g
	if over.Hops > 0 {
		out.Hops = over.Hops
	}
	if over.Decay > 0 {
		out.Decay = over.Decay
	}
	if over.MaxFanOut > 0 {
		out.MaxFanOut = over.MaxFanOut
	}
	if over.Epsilon > 0 {
		out.Epsilon = over.Epsilon
	}
	if over.EnergyThreshold > 0 {
		out.EnergyThreshold = over.EnergyThreshold
	}
	if over.MaxExpansions > 0 {
		out.MaxExpansions = over.MaxExpansions
	}
	if over.MaxDF > 0 {
		out.MaxDF = over.MaxDF
	}
	if over.ExpansionCap > 0 {
		out.ExpansionCap = over.ExpansionCap
	}
	return out
}

func (s ScoringConfig) merge(over ScoringConfig) ScoringConfig {
	out := s
	if over.K1 > 0 {
		out.K1 = over.K1
	}
	if over.B > 0 {
		out.B = over.B
	}
	if over.CoordWeight > 0 {
		out.CoordWeight = over.CoordWeight
	}
	if over.OrderingFactor > 0 {
		out.OrderingFactor = over.OrderingFactor
	}
	if over.ExpansionBlend > 0 {
		out.ExpansionBlend = over.ExpansionBlend
	}
	return out
}
