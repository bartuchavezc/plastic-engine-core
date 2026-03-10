package query

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// DefaultLimit is the number of hits returned when the client omits a limit.
	DefaultLimit = 20
	// MaxLimit caps the number of hits that can be requested in a single query.
	MaxLimit = 100
)

// Request describes the payload accepted by the search cluster.
type Request struct {
	IndexID  string   `json:"index_id,omitempty"`
	ShardIDs []string `json:"shard_ids,omitempty"`
	Query    Clause   `json:"query"`
	Filters  []Clause `json:"filters,omitempty"`
	Limit    int      `json:"limit,omitempty"`
	Cursor   string   `json:"cursor,omitempty"`
}

// ValidationError indicates the payload failed semantic validation.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// Clause represents a logical unit in the query tree.
type Clause struct {
	Term   *TermQuery   `json:"term,omitempty"`
	Match  *MatchQuery  `json:"match,omitempty"`
	Range  *RangeQuery  `json:"range,omitempty"`
	Prefix *PrefixQuery `json:"prefix,omitempty"`
	Hybrid *HybridQuery `json:"hybrid,omitempty"`
}

// TermQuery matches documents whose field exactly equals the provided value.
type TermQuery struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

// MatchQuery performs analyzer/tokenizer processing and scoring on the field.
type MatchQuery struct {
	Field     string  `json:"field"`
	Value     string  `json:"value"`
	Boost     float64 `json:"boost,omitempty"`
	Fuzziness int     `json:"fuzziness,omitempty"` // 0 = disabled, 1-2 = max DL edit distance for zero-hit tokens
}

// RangeQuery constraints the field to a value window.
type RangeQuery struct {
	Field string `json:"field"`
	GTE   any    `json:"gte,omitempty"`
	GT    any    `json:"gt,omitempty"`
	LTE   any    `json:"lte,omitempty"`
	LT    any    `json:"lt,omitempty"`
}

// PrefixQuery matches documents whose field starts with the given prefix.
// Uses edge n-grams for efficient prefix matching.
type PrefixQuery struct {
	Field string  `json:"field"`
	Value string  `json:"value"`
	Boost float64 `json:"boost,omitempty"`
}

// HybridQuery performs BM25 scoring on original tokens plus graph-expanded
// terms from the term co-occurrence matrix via SpreadTopK.
type HybridQuery struct {
	Field        string  `json:"field"`
	Value        string  `json:"value"`
	Boost        float64 `json:"boost,omitempty"`
	Hops         int     `json:"hops,omitempty"`
	Decay        float64 `json:"decay,omitempty"`
	MaxFanOut    int     `json:"max_fan_out,omitempty"`
	Epsilon      float64 `json:"epsilon,omitempty"`
	Fuzziness    int     `json:"fuzziness,omitempty"` // 0 = disabled, 1-2 = max DL edit distance for zero-hit tokens
	ExpansionCap    float64 `json:"expansion_cap,omitempty"`    // max boost multiplier for expanded terms (default 0.3)
	MaxDF           int64   `json:"max_df,omitempty"`           // skip expanded terms with DF above this threshold (default 5000)
	EnergyThreshold float64 `json:"energy_threshold,omitempty"` // minimum energy to explore a node during spread activation (default 0.01)
}

// Normalize prepares the request by trimming whitespace, applying defaults and
// computing derived attributes such as boosts.
func (r *Request) Normalize() error {
	r.IndexID = strings.TrimSpace(r.IndexID)
	r.Cursor = strings.TrimSpace(r.Cursor)

	if r.Limit == 0 {
		r.Limit = DefaultLimit
	}

	if err := r.Query.normalize(true); err != nil {
		return err
	}

	for i := range r.Filters {
		if err := r.Filters[i].normalize(false); err != nil {
			return err
		}
	}

	return nil
}

// Validate ensures the request contains a coherent query definition.
func (r Request) Validate() error {
	if r.IndexID == "" && len(r.ShardIDs) == 0 {
		return &ValidationError{
			Field:   "index_id or shard_ids",
			Message: "either index_id or shard_ids must be provided",
		}
	}

	if r.IndexID != "" && len(r.ShardIDs) > 0 {
		return &ValidationError{
			Field:   "index_id and shard_ids",
			Message: "cannot specify both index_id and shard_ids",
		}
	}

	if r.Limit <= 0 {
		return &ValidationError{
			Field:   "limit",
			Message: "limit must be positive",
		}
	}
	if r.Limit > MaxLimit {
		return &ValidationError{
			Field:   "limit",
			Message: fmt.Sprintf("limit cannot exceed %d", MaxLimit),
		}
	}

	if err := r.Query.validate(true, "query"); err != nil {
		return err
	}

	for i := range r.Filters {
		if err := r.Filters[i].validate(false, fmt.Sprintf("filters[%d]", i)); err != nil {
			return err
		}
	}

	return nil
}

func (c *Clause) normalize(allowMatch bool) error {
	set := c.populated()
	if set == 0 {
		return &ValidationError{
			Field:   "clause",
			Message: "one query operator must be provided",
		}
	}
	if set > 1 {
		return &ValidationError{
			Field:   "clause",
			Message: "only one query operator can be provided per clause",
		}
	}

	if c.Term != nil {
		return c.Term.normalize()
	}
	if c.Match != nil {
		if !allowMatch {
			return &ValidationError{
				Field:   "clause.match",
				Message: "match operator is not allowed in this context",
			}
		}
		return c.Match.normalize()
	}
	if c.Range != nil {
		return c.Range.normalize()
	}
	if c.Prefix != nil {
		if !allowMatch {
			return &ValidationError{
				Field:   "clause.prefix",
				Message: "prefix operator is not allowed in this context",
			}
		}
		return c.Prefix.normalize()
	}
	if c.Hybrid != nil {
		if !allowMatch {
			return &ValidationError{
				Field:   "clause.hybrid",
				Message: "hybrid operator is not allowed in this context",
			}
		}
		return c.Hybrid.normalize()
	}

	return nil
}

func (c Clause) validate(allowMatch bool, fieldPrefix string) error {
	set := c.populated()
	if set == 0 {
		return &ValidationError{
			Field:   fieldPrefix,
			Message: "clause must contain exactly one operator",
		}
	}
	if set > 1 {
		return &ValidationError{
			Field:   fieldPrefix,
			Message: "clause contains more than one operator",
		}
	}

	switch {
	case c.Term != nil:
		return c.Term.validate(fieldPrefix + ".term")
	case c.Match != nil:
		if !allowMatch {
			return &ValidationError{
				Field:   fieldPrefix + ".match",
				Message: "match operator is not allowed here",
			}
		}
		return c.Match.validate(fieldPrefix + ".match")
	case c.Range != nil:
		return c.Range.validate(fieldPrefix + ".range")
	case c.Prefix != nil:
		if !allowMatch {
			return &ValidationError{
				Field:   fieldPrefix + ".prefix",
				Message: "prefix operator is not allowed here",
			}
		}
		return c.Prefix.validate(fieldPrefix + ".prefix")
	case c.Hybrid != nil:
		if !allowMatch {
			return &ValidationError{
				Field:   fieldPrefix + ".hybrid",
				Message: "hybrid operator is not allowed here",
			}
		}
		return c.Hybrid.validate(fieldPrefix + ".hybrid")
	default:
		// Should be unreachable because populated() already guards this, but keep defensive.
		return &ValidationError{
			Field:   fieldPrefix,
			Message: "clause missing operator",
		}
	}
}

func (c Clause) populated() int {
	count := 0
	if c.Term != nil {
		count++
	}
	if c.Match != nil {
		count++
	}
	if c.Range != nil {
		count++
	}
	if c.Prefix != nil {
		count++
	}
	if c.Hybrid != nil {
		count++
	}
	return count
}

func (t *TermQuery) normalize() error {
	t.Field = strings.TrimSpace(t.Field)
	if s, ok := t.Value.(string); ok {
		t.Value = strings.TrimSpace(s)
	}
	return nil
}

func (t TermQuery) validate(fieldPrefix string) error {
	if t.Field == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".field",
			Message: "field is required",
		}
	}
	if t.Value == nil {
		return &ValidationError{
			Field:   fieldPrefix + ".value",
			Message: "value is required",
		}
	}
	if s, ok := t.Value.(string); ok && strings.TrimSpace(s) == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".value",
			Message: "value is required",
		}
	}
	return nil
}

func (m *MatchQuery) normalize() error {
	field, boost, err := parseBoostedField(m.Field)
	if err != nil {
		return err
	}
	m.Field = field

	m.Value = strings.TrimSpace(m.Value)
	if m.Boost <= 0 {
		if boost > 0 {
			m.Boost = boost
		} else {
			m.Boost = 1
		}
	} else if boost > 0 {
		m.Boost *= boost
	}

	return nil
}

func (m MatchQuery) validate(fieldPrefix string) error {
	if m.Field == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".field",
			Message: "field is required",
		}
	}
	if m.Value == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".value",
			Message: "value is required",
		}
	}
	if m.Boost <= 0 {
		return &ValidationError{
			Field:   fieldPrefix + ".boost",
			Message: "boost must be greater than zero",
		}
	}
	return nil
}

func (r *RangeQuery) normalize() error {
	r.Field = strings.TrimSpace(r.Field)
	return nil
}

func (r RangeQuery) validate(fieldPrefix string) error {
	if r.Field == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".field",
			Message: "field is required",
		}
	}

	if r.GT == nil && r.GTE == nil && r.LT == nil && r.LTE == nil {
		return &ValidationError{
			Field:   fieldPrefix,
			Message: "at least one bound must be provided",
		}
	}

	return nil
}

func parseBoostedField(raw string) (string, float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0, &ValidationError{
			Field:   "match.field",
			Message: "field is required",
		}
	}

	parts := strings.SplitN(raw, "^", 2)
	field := strings.TrimSpace(parts[0])
	if field == "" {
		return "", 0, &ValidationError{
			Field:   "match.field",
			Message: "field is required",
		}
	}

	if len(parts) == 1 {
		return field, 0, nil
	}

	boostRaw := strings.TrimSpace(parts[1])
	if boostRaw == "" {
		return "", 0, &ValidationError{
			Field:   "match.field",
			Message: "boost value is required after ^",
		}
	}

	value, err := strconv.ParseFloat(boostRaw, 64)
	if err != nil {
		return "", 0, &ValidationError{
			Field:   "match.field",
			Message: "boost value must be numeric",
		}
	}

	if value <= 0 {
		return "", 0, &ValidationError{
			Field:   "match.field",
			Message: "boost must be greater than zero",
		}
	}

	return field, value, nil
}

func (p *PrefixQuery) normalize() error {
	field, boost, err := parseBoostedField(p.Field)
	if err != nil {
		return err
	}
	p.Field = field

	p.Value = strings.TrimSpace(p.Value)
	p.Value = strings.ToLower(p.Value) // Normalize to lowercase for prefix matching

	if p.Boost <= 0 {
		if boost > 0 {
			p.Boost = boost
		} else {
			p.Boost = 1
		}
	} else if boost > 0 {
		p.Boost *= boost
	}

	return nil
}

func (p PrefixQuery) validate(fieldPrefix string) error {
	if p.Field == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".field",
			Message: "field is required",
		}
	}
	if p.Value == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".value",
			Message: "prefix value is required",
		}
	}
	if p.Boost <= 0 {
		return &ValidationError{
			Field:   fieldPrefix + ".boost",
			Message: "boost must be greater than zero",
		}
	}
	return nil
}

func (h *HybridQuery) normalize() error {
	field, boost, err := parseBoostedField(h.Field)
	if err != nil {
		return err
	}
	h.Field = field

	h.Value = strings.TrimSpace(h.Value)
	if h.Boost <= 0 {
		if boost > 0 {
			h.Boost = boost
		} else {
			h.Boost = 1
		}
	} else if boost > 0 {
		h.Boost *= boost
	}

	if h.Hops <= 0 {
		h.Hops = 1
	}
	if h.Decay <= 0 {
		h.Decay = 0.7
	}
	if h.MaxFanOut <= 0 {
		h.MaxFanOut = 15
	}
	if h.Epsilon <= 0 {
		h.Epsilon = 0.05
	}
	if h.ExpansionCap <= 0 {
		h.ExpansionCap = 0.3
	}
	if h.MaxDF <= 0 {
		h.MaxDF = 5000
	}
	if h.EnergyThreshold <= 0 {
		h.EnergyThreshold = 0.01
	}

	return nil
}

func (h HybridQuery) validate(fieldPrefix string) error {
	if h.Field == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".field",
			Message: "field is required",
		}
	}
	if h.Value == "" {
		return &ValidationError{
			Field:   fieldPrefix + ".value",
			Message: "value is required",
		}
	}
	if h.Boost <= 0 {
		return &ValidationError{
			Field:   fieldPrefix + ".boost",
			Message: "boost must be greater than zero",
		}
	}
	if h.Hops < 1 || h.Hops > 3 {
		return &ValidationError{
			Field:   fieldPrefix + ".hops",
			Message: "hops must be between 1 and 3",
		}
	}
	return nil
}
