package indexstore

import (
	"math"
	"sort"
	"sync"
	"time"
)

// posEntry pairs a document position with the index into the terms slice and its sentence.
// Used to build a position-ordered view for sliding window extraction.
type posEntry struct {
	pos        int
	termIdx    int
	sentenceID int
}

// posEntriesPool reuses slices for position-ordered extraction.
var posEntriesPool = sync.Pool{
	New: func() any {
		s := make([]posEntry, 0, 256)
		return &s
	},
}

// CooccurrenceConfig configures the co-occurrence accumulator.
type CooccurrenceConfig struct {
	// Disabled skips co-occurrence extraction entirely.
	// Useful for benchmarking pure Pebble ingestion throughput in isolation.
	Disabled bool `json:"disabled,omitempty"`

	// WindowSize is the maximum positional distance within a sentence to count as co-occurring.
	// 0 = sentence boundary only (default). Positive values cap within sentence.
	WindowSize int `json:"window_size,omitempty"`

	// MinPairCount is the minimum co-occurrences before creating an edge.
	// Default: 3.
	MinPairCount int64 `json:"min_pair_count,omitempty"`

	// MaxDFThreshold skips ultra-common terms at drain time.
	// Default: 10000.
	MaxDFThreshold int64 `json:"max_df_threshold,omitempty"`

	// DistanceDecay controls the positional decay function: "log" (default), "linear", "quadratic".
	DistanceDecay string `json:"distance_decay,omitempty"`

	// IncludeFields restricts co-occurrence extraction to these fields. Empty = all fields.
	IncludeFields []string `json:"include_fields,omitempty"`

	// WeightingMethod selects the edge weight formula: "npmi" (default), "llr", "dice", "ml".
	WeightingMethod string `json:"weighting_method,omitempty"`

	// WeightingModel is the model name in the ModelStore, used when WeightingMethod == "ml".
	WeightingModel string `json:"weighting_model,omitempty"`

	// PositionGapMode controls position numbering: "original" (default) keeps analyzer gaps,
	// "compressed" renumbers positions sequentially within each sentence after stopword removal.
	PositionGapMode string `json:"position_gap_mode,omitempty"`

	// Worker tuning (node-level, not persisted in index definition)
	DrainInterval     time.Duration `json:"-"`
	MaxExtractWorkers int           `json:"-"`
	MaxBufferSize     int           `json:"-"`
	EpochInterval     time.Duration `json:"-"`
}

// DefaultCooccurrenceConfig returns sensible defaults.
func DefaultCooccurrenceConfig() CooccurrenceConfig {
	return CooccurrenceConfig{
		WindowSize:     0, // 0 = sentence boundary only
		MaxDFThreshold: 10000,
		DrainInterval:  10 * time.Second,
		MinPairCount:   3,
	}
}

// cooccurrencePair is a flat struct for a single co-occurrence observation.
// Replaces map[cooccurrencePairKey]*cooccurrenceStats to eliminate map overhead
// and reduce GC pressure from pointer-heavy structures.
type cooccurrencePair struct {
	TermA       string
	TermB       string
	Count       int64   // number of observations (for NPMI)
	WeightedSum float64 // Σ 1/log₂(1+dist) — positional decay weight
}

// pairLess returns true if a sorts before b by (TermA, TermB).
func pairLess(a, b *cooccurrencePair) bool {
	if a.TermA != b.TermA {
		return a.TermA < b.TermA
	}
	return a.TermB < b.TermB
}

// localPairsPool reuses slices for per-document pair extraction.
// Each ExtractFromDocument call borrows a slice, appends pairs, merges into
// global buffer, then returns the slice. Avoids per-call heap allocations.
var localPairsPool = sync.Pool{
	New: func() any {
		s := make([]cooccurrencePair, 0, 256)
		return &s
	},
}

// prefixedTermsPool reuses slices for pre-built "field\x00term" strings.
// Avoids O(n²) string concatenation in the pair extraction loop.
var prefixedTermsPool = sync.Pool{
	New: func() any {
		s := make([]string, 0, 64)
		return &s
	},
}

// cooccurrenceAccumulator buffers term co-occurrence pairs with distance info.
// It processes documents asynchronously via a channel + worker pool to keep
// pair extraction off the hot indexing path.
//
// Two-phase drain architecture:
//   - Hot drain (every DrainInterval): Drain() returns raw pairs from the extraction buffer,
//     which are merged into epochPairs — integer-only operations, no Pebble I/O.
//   - Cold epoch (every EpochInterval): DrainEpoch() returns the accumulated epochPairs
//     for NPMI scoring with a DF snapshot. This keeps expensive I/O off the hot path.
//
// Allocation optimizations:
//   - Double-buffer swap: two pre-allocated buffers alternate on Drain() to avoid
//     re-allocating the backing array every drain cycle.
//   - In-place epoch merge: mergeInto grows epochPairs once and merges right-to-left,
//     eliminating per-drain intermediate slice allocations.
type cooccurrenceAccumulator struct {
	mu      sync.Mutex
	buffers [2][]cooccurrencePair
	active  int // index into buffers (0 or 1)
	cfg     CooccurrenceConfig

	// epochPairs accumulates merged pairs between cold epochs.
	// Written by hot drains (under epochMu), read+reset by cold epoch.
	epochMu    sync.Mutex
	epochPairs []cooccurrencePair

	maxBuffer  int
	docQueue   chan []DocumentBatch
	numWorkers int
	sem        chan struct{}
	wg         sync.WaitGroup
}

// distanceWeight computes the positional decay weight for a given distance.
func (a *cooccurrenceAccumulator) distanceWeight(dist int) float64 {
	d := float64(dist)
	switch a.cfg.DistanceDecay {
	case "linear":
		return 1.0 / d
	case "quadratic":
		return 1.0 / (d * d)
	default: // "log" or empty
		return 1.0 / math.Log2(1.0+d)
	}
}

// fieldAllowed returns true if the field should be included in co-occurrence extraction.
func (a *cooccurrenceAccumulator) fieldAllowed(field string) bool {
	if len(a.cfg.IncludeFields) == 0 {
		return true
	}
	for _, f := range a.cfg.IncludeFields {
		if f == field {
			return true
		}
	}
	return false
}

func newCooccurrenceAccumulator(cfg CooccurrenceConfig) *cooccurrenceAccumulator {
	if cfg.MaxDFThreshold <= 0 {
		cfg.MaxDFThreshold = 10000
	}
	if cfg.DrainInterval <= 0 {
		cfg.DrainInterval = 10 * time.Second
	}
	if cfg.MinPairCount <= 0 {
		cfg.MinPairCount = 3
	}

	numWorkers := cfg.MaxExtractWorkers
	if numWorkers <= 0 {
		numWorkers = 2
	}

	maxBuf := cfg.MaxBufferSize
	if maxBuf <= 0 {
		maxBuf = 200_000
	}

	a := &cooccurrenceAccumulator{
		cfg:        cfg,
		maxBuffer:  maxBuf,
		docQueue:   make(chan []DocumentBatch, 4),
		numWorkers: numWorkers,
		sem:        make(chan struct{}, numWorkers),
	}
	a.buffers[0] = make([]cooccurrencePair, 0, 4096)
	a.buffers[1] = make([]cooccurrencePair, 0, 4096)

	a.wg.Add(1)
	go a.extractWorker()

	return a
}

// EnqueueBatch sends a batch of documents for async co-occurrence extraction.
// Non-blocking: drops the batch if the queue is full (back-pressure).
func (a *cooccurrenceAccumulator) EnqueueBatch(docs []DocumentBatch) {
	select {
	case a.docQueue <- docs:
	default:
	}
}

// extractWorker consumes document batches from the queue and extracts
// co-occurrence pairs in parallel using a semaphore-bounded worker pool.
func (a *cooccurrenceAccumulator) extractWorker() {
	defer a.wg.Done()
	for docs := range a.docQueue {
		var batchWg sync.WaitGroup
		for _, doc := range docs {
			for field, terms := range doc.FieldTerms {
				if len(terms) < 2 {
					continue
				}
				if !a.fieldAllowed(field) {
					continue
				}
				batchWg.Add(1)
				field, terms := field, terms
				a.sem <- struct{}{}
				go func() {
					defer batchWg.Done()
					defer func() { <-a.sem }()
					a.ExtractFromDocument(field, terms)
				}()
			}
		}
		batchWg.Wait()
	}
}

// Close stops the extract worker and waits for all pending work to finish.
func (a *cooccurrenceAccumulator) Close() {
	close(a.docQueue)
	a.wg.Wait()
}

// ExtractFromDocument extracts co-occurrence pairs from a document's terms using
// sentence-based windowing: pairs are only extracted between terms in the same sentence,
// with a sliding window cap of WindowSize positions within each sentence.
//
// Algorithm:
//  1. Flatten all (position, termIndex, sentenceID) entries from every term.
//  2. Sort by (sentenceID, position).
//  3. Group by sentenceID, then slide a window within each sentence group.
//  4. Weight each pair by positional decay: w = 1/log₂(1 + distance).
func (a *cooccurrenceAccumulator) ExtractFromDocument(field string, terms []TermPosting) {
	if len(terms) < 2 {
		return
	}

	// Pre-build "field\x00term" strings once — O(n).
	ptPtr := prefixedTermsPool.Get().(*[]string)
	prefixed := (*ptPtr)[:0]
	for _, tp := range terms {
		prefixed = append(prefixed, field+"\x00"+tp.Term)
	}

	// Flatten all positions into a (pos, termIdx, sentenceID) array.
	pePtr := posEntriesPool.Get().(*[]posEntry)
	entries := (*pePtr)[:0]
	for idx, tp := range terms {
		for pi, pos := range tp.Positions {
			sid := 0
			if pi < len(tp.SentenceIDs) {
				sid = tp.SentenceIDs[pi]
			}
			entries = append(entries, posEntry{pos: pos, termIdx: idx, sentenceID: sid})
		}
	}

	// Sort by (sentenceID, position).
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].sentenceID != entries[j].sentenceID {
			return entries[i].sentenceID < entries[j].sentenceID
		}
		return entries[i].pos < entries[j].pos
	})

	// Compressed position gap mode: renumber positions 0,1,2,... within each sentence.
	if a.cfg.PositionGapMode == "compressed" {
		seq := 0
		prevSid := -1
		for k := range entries {
			if entries[k].sentenceID != prevSid {
				seq = 0
				prevSid = entries[k].sentenceID
			}
			entries[k].pos = seq
			seq++
		}
	}

	// Pair extraction within each sentence group.
	lpPtr := localPairsPool.Get().(*[]cooccurrencePair)
	local := (*lpPtr)[:0]
	maxWindow := a.cfg.WindowSize // 0 = no window limit (sentence boundary only)

	sentStart := 0
	for sentStart < len(entries) {
		sid := entries[sentStart].sentenceID
		sentEnd := sentStart
		for sentEnd < len(entries) && entries[sentEnd].sentenceID == sid {
			sentEnd++
		}

		for i := sentStart; i < sentEnd; i++ {
			for j := i + 1; j < sentEnd; j++ {
				dist := entries[j].pos - entries[i].pos
				if maxWindow > 0 && dist > maxWindow {
					break // sorted by position, so all further j are farther
				}
				ti, tj := entries[i].termIdx, entries[j].termIdx
				if ti == tj {
					continue // same term, skip
				}

				termA, termB := prefixed[ti], prefixed[tj]
				if termA > termB {
					termA, termB = termB, termA
				}

				w := a.distanceWeight(dist)

				local = append(local, cooccurrencePair{
					TermA:       termA,
					TermB:       termB,
					Count:       1,
					WeightedSum: w,
				})
			}
		}

		sentStart = sentEnd
	}

	// Return pooled slices
	*pePtr = entries
	posEntriesPool.Put(pePtr)
	*ptPtr = prefixed
	prefixedTermsPool.Put(ptPtr)

	if len(local) > 0 {
		a.mu.Lock()
		remaining := a.maxBuffer - len(a.buffers[a.active])
		if remaining > 0 {
			if len(local) > remaining {
				local = local[:remaining]
			}
			a.buffers[a.active] = append(a.buffers[a.active], local...)
		}
		a.mu.Unlock()
	}

	*lpPtr = local
	localPairsPool.Put(lpPtr)
}

// Drain returns accumulated pairs sorted and merged by key, then resets the buffer.
// Uses double-buffer swap to avoid re-allocating the backing array each drain.
func (a *cooccurrenceAccumulator) Drain() []cooccurrencePair {
	a.mu.Lock()
	buf := a.buffers[a.active]
	if len(buf) == 0 {
		a.mu.Unlock()
		return nil
	}
	// Swap to the inactive buffer (already allocated from a previous drain)
	a.active ^= 1
	a.buffers[a.active] = a.buffers[a.active][:0]
	a.mu.Unlock()

	// Sort by (TermA, TermB)
	sort.Slice(buf, func(i, j int) bool {
		return pairLess(&buf[i], &buf[j])
	})

	// Merge consecutive duplicates in-place
	w := 0
	for r := 1; r < len(buf); r++ {
		if buf[r].TermA == buf[w].TermA && buf[r].TermB == buf[w].TermB {
			buf[w].Count += buf[r].Count
			buf[w].WeightedSum += buf[r].WeightedSum
		} else {
			w++
			buf[w] = buf[r]
		}
	}
	return buf[:w+1]
}

// MergeHotDrain merges a sorted+deduped drain batch into the epoch accumulator.
// Called by the hot drain worker — only integer operations, no I/O.
// Uses in-place merge to avoid allocating a new slice every drain cycle.
func (a *cooccurrenceAccumulator) MergeHotDrain(drained []cooccurrencePair) {
	if len(drained) == 0 {
		return
	}

	a.epochMu.Lock()
	defer a.epochMu.Unlock()

	if len(a.epochPairs) == 0 {
		// First drain in this epoch — just take ownership.
		a.epochPairs = append(a.epochPairs[:0], drained...)
		return
	}

	// Merge in-place: grows epochPairs capacity once, merges right-to-left.
	mergeInto(&a.epochPairs, drained)
}

// DrainEpoch returns all accumulated epoch pairs and resets the epoch buffer.
// Called by the cold epoch worker for NPMI scoring.
func (a *cooccurrenceAccumulator) DrainEpoch() []cooccurrencePair {
	a.epochMu.Lock()
	if len(a.epochPairs) == 0 {
		a.epochMu.Unlock()
		return nil
	}
	pairs := a.epochPairs
	a.epochPairs = make([]cooccurrencePair, 0, cap(pairs)/2)
	a.epochMu.Unlock()
	return pairs
}

// mergeInto merges src into *dst in-place, combining duplicates.
// Both dst and src must be sorted by (TermA, TermB).
// Grows dst's backing array only when capacity is insufficient, then
// merges right-to-left to avoid a temporary allocation.
func mergeInto(dst *[]cooccurrencePair, src []cooccurrencePair) {
	a := *dst
	b := src
	if len(b) == 0 {
		return
	}

	aLen := len(a)
	needed := aLen + len(b)

	if cap(a) < needed {
		// Single growth with 25% headroom — amortized across epoch
		newCap := needed + needed/4
		grown := make([]cooccurrencePair, needed, newCap)
		start := mergeRightToLeft(grown, a, b)
		if start > 0 {
			copy(grown, grown[start:needed])
		}
		*dst = grown[:needed-start]
		return
	}

	// Enough capacity — merge right-to-left within existing backing array
	merged := a[:needed]
	start := mergeRightToLeft(merged, a[:aLen], b)
	if start > 0 {
		copy(merged, merged[start:needed])
	}
	*dst = merged[:needed-start]
}

// mergeRightToLeft merges two sorted pair slices into result, writing from the end.
// result must have len >= len(a)+len(b). Combines duplicate (TermA, TermB) entries.
// When a and result share backing storage, the right-to-left write order ensures
// source elements are read before being overwritten.
// Returns the start index of valid data in result (> 0 if duplicates were merged).
func mergeRightToLeft(result, a, b []cooccurrencePair) int {
	i := len(a) - 1
	j := len(b) - 1
	w := len(a) + len(b) - 1

	for i >= 0 && j >= 0 {
		if pairLess(&b[j], &a[i]) {
			result[w] = a[i]
			i--
		} else if pairLess(&a[i], &b[j]) {
			result[w] = b[j]
			j--
		} else {
			// Same key — merge counts, consume both
			result[w] = a[i]
			result[w].Count += b[j].Count
			result[w].WeightedSum += b[j].WeightedSum
			i--
			j--
		}
		w--
	}

	for ; i >= 0; i-- {
		result[w] = a[i]
		w--
	}
	for ; j >= 0; j-- {
		result[w] = b[j]
		w--
	}

	return w + 1
}

// computeNPMIWeight computes the NPMI-based edge weight with positional decay boost.
//
//	NPMI = log(P(A,B) / (P(A) × P(B))) / -log(P(A,B))
//	avgWeight = mean of 1/log₂(1+dist) across observations (~1.0 for adjacent, ~0.15 for dist=15)
//	weight = clamp(NPMI × (1.0 + avgWeight), 0, 1)
func computeNPMIWeight(cooccCount, dfA, dfB, totalDocs int64, avgWeight float64) float64 {
	if totalDocs <= 0 || dfA <= 0 || dfB <= 0 || cooccCount <= 0 {
		return 0
	}

	n := float64(totalDocs)
	pA := float64(dfA) / n
	pB := float64(dfB) / n
	pAB := float64(cooccCount) / n

	if pAB <= 0 || pA <= 0 || pB <= 0 {
		return 0
	}

	pmi := math.Log(pAB / (pA * pB))
	negLogPAB := -math.Log(pAB)

	if negLogPAB == 0 {
		return 0
	}

	npmi := pmi / negLogPAB

	weight := npmi * (1.0 + avgWeight)

	if weight < 0 {
		return 0
	}
	if weight > 1 {
		return 1
	}
	return weight
}

// computeLLRWeight computes a Log-Likelihood Ratio edge weight with positional decay boost.
// Uses the G² statistic from a 2×2 contingency table, normalized to [0,1].
func computeLLRWeight(cooccCount, dfA, dfB, totalDocs int64, avgWeight float64) float64 {
	if totalDocs <= 0 || dfA <= 0 || dfB <= 0 || cooccCount <= 0 {
		return 0
	}

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
	llr *= 2

	// Normalize: divide by theoretical max (2*N*ln2) and boost with avgWeight.
	norm := 2 * n * math.Ln2
	if norm <= 0 {
		return 0
	}

	weight := (llr / norm) * (1.0 + avgWeight)
	if weight < 0 {
		return 0
	}
	if weight > 1 {
		return 1
	}
	return weight
}

// computeDiceWeight computes a Dice coefficient edge weight with positional decay boost.
// Dice = 2 * count(A∩B) / (count(A) + count(B)), normalized to [0,1].
func computeDiceWeight(cooccCount, dfA, dfB, totalDocs int64, avgWeight float64) float64 {
	if dfA <= 0 || dfB <= 0 || cooccCount <= 0 {
		return 0
	}

	dice := 2.0 * float64(cooccCount) / (float64(dfA) + float64(dfB))
	weight := dice * (1.0 + avgWeight)

	if weight < 0 {
		return 0
	}
	if weight > 1 {
		return 1
	}
	return weight
}
