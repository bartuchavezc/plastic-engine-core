package segment

import (
	"container/heap"
	"fmt"
	"sort"
	"time"
)

// MergePolicy determines which segments to merge.
type MergePolicy interface {
	// FindMergeCandidates returns segments that should be merged.
	FindMergeCandidates(segments []*DiskSegment) []MergeCandidate
}

// TieredMergePolicy groups segments by size tiers and merges within tiers.
type TieredMergePolicy struct {
	// MaxSegmentsPerTier is the max segments before triggering merge.
	MaxSegmentsPerTier int

	// TierMultiplier is how much larger each tier is.
	TierMultiplier int

	// MinMergeSize is the minimum segment size to consider for merging.
	MinMergeSize int

	// MaxMergeSize is the maximum segment size to produce.
	MaxMergeSize int
}

// DefaultMergePolicy returns a sensible default merge policy.
func DefaultMergePolicy() *TieredMergePolicy {
	return &TieredMergePolicy{
		MaxSegmentsPerTier: 5,
		TierMultiplier:     10,
		MinMergeSize:       100,
		MaxMergeSize:       1000000,
	}
}

// FindMergeCandidates finds segments that should be merged.
func (p *TieredMergePolicy) FindMergeCandidates(segments []*DiskSegment) []MergeCandidate {
	if len(segments) < 2 {
		return nil
	}

	// Group by level
	levels := make(map[int][]*DiskSegment)
	for _, seg := range segments {
		level := seg.Meta().Level
		levels[level] = append(levels[level], seg)
	}

	var candidates []MergeCandidate

	// Check each level
	for level, segs := range levels {
		if len(segs) >= p.MaxSegmentsPerTier {
			// Sort by creation time (oldest first)
			sort.Slice(segs, func(i, j int) bool {
				return segs[i].Meta().CreatedAt.Before(segs[j].Meta().CreatedAt)
			})

			// Take the oldest segments up to max per tier
			toMerge := segs
			if len(toMerge) > p.MaxSegmentsPerTier {
				toMerge = toMerge[:p.MaxSegmentsPerTier]
			}

			candidates = append(candidates, MergeCandidate{
				Segments: toMerge,
				Level:    level,
			})
		}
	}

	return candidates
}

// Merger handles the actual merging of segments.
type Merger struct {
	policy MergePolicy
	config Config
}

// NewMerger creates a new merger.
func NewMerger(policy MergePolicy, config Config) *Merger {
	if policy == nil {
		policy = DefaultMergePolicy()
	}
	return &Merger{
		policy: policy,
		config: config,
	}
}

// termHeapItem is used in k-way merge.
type termHeapItem struct {
	termID   string
	postings []Posting
	df       int64
	iterIdx  int
}

// termHeap implements heap.Interface for k-way merge.
type termHeap []termHeapItem

func (h termHeap) Len() int           { return len(h) }
func (h termHeap) Less(i, j int) bool { return h[i].termID < h[j].termID }
func (h termHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *termHeap) Push(x interface{}) {
	*h = append(*h, x.(termHeapItem))
}

func (h *termHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeSegments performs a k-way merge of segments.
func (m *Merger) MergeSegments(segments []*DiskSegment, outputDir string, newLevel int) (*DiskSegment, error) {
	if len(segments) == 0 {
		return nil, nil
	}

	if len(segments) == 1 {
		// Nothing to merge
		return segments[0], nil
	}

	newID := generateSegmentID()
	writer, err := NewDiskSegmentWriter(outputDir, newID)
	if err != nil {
		return nil, err
	}

	// Create iterators
	iters := make([]SegmentIterator, len(segments))
	for i, seg := range segments {
		iters[i] = seg.Iterator()
	}
	defer func() {
		for _, iter := range iters {
			if iter != nil {
				iter.Close()
			}
		}
	}()

	// Initialize heap with first term from each iterator
	h := &termHeap{}
	heap.Init(h)

	for i, iter := range iters {
		if iter.Next() {
			heap.Push(h, termHeapItem{
				termID:   iter.Term(),
				postings: iter.Postings(),
				df:       iter.DF(),
				iterIdx:  i,
			})
		}
	}

	// K-way merge
	var currentTermID string
	var mergedPostings []Posting
	var mergedDF int64
	totalDocs := 0

	flushTerm := func() error {
		if currentTermID == "" || len(mergedPostings) == 0 {
			return nil
		}

		// Deduplicate by docID
		postingMap := make(map[string]Posting)
		for _, p := range mergedPostings {
			postingMap[p.DocID] = p
		}

		deduped := make([]Posting, 0, len(postingMap))
		for _, p := range postingMap {
			deduped = append(deduped, p)
		}

		return writer.AddTerm(currentTermID, deduped, mergedDF)
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(termHeapItem)

		if item.termID != currentTermID {
			// Flush previous term
			if err := flushTerm(); err != nil {
				writer.Abort()
				return nil, err
			}

			currentTermID = item.termID
			mergedPostings = nil
			mergedDF = 0
		}

		// Merge postings
		mergedPostings = append(mergedPostings, item.postings...)
		mergedDF += item.df

		// Advance iterator
		iter := iters[item.iterIdx]
		if iter.Next() {
			heap.Push(h, termHeapItem{
				termID:   iter.Term(),
				postings: iter.Postings(),
				df:       iter.DF(),
				iterIdx:  item.iterIdx,
			})
		}
	}

	// Flush last term
	if err := flushTerm(); err != nil {
		writer.Abort()
		return nil, err
	}

	// Calculate total docs
	for _, seg := range segments {
		totalDocs += seg.DocCount()
	}

	// Set metadata
	writer.SetMeta(SegmentMeta{
		ID:       newID,
		DocCount: totalDocs,
		Level:    newLevel,
	})

	// Finalize
	path, err := writer.Finalize()
	if err != nil {
		return nil, err
	}

	return OpenDiskSegment(path)
}

// MergePostingLists merges multiple posting lists for the same term.
func MergePostingLists(lists [][]Posting) []Posting {
	if len(lists) == 0 {
		return nil
	}

	if len(lists) == 1 {
		return lists[0]
	}

	// Collect all postings
	total := 0
	for _, list := range lists {
		total += len(list)
	}

	merged := make([]Posting, 0, total)
	for _, list := range lists {
		merged = append(merged, list...)
	}

	// Sort by docID
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].DocID < merged[j].DocID
	})

	// Deduplicate (keep last occurrence for updates)
	if len(merged) == 0 {
		return merged
	}

	deduped := make([]Posting, 0, len(merged))
	deduped = append(deduped, merged[0])

	for i := 1; i < len(merged); i++ {
		if merged[i].DocID != deduped[len(deduped)-1].DocID {
			deduped = append(deduped, merged[i])
		} else {
			// Replace with newer posting
			deduped[len(deduped)-1] = merged[i]
		}
	}

	return deduped
}

// generateSegmentID creates a unique segment ID.
func generateSegmentID() string {
	return fmt.Sprintf("seg_%d", time.Now().UnixNano())
}
