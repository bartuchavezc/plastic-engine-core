package segment

import (
	"sort"
	"sync"
	"time"
)

// MemSegment is an in-memory segment that receives writes.
// It is the "active" segment that accumulates documents before being flushed to disk.
type MemSegment struct {
	mu sync.RWMutex

	id       string
	postings map[string]*memPostingList // term_id -> posting list
	docCount int
	termDF   map[string]int64 // term_id -> local DF (docs in this segment)
	docs     map[string]bool  // doc_id -> exists (for counting unique docs)

	createdAt time.Time
	minDocID  string
	maxDocID  string
}

// memPostingList holds postings for a single term in memory.
type memPostingList struct {
	termID   string
	postings []Posting
	// Removed docSet map - use linear search instead (postings are usually small per term)
}

// NewMemSegment creates a new in-memory segment.
func NewMemSegment(id string) *MemSegment {
	return &MemSegment{
		id:        id,
		postings:  make(map[string]*memPostingList),
		termDF:    make(map[string]int64),
		docs:      make(map[string]bool),
		createdAt: time.Now().UTC(),
	}
}

// ID returns the segment identifier.
func (s *MemSegment) ID() string {
	return s.id
}

// Meta returns segment metadata.
func (s *MemSegment) Meta() SegmentMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return SegmentMeta{
		ID:        s.id,
		Version:   1,
		DocCount:  s.docCount,
		TermCount: len(s.postings),
		CreatedAt: s.createdAt,
		MinDocID:  s.minDocID,
		MaxDocID:  s.maxDocID,
		Level:     0, // Memory segment is always level 0
	}
}

// Add adds a posting to the segment.
func (s *MemSegment) Add(termID, docID string, tf int, positions []int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get or create posting list for this term
	pl, ok := s.postings[termID]
	if !ok {
		pl = &memPostingList{
			termID:   termID,
			postings: make([]Posting, 0, 8),
		}
		s.postings[termID] = pl
	}

	// Check if this doc already has a posting for this term (linear search - usually small)
	found := false
	for i := range pl.postings {
		if pl.postings[i].DocID == docID {
			// Update existing posting
			pl.postings[i].TF = tf
			pl.postings[i].Positions = positions
			found = true
			break
		}
	}

	if !found {
		// Add new posting
		pl.postings = append(pl.postings, Posting{
			DocID:     docID,
			TF:        tf,
			Positions: positions,
		})
		s.termDF[termID]++
	}

	// Track document
	if !s.docs[docID] {
		s.docs[docID] = true
		s.docCount++

		// Update min/max
		if s.minDocID == "" || docID < s.minDocID {
			s.minDocID = docID
		}
		if docID > s.maxDocID {
			s.maxDocID = docID
		}
	}
}

// AddPosting adds a posting struct directly.
func (s *MemSegment) AddPosting(termID string, posting Posting) {
	s.Add(termID, posting.DocID, posting.TF, posting.Positions)
}

// Search finds all postings for a term.
func (s *MemSegment) Search(termID string) []Hit {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pl, ok := s.postings[termID]
	if !ok {
		return nil
	}

	hits := make([]Hit, len(pl.postings))
	for i, p := range pl.postings {
		hits[i] = Hit{
			DocID:     p.DocID,
			TermID:    termID,
			TF:        p.TF,
			Positions: p.Positions,
			SegmentID: s.id,
		}
	}

	return hits
}

// GetLocalDF returns the document frequency for a term in this segment.
func (s *MemSegment) GetLocalDF(termID string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.termDF[termID]
}

// DocCount returns the number of documents in this segment.
func (s *MemSegment) DocCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.docCount
}

// TermCount returns the number of unique terms.
func (s *MemSegment) TermCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.postings)
}

// Close releases resources (no-op for memory segment).
func (s *MemSegment) Close() error {
	return nil
}

// Iterator returns an iterator over all terms in sorted order.
// NOTE: The caller must ensure the MemSegment is not modified during iteration.
// This is safe in the flush path because the segment is swapped atomically before iteration.
func (s *MemSegment) Iterator() SegmentIterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect and sort term IDs
	termIDs := make([]string, 0, len(s.postings))
	for termID := range s.postings {
		termIDs = append(termIDs, termID)
	}
	sort.Strings(termIDs)

	// OPTIMIZATION: No deep copy needed - the MemSegment is swapped before iteration
	// so it won't be modified. Just keep references to the posting lists.
	postingsRef := make(map[string]*memPostingList, len(s.postings))
	for termID, pl := range s.postings {
		postingsRef[termID] = pl
	}

	dfSnapshot := make(map[string]int64, len(s.termDF))
	for k, v := range s.termDF {
		dfSnapshot[k] = v
	}

	return &memSegmentIterator{
		termIDs:     termIDs,
		postingsRef: postingsRef,
		df:          dfSnapshot,
		pos:         -1,
	}
}

// memSegmentIterator iterates over a memory segment.
type memSegmentIterator struct {
	termIDs     []string
	postingsRef map[string]*memPostingList // References, not copies
	df          map[string]int64
	pos         int
}

func (it *memSegmentIterator) Next() bool {
	it.pos++
	return it.pos < len(it.termIDs)
}

func (it *memSegmentIterator) Term() string {
	if it.pos < 0 || it.pos >= len(it.termIDs) {
		return ""
	}
	return it.termIDs[it.pos]
}

func (it *memSegmentIterator) Postings() []Posting {
	if it.pos < 0 || it.pos >= len(it.termIDs) {
		return nil
	}
	termID := it.termIDs[it.pos]
	if pl, ok := it.postingsRef[termID]; ok {
		return pl.postings
	}
	return nil
}

func (it *memSegmentIterator) DF() int64 {
	if it.pos < 0 || it.pos >= len(it.termIDs) {
		return 0
	}
	return it.df[it.termIDs[it.pos]]
}

func (it *memSegmentIterator) Close() error {
	return nil
}

// Clear resets the segment to empty state.
func (s *MemSegment) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.postings = make(map[string]*memPostingList)
	s.termDF = make(map[string]int64)
	s.docs = make(map[string]bool)
	s.docCount = 0
	s.minDocID = ""
	s.maxDocID = ""
}

// HasDoc returns true if the document exists in this segment.
func (s *MemSegment) HasDoc(docID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.docs[docID]
}

// GetPostingsForDoc returns all postings for a document.
func (s *MemSegment) GetPostingsForDoc(docID string) []PostingList {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []PostingList
	for termID, pl := range s.postings {
		for _, p := range pl.postings {
			if p.DocID == docID {
				result = append(result, PostingList{
					TermID:   termID,
					Postings: []Posting{p},
				})
				break
			}
		}
	}
	return result
}
