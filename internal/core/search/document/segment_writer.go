package document

import (
	"context"
	"fmt"

	"plastic-engine-core/internal/core/search/segment"
	"plastic-engine-core/internal/pkg/logger"
)

// SegmentIndexWriter writes document postings using the segment-based storage.
type SegmentIndexWriter struct {
	segmentMgr *segment.Manager
	log        logger.Logger
}

// NewSegmentIndexWriter creates a new SegmentIndexWriter.
func NewSegmentIndexWriter(segmentMgr *segment.Manager, log logger.Logger) *SegmentIndexWriter {
	if log == nil {
		log = logger.DefaultLogger()
	}
	return &SegmentIndexWriter{
		segmentMgr: segmentMgr,
		log:        log,
	}
}

// Index stores the tokens for a document.
func (w *SegmentIndexWriter) Index(ctx context.Context, req DocumentWriteRequest) error {
	if req.DocumentID == "" {
		return fmt.Errorf("document id is required")
	}

	// Convert to segment.Manager format
	fieldTerms := make(map[string][]segment.TermPosting, len(req.Fields))

	for _, field := range req.Fields {
		name := field.Field.Name
		if name == "" {
			continue
		}

		tokens := field.Tokens
		if len(tokens) == 0 {
			continue
		}

		// Group tokens by term using index into slice (avoids pointer allocation)
		termIndex := make(map[string]int, len(tokens)/2) // estimate unique terms
		postings := make([]segment.TermPosting, 0, len(tokens)/2)

		for _, token := range tokens {
			if token.Term == "" {
				continue
			}

			if idx, exists := termIndex[token.Term]; exists {
				// Term already seen - update in place
				postings[idx].TF++
				postings[idx].Positions = append(postings[idx].Positions, token.Position)
			} else {
				// New term - add to slice and index
				termIndex[token.Term] = len(postings)
				postings = append(postings, segment.TermPosting{
					Term:      token.Term,
					TF:        1,
					Positions: []int{token.Position}, // pre-allocate with first position
				})
			}
		}

		if len(postings) > 0 {
			fieldTerms[name] = postings
		}
	}

	if len(fieldTerms) == 0 {
		return nil
	}

	return w.segmentMgr.IndexDocument(ctx, req.DocumentID, fieldTerms)
}

// IndexBatch indexes multiple documents.
func (w *SegmentIndexWriter) IndexBatch(ctx context.Context, requests []DocumentWriteRequest) error {
	if len(requests) == 0 {
		return nil
	}

	w.log.Debug("IndexBatch: starting",
		logger.Field{Key: "requests_count", Value: len(requests)},
	)

	for i, req := range requests {
		if err := w.Index(ctx, req); err != nil {
			w.log.Error("IndexBatch: failed to index document",
				logger.Field{Key: "document_id", Value: req.DocumentID},
				logger.Field{Key: "index", Value: i},
				logger.Field{Key: "error", Value: err},
			)
			return fmt.Errorf("indexing document %s: %w", req.DocumentID, err)
		}
	}

	w.log.Info("IndexBatch: completed successfully",
		logger.Field{Key: "documents_count", Value: len(requests)},
	)

	return nil
}

// Delete removes a document from the index.
// Note: With segment-based storage, deletes are handled by marking documents as deleted
// and cleaning up during segment merges.
func (w *SegmentIndexWriter) Delete(ctx context.Context, documentID string) error {
	// TODO: Implement delete by adding to delete set in segment manager
	// For now, deletes will be handled during segment merge
	w.log.Warn("Delete: not fully implemented yet, document will be cleaned on merge",
		logger.Field{Key: "document_id", Value: documentID},
	)
	return nil
}

// SegmentManager returns the underlying segment manager.
func (w *SegmentIndexWriter) SegmentManager() *segment.Manager {
	return w.segmentMgr
}
