package document

import (
	"context"
	"fmt"
	"strings"

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
	if strings.TrimSpace(req.DocumentID) == "" {
		return fmt.Errorf("document id is required")
	}

	// Convert to segment.Manager format
	fieldTerms := make(map[string][]segment.TermPosting)

	for _, field := range req.Fields {
		name := field.Field.Name
		if strings.TrimSpace(name) == "" {
			continue
		}

		// Group tokens by term to calculate TF and positions
		termData := make(map[string]*segment.TermPosting)

		for _, token := range field.Tokens {
			if strings.TrimSpace(token.Term) == "" {
				continue
			}

			tp, exists := termData[token.Term]
			if !exists {
				tp = &segment.TermPosting{
					Term:      token.Term,
					TF:        0,
					Positions: make([]int, 0),
				}
				termData[token.Term] = tp
			}
			tp.TF++
			tp.Positions = append(tp.Positions, token.Position)
		}

		// Convert to slice
		postings := make([]segment.TermPosting, 0, len(termData))
		for _, tp := range termData {
			postings = append(postings, *tp)
		}

		if len(postings) > 0 {
			fieldTerms[name] = postings
		}
	}

	if len(fieldTerms) == 0 {
		return nil
	}

	// Index the document using segment manager
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
