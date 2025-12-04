package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	indexes "plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/adapters/storage/pebble"
)

// ShardStore abstracts the persistence of indexing data inside a shard.
type ShardStore interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
	GetInt64(key string) (int64, error)
	SetInt64(key string, value int64) error
	GetFloat64(key string) (float64, error)
	SetFloat64(key string, value float64) error
	Increment(key string, delta int64) (int64, error)
}

// DocumentWriteRequest represents the terms generated for a document.
type DocumentWriteRequest struct {
	DocumentID  string
	Fields      []FieldTerms
	NgramConfig NgramConfig
}

// FieldTerms groups the tokens produced for a field.
type FieldTerms struct {
	Field  indexes.FieldMapping
	Tokens []Token
}

// Posting stores the posting data for a term in a document.
type Posting struct {
	TF        int   `json:"tf"`                  // Term frequency
	Positions []int `json:"positions,omitempty"` // Token positions
}

// IndexWriter persists document postings and forward indexes using term registry.
type IndexWriter struct {
	store        ShardStore
	termRegistry *TermRegistry
}

// NewIndexWriter builds an IndexWriter for the given shard store.
func NewIndexWriter(store ShardStore) *IndexWriter {
	return &IndexWriter{
		store:        store,
		termRegistry: NewTermRegistry(store),
	}
}

// Index stores the tokens for a document, updating inverted indexes incrementally.
func (w *IndexWriter) Index(ctx context.Context, req DocumentWriteRequest) error {
	if stringsTrim(req.DocumentID) == "" {
		return fmt.Errorf("document id is required")
	}

	newState := buildForwardState(req)

	prevState, err := w.loadForward(req.DocumentID)
	if err != nil {
		return err
	}

	isNewDoc := len(prevState.Fields) == 0

	if err := w.applyDiff(ctx, prevState, newState, req.DocumentID, req.NgramConfig); err != nil {
		return err
	}

	if err := w.saveForward(req.DocumentID, newState); err != nil {
		return err
	}

	// Update metadata
	if isNewDoc {
		if _, err := w.store.Increment(pebble.DocCountKey(), 1); err != nil {
			return fmt.Errorf("increment doc count: %w", err)
		}
	}

	// Update average document length per field
	for fieldName, fieldData := range newState.Fields {
		termCount := int64(len(fieldData.Tokens))
		if err := w.updateAvgDocLen(fieldName, termCount, isNewDoc); err != nil {
			return fmt.Errorf("update avg doc len for %s: %w", fieldName, err)
		}
	}

	return nil
}

// Delete removes a document from the index.
func (w *IndexWriter) Delete(ctx context.Context, documentID string, ngramConfig NgramConfig) error {
	if stringsTrim(documentID) == "" {
		return fmt.Errorf("document id is required")
	}

	prevState, err := w.loadForward(documentID)
	if err != nil {
		return err
	}

	if len(prevState.Fields) == 0 {
		return nil // Document doesn't exist
	}

	// Remove all postings
	emptyState := newForwardDocument()
	if err := w.applyDiff(ctx, prevState, emptyState, documentID, ngramConfig); err != nil {
		return err
	}

	// Delete forward index
	if err := w.store.Delete(pebble.ForwardKey(documentID)); err != nil && !pebble.IsNotFound(err) {
		return fmt.Errorf("delete forward index: %w", err)
	}

	// Decrement doc count
	if _, err := w.store.Increment(pebble.DocCountKey(), -1); err != nil {
		return fmt.Errorf("decrement doc count: %w", err)
	}

	return nil
}

func (w *IndexWriter) loadForward(documentID string) (forwardDocument, error) {
	key := pebble.ForwardKey(documentID)
	raw, err := w.store.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) || pebble.IsNotFound(err) {
			return newForwardDocument(), nil
		}
		return forwardDocument{}, fmt.Errorf("load forward index: %w", err)
	}

	var doc forwardDocument
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return forwardDocument{}, fmt.Errorf("decode forward index: %w", err)
	}

	doc.ensureMaps()
	return doc, nil
}

func (w *IndexWriter) saveForward(documentID string, doc forwardDocument) error {
	payload, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encode forward index: %w", err)
	}

	if err := w.store.Set(pebble.ForwardKey(documentID), string(payload)); err != nil {
		return fmt.Errorf("persist forward index: %w", err)
	}

	return nil
}

func (w *IndexWriter) applyDiff(ctx context.Context, prev, next forwardDocument, documentID string, ngramConfig NgramConfig) error {
	prevTerms := prev.asTermMap()
	nextTerms := next.asTermMap()

	// Remove terms that no longer exist
	for field, terms := range prevTerms {
		for term := range terms {
			if _, ok := nextTerms[field][term]; !ok {
				if err := w.removeTerm(ctx, field, term, documentID, ngramConfig); err != nil {
					return err
				}
			}
		}
	}

	// Add new terms
	for field, terms := range nextTerms {
		for term, posting := range terms {
			if _, existed := prevTerms[field][term]; !existed {
				if err := w.addTerm(ctx, field, term, documentID, posting, ngramConfig); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (w *IndexWriter) addTerm(ctx context.Context, field, term, documentID string, posting Posting, ngramConfig NgramConfig) error {
	// Get or create term in registry
	entry, err := w.termRegistry.GetOrCreate(ctx, field, term)
	if err != nil {
		return fmt.Errorf("get term entry: %w", err)
	}

	// Write posting
	postingData, err := json.Marshal(posting)
	if err != nil {
		return fmt.Errorf("encode posting: %w", err)
	}

	invKey := pebble.InvertedKey(entry.TermID, documentID)
	if err := w.store.Set(invKey, string(postingData)); err != nil {
		return fmt.Errorf("set inverted entry: %w", err)
	}

	// Increment document frequency
	if err := w.termRegistry.IncrementDF(ctx, field, term); err != nil {
		return fmt.Errorf("increment df: %w", err)
	}

	// Write edge n-grams
	ngrams := GenerateEdgeNgrams(term, ngramConfig)
	for _, ngram := range ngrams {
		ngramKey := pebble.NgramKey(field, ngram, entry.TermID)
		// Empty value - key-only existence check
		if err := w.store.Set(ngramKey, ""); err != nil {
			return fmt.Errorf("set ngram entry: %w", err)
		}
	}

	return nil
}

func (w *IndexWriter) removeTerm(ctx context.Context, field, term, documentID string, ngramConfig NgramConfig) error {
	// Get term from registry
	entry, found, err := w.termRegistry.Get(ctx, field, term)
	if err != nil {
		return fmt.Errorf("get term entry: %w", err)
	}
	if !found {
		return nil // Term doesn't exist, nothing to remove
	}

	// Delete posting
	invKey := pebble.InvertedKey(entry.TermID, documentID)
	if err := w.store.Delete(invKey); err != nil && !pebble.IsNotFound(err) {
		return fmt.Errorf("delete inverted entry: %w", err)
	}

	// Decrement document frequency
	if err := w.termRegistry.DecrementDF(ctx, field, term); err != nil {
		return fmt.Errorf("decrement df: %w", err)
	}

	// Note: We don't delete n-gram entries here because they may still be used by other documents.
	// N-gram cleanup could be done as a background maintenance task if needed.

	return nil
}

func (w *IndexWriter) updateAvgDocLen(field string, termCount int64, isNewDoc bool) error {
	// Get current totals
	totalTerms, err := w.store.GetInt64(pebble.TotalTermLenKey(field))
	if err != nil && !pebble.IsNotFound(err) {
		return err
	}

	fieldDocs, err := w.store.GetInt64(pebble.FieldDocsKey(field))
	if err != nil && !pebble.IsNotFound(err) {
		return err
	}

	// Update totals
	totalTerms += termCount
	if isNewDoc {
		fieldDocs++
	}

	// Persist
	if err := w.store.SetInt64(pebble.TotalTermLenKey(field), totalTerms); err != nil {
		return err
	}
	if err := w.store.SetInt64(pebble.FieldDocsKey(field), fieldDocs); err != nil {
		return err
	}

	// Calculate and store average
	if fieldDocs > 0 {
		avgLen := float64(totalTerms) / float64(fieldDocs)
		if err := w.store.SetFloat64(pebble.AvgDocLenKey(field), avgLen); err != nil {
			return err
		}
	}

	return nil
}

func buildForwardState(req DocumentWriteRequest) forwardDocument {
	doc := newForwardDocument()

	for _, field := range req.Fields {
		name := field.Field.Name
		if stringsTrim(name) == "" {
			continue
		}

		target := doc.Fields[name]

		// Group tokens by term to calculate TF and positions
		termData := make(map[string][]int) // term -> positions
		for _, token := range field.Tokens {
			if stringsTrim(token.Term) == "" {
				continue
			}
			termData[token.Term] = append(termData[token.Term], token.Position)
		}

		if len(termData) == 0 {
			continue
		}

		target.Tokens = make([]forwardToken, 0, len(termData))
		for term, positions := range termData {
			sort.Ints(positions)
			target.Tokens = append(target.Tokens, forwardToken{
				Term:      term,
				TF:        len(positions),
				Positions: positions,
			})
		}

		sort.Slice(target.Tokens, func(i, j int) bool {
			return target.Tokens[i].Term < target.Tokens[j].Term
		})

		doc.Fields[name] = target
	}

	return doc
}

type forwardDocument struct {
	Fields map[string]forwardField `json:"fields"`
}

type forwardField struct {
	Tokens []forwardToken `json:"tokens"`
}

type forwardToken struct {
	Term      string `json:"term"`
	TF        int    `json:"tf"`
	Positions []int  `json:"positions,omitempty"`
}

func newForwardDocument() forwardDocument {
	return forwardDocument{
		Fields: make(map[string]forwardField),
	}
}

func (f *forwardDocument) ensureMaps() {
	if f.Fields == nil {
		f.Fields = make(map[string]forwardField)
	}
}

// asTermMap returns a map of field -> term -> Posting for diff operations.
func (f forwardDocument) asTermMap() map[string]map[string]Posting {
	result := make(map[string]map[string]Posting, len(f.Fields))
	for field, data := range f.Fields {
		terms := make(map[string]Posting, len(data.Tokens))
		for _, token := range data.Tokens {
			if stringsTrim(token.Term) == "" {
				continue
			}
			terms[token.Term] = Posting{
				TF:        token.TF,
				Positions: token.Positions,
			}
		}
		result[field] = terms
	}
	return result
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}
