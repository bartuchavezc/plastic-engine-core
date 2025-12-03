package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	coreindex "plastic-engine-core/internal/core/index"
	"plastic-engine-core/internal/core/storage/pebble"
)

// ShardStore abstracts the persistence of indexing data inside a shard.
type ShardStore interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
}

// DocumentWriteRequest represents the terms generated for a document.
type DocumentWriteRequest struct {
	DocumentID string
	Fields     []FieldTerms
}

// FieldTerms groups the tokens produced for a field.
type FieldTerms struct {
	Field  coreindex.FieldMapping
	Tokens []Token
}

// IndexWriter persists document postings and forward indexes.
type IndexWriter struct {
	store ShardStore
}

// NewIndexWriter builds an IndexWriter for the given shard store.
func NewIndexWriter(store ShardStore) *IndexWriter {
	return &IndexWriter{store: store}
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

	if err := w.applyDiff(prevState, newState, req.DocumentID); err != nil {
		return err
	}

	if err := w.saveForward(req.DocumentID, newState); err != nil {
		return err
	}

	return nil
}

func (w *IndexWriter) loadForward(documentID string) (forwardDocument, error) {
	key := forwardKey(documentID)
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

	// TODO batching: group forward + postings writes into a single Pebble batch.
	if err := w.store.Set(forwardKey(documentID), string(payload)); err != nil {
		return fmt.Errorf("persist forward index: %w", err)
	}

	return nil
}

func (w *IndexWriter) applyDiff(prev, next forwardDocument, documentID string) error {
	prevSet := prev.asTermSet()
	nextSet := next.asTermSet()

	for field, terms := range prevSet {
		for term := range terms {
			if _, ok := nextSet[field][term]; !ok {
				if err := w.store.Delete(invertedKey(field, term, documentID)); err != nil && !errors.Is(err, pebble.ErrNotFound) {
					return fmt.Errorf("delete inverted entry %s/%s: %w", field, term, err)
				}
			}
		}
	}

	for field, terms := range nextSet {
		for term := range terms {
			if _, existed := prevSet[field][term]; !existed {
				if err := w.store.Set(invertedKey(field, term, documentID), documentID); err != nil {
					return fmt.Errorf("set inverted entry %s/%s: %w", field, term, err)
				}
			}
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

		unique := make(map[string]Token)
		for _, token := range field.Tokens {
			if stringsTrim(token.Term) == "" {
				continue
			}
			if _, exists := unique[token.Term]; !exists {
				unique[token.Term] = token
			}
		}

		if len(unique) == 0 {
			continue
		}

		target.Tokens = make([]forwardToken, 0, len(unique))
		for term, token := range unique {
			target.Tokens = append(target.Tokens, forwardToken{
				Term:     term,
				Position: token.Position,
			})
		}

		sort.Slice(target.Tokens, func(i, j int) bool {
			if target.Tokens[i].Position == target.Tokens[j].Position {
				return target.Tokens[i].Term < target.Tokens[j].Term
			}
			return target.Tokens[i].Position < target.Tokens[j].Position
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
	Term     string `json:"term"`
	Position int    `json:"position,omitempty"`
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

func (f forwardDocument) asTermSet() map[string]map[string]struct{} {
	set := make(map[string]map[string]struct{}, len(f.Fields))
	for field, data := range f.Fields {
		terms := make(map[string]struct{}, len(data.Tokens))
		for _, token := range data.Tokens {
			if stringsTrim(token.Term) == "" {
				continue
			}
			terms[token.Term] = struct{}{}
		}
		set[field] = terms
	}
	return set
}

func forwardKey(docID string) string {
	return "fwd:" + docID
}

func invertedKey(field, term, docID string) string {
	return "inv:" + field + ":" + term + ":" + docID
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}
