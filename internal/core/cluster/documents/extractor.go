package documents

import (
	"errors"

	"github.com/tidwall/gjson"
)

// Extractor uses gjson to extract routing fields from documents without full deserialization.
type Extractor struct{}

// NewExtractor creates a new document extractor.
func NewExtractor() *Extractor {
	return &Extractor{}
}

// ExtractedDoc contains the extracted routing information and raw payload bytes.
type ExtractedDoc struct {
	DocumentID string
	Routing    map[string]string
	RawPayload []byte // Raw bytes of the payload field, never parsed
}

// ExtractFromRequest extracts routing information from a raw document request.
// The document format expected is: {"document_id": "...", "routing": {...}, "payload": {...}}
func (e *Extractor) ExtractFromRequest(rawDoc []byte) (ExtractedDoc, error) {
	docID := gjson.GetBytes(rawDoc, "document_id").String()
	if docID == "" {
		return ExtractedDoc{}, errors.New("document_id is required")
	}

	var routing map[string]string
	routingResult := gjson.GetBytes(rawDoc, "routing")
	if routingResult.Exists() && routingResult.IsObject() {
		routing = make(map[string]string)
		routingResult.ForEach(func(key, value gjson.Result) bool {
			routing[key.String()] = value.String()
			return true
		})
	}

	// Extract payload as raw bytes without parsing
	var rawPayload []byte
	payloadResult := gjson.GetBytes(rawDoc, "payload")
	if payloadResult.Exists() {
		rawPayload = []byte(payloadResult.Raw)
	} else {
		rawPayload = []byte("{}")
	}

	return ExtractedDoc{
		DocumentID: docID,
		Routing:    routing,
		RawPayload: rawPayload,
	}, nil
}

// ExtractFieldForRouting extracts a specific field value from raw payload bytes for routing purposes.
// This is used by the sharding module to get routing field values without full deserialization.
func (e *Extractor) ExtractFieldForRouting(rawPayload []byte, fieldName string) (string, bool) {
	result := gjson.GetBytes(rawPayload, fieldName)
	if !result.Exists() {
		return "", false
	}
	return result.String(), true
}

// ExtractBulk extracts routing information from a bulk request without deserializing all documents.
// The expected format is: {"documents": [{...}, {...}, ...]}
func (e *Extractor) ExtractBulk(rawBulk []byte) ([]ExtractedBulkDoc, error) {
	docsResult := gjson.GetBytes(rawBulk, "documents")
	if !docsResult.Exists() || !docsResult.IsArray() {
		return nil, errors.New("documents array is required")
	}

	var docs []ExtractedBulkDoc
	var extractErr error

	docsResult.ForEach(func(_, value gjson.Result) bool {
		rawDoc := []byte(value.Raw)

		indexID := gjson.GetBytes(rawDoc, "index_id").String()
		if indexID == "" {
			extractErr = errors.New("index_id is required in bulk document")
			return false
		}

		docID := gjson.GetBytes(rawDoc, "document_id").String()
		if docID == "" {
			extractErr = errors.New("document_id is required in bulk document")
			return false
		}

		var routing map[string]string
		routingResult := gjson.GetBytes(rawDoc, "routing")
		if routingResult.Exists() && routingResult.IsObject() {
			routing = make(map[string]string)
			routingResult.ForEach(func(key, val gjson.Result) bool {
				routing[key.String()] = val.String()
				return true
			})
		}

		var rawPayload []byte
		payloadResult := gjson.GetBytes(rawDoc, "payload")
		if payloadResult.Exists() {
			rawPayload = []byte(payloadResult.Raw)
		} else {
			rawPayload = []byte("{}")
		}

		docs = append(docs, ExtractedBulkDoc{
			IndexID:    indexID,
			DocumentID: docID,
			Routing:    routing,
			RawPayload: rawPayload,
		})

		return true
	})

	if extractErr != nil {
		return nil, extractErr
	}

	return docs, nil
}

// ExtractedBulkDoc contains extracted information from a document in a bulk request.
type ExtractedBulkDoc struct {
	IndexID    string
	DocumentID string
	Routing    map[string]string
	RawPayload []byte
}
