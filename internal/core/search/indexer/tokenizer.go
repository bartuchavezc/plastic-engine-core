package indexer

import (
	"fmt"
	"strings"
)

// Token represents the output of tokenization/analyzing a field value.
type Token struct {
	Term      string
	Position  int
	StartByte int
	EndByte   int
}

// Tokenizer splits raw text into tokens.
type Tokenizer interface {
	Tokenize(value string) []Token
}

// TokenizerFactory resolves tokenizers by name.
type TokenizerFactory struct{}

// NewTokenizer builds a tokenizer by identifier.
func (f TokenizerFactory) NewTokenizer(name string) (Tokenizer, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "whitespace":
		return WhitespaceTokenizer{}, nil
	default:
		return nil, fmt.Errorf("unsupported tokenizer %q", name)
	}
}

// WhitespaceTokenizer splits text by spaces preserving basic offsets.
type WhitespaceTokenizer struct{}

func (WhitespaceTokenizer) Tokenize(value string) []Token {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	terms := strings.Fields(value)
	tokens := make([]Token, 0, len(terms))

	offset := 0
	position := 0

	for _, term := range terms {
		position++

		start := strings.Index(value[offset:], term)
		if start < 0 {
			continue
		}
		start += offset
		end := start + len(term)

		tokens = append(tokens, Token{
			Term:      term,
			Position:  position,
			StartByte: start,
			EndByte:   end,
		})

		offset = end
	}

	return tokens
}
