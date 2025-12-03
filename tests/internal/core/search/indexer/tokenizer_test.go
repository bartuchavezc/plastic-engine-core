package indexer_test

import (
	"testing"

	"plastic-engine-core/internal/core/search/indexer"
)

func TestWhitespaceTokenizer(t *testing.T) {
	t.Parallel()

	tokenizer := indexer.WhitespaceTokenizer{}
	input := "Hello world  from  Plastic"

	tokens := tokenizer.Tokenize(input)

	if len(tokens) != 4 {
		t.Fatalf("tokens len = %d, want 4", len(tokens))
	}

	if tokens[0].Term != "Hello" || tokens[0].Position != 1 {
		t.Fatalf("first token = %+v, want Hello position 1", tokens[0])
	}
	if tokens[1].StartByte <= tokens[0].EndByte {
		t.Fatalf("expected token offsets to be increasing: %v -> %v", tokens[0], tokens[1])
	}
}
