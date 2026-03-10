package document

import (
	"strings"
	"unicode"

	"github.com/blevesearch/segment"
)

// StandardTokenizer implements UAX#29 Unicode Text Segmentation.
// It uses the blevesearch/segment library for proper word boundary detection,
// filtering out non-word segments and punctuation-only tokens.
// It also detects sentence boundaries from punctuation (. ! ? ; ¿ ¡) and
// assigns a SentenceID to each token.
type StandardTokenizer struct{}

func (StandardTokenizer) Tokenize(value string) []Token {
	if value == "" {
		return nil
	}

	segmenter := segment.NewWordSegmenterDirect([]byte(value))
	var tokens []Token
	position := 0
	byteOffset := 0
	sentenceID := 0
	pendingBreak := false

	for segmenter.Segment() {
		typ := segmenter.Type()
		tokenBytes := segmenter.Bytes()
		tokenLen := len(tokenBytes)

		start := byteOffset
		end := byteOffset + tokenLen
		byteOffset = end

		// Type 0 = non-word (whitespace, punctuation, etc).
		// Check for sentence-ending punctuation before skipping.
		if typ == 0 {
			if isSentenceEndNonWord(tokenBytes) {
				pendingBreak = true
			}
			continue
		}

		tokenStr := string(tokenBytes)

		// Skip pure punctuation/symbol tokens (type > 0 but all punct)
		if isPunctuation(tokenStr) {
			continue
		}

		// Defense-in-depth: trim attached punctuation/symbols (e.g., "kids!!!" → "kids")
		// and normalize to lowercase. The analyzer also does this, but applying it here
		// ensures clean tokens even if documents bypass the analyzer.
		tokenStr = strings.TrimFunc(tokenStr, func(r rune) bool {
			return unicode.IsPunct(r) || unicode.IsSymbol(r)
		})
		if tokenStr == "" {
			continue
		}
		tokenStr = strings.ToLower(tokenStr)

		// Real word — if there's a pending sentence break, advance sentenceID
		if pendingBreak {
			sentenceID++
			pendingBreak = false
		}

		position++
		tokens = append(tokens, Token{
			Term:       tokenStr,
			Position:   position,
			StartByte:  start,
			EndByte:    end,
			SentenceID: sentenceID,
		})
	}

	return tokens
}

// isPunctuation returns true if every rune in s is punctuation or a symbol.
func isPunctuation(s string) bool {
	for _, r := range s {
		if !unicode.IsPunct(r) && !unicode.IsSymbol(r) {
			return false
		}
	}
	return true
}

// isSentenceEndNonWord checks if a type-0 (non-word) segment contains
// sentence-ending punctuation. The segmenter emits . ! ? ; as type 0.
func isSentenceEndNonWord(b []byte) bool {
	for _, c := range b {
		switch c {
		case '.', '!', '?', ';':
			return true
		}
	}
	// Check for multi-byte sentence-end punctuation (¿ ¡)
	for i := 0; i < len(b); {
		r, size := decodeRune(b[i:])
		if r == '¿' || r == '¡' {
			return true
		}
		i += size
	}
	return false
}

// decodeRune is a minimal UTF-8 decoder to avoid importing unicode/utf8
// in a hot path (though this path is not that hot).
func decodeRune(b []byte) (rune, int) {
	if len(b) == 0 {
		return 0, 0
	}
	c := b[0]
	if c < 0x80 {
		return rune(c), 1
	}
	if c < 0xC0 {
		return 0xFFFD, 1
	}
	if c < 0xE0 && len(b) >= 2 {
		return rune(c&0x1F)<<6 | rune(b[1]&0x3F), 2
	}
	if c < 0xF0 && len(b) >= 3 {
		return rune(c&0x0F)<<12 | rune(b[1]&0x3F)<<6 | rune(b[2]&0x3F), 3
	}
	if len(b) >= 4 {
		return rune(c&0x07)<<18 | rune(b[1]&0x3F)<<12 | rune(b[2]&0x3F)<<6 | rune(b[3]&0x3F), 4
	}
	return 0xFFFD, 1
}
