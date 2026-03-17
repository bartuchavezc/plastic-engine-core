package document

import (
	"strings"
	"unicode"

	"github.com/kljensen/snowball"
)

// StandardAnalyzer mirrors Elasticsearch's standard analysis chain:
//
//  1. Lowercase normalization
//  2. Stop-word removal (configurable per language)
//  3. Snowball stemming to reduce words to root forms
//     (plurals → singular, conjugations → infinitive, etc.)
//
// Language defaults to "english" if not set.
type StandardAnalyzer struct {
	// Language for stemming and stop words: "english", "spanish", "french", etc.
	// Empty defaults to "english".
	Language string

	// ExtraStopWords appended to the built-in set.
	ExtraStopWords []string

	stopSet map[string]struct{} // built lazily
}

// NewStandardAnalyzer creates a StandardAnalyzer for the given language.
func NewStandardAnalyzer(language string) *StandardAnalyzer {
	if language == "" {
		language = "english"
	}
	a := &StandardAnalyzer{Language: language}
	a.buildStopSet()
	return a
}

func (a *StandardAnalyzer) Analyze(tokens []Token) []Token {
	if a.stopSet == nil {
		a.buildStopSet()
	}

	lang := a.Language
	if lang == "" {
		lang = "english"
	}

	result := make([]Token, 0, len(tokens))

	for i := range tokens {
		// 1. Lowercase
		term := strings.ToLower(tokens[i].Term)

		// 2. Strip leading/trailing punctuation and symbols (e.g. quotes, hyphens)
		term = strings.TrimFunc(term, func(r rune) bool {
			return unicode.IsPunct(r) || unicode.IsSymbol(r)
		})
		if term == "" {
			continue
		}

		// 2b. If interior non-alphanumeric chars exist, split on boundaries.
		// E.g., "face&body" → ["face","body"], "urns,cats,dogs" → ["urns","cats","dogs"]
		// Clean tokens (no interior non-alpha) take the fast path.
		if needsStrip(term) {
			parts := strings.FieldsFunc(term, func(r rune) bool {
				return !unicode.IsLetter(r) && !unicode.IsDigit(r)
			})
			for _, part := range parts {
				if processed, ok := a.processTerm(part, lang); ok {
					tok := tokens[i]
					tok.Term = processed
					result = append(result, tok)
				}
			}
			continue
		}

		// Fast path: clean token, no splitting needed
		if processed, ok := a.processTerm(term, lang); ok {
			tok := tokens[i]
			tok.Term = processed
			result = append(result, tok)
		}
	}

	return result
}

// processTerm applies stop-word filtering and stemming to a single term.
// Returns the processed term and whether it should be kept.
func (a *StandardAnalyzer) processTerm(term, lang string) (string, bool) {
	if len(term) < 2 {
		return "", false
	}
	// Stop-word removal
	if _, stop := a.stopSet[term]; stop {
		return "", false
	}
	// Snowball stemming — trust the stemmer for all output lengths
	if stemmed, err := snowball.Stem(term, lang, false); err == nil && stemmed != "" {
		return stemmed, true
	}
	return term, true
}

// needsStrip returns true if s contains any non-letter, non-digit rune.
// Fast check to skip the split path for clean tokens.
func needsStrip(s string) bool {
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

func (a *StandardAnalyzer) buildStopSet() {
	lang := a.Language
	if lang == "" {
		lang = "english"
	}

	words := stopWordsFor(lang)
	a.stopSet = make(map[string]struct{}, len(words)+len(a.ExtraStopWords))
	for _, w := range words {
		a.stopSet[w] = struct{}{}
	}
	for _, w := range a.ExtraStopWords {
		a.stopSet[strings.ToLower(w)] = struct{}{}
	}
}

// stopWordsFor returns the default stop-word list for a language.
// Mirrors Elasticsearch's _english_, _spanish_, etc. defaults.
func stopWordsFor(language string) []string {
	switch strings.ToLower(language) {
	case "english":
		return englishStopWords
	case "spanish":
		return spanishStopWords
	default:
		return englishStopWords
	}
}

// englishStopWords — Elasticsearch/Lucene default English stop words (_english_).
var englishStopWords = []string{
	"a", "an", "and", "are", "as", "at", "be", "but", "by",
	"do", "for", "from", "had", "has", "have", "he", "her",
	"him", "his", "how", "i", "if", "in", "into", "is", "it", "its",
	"me", "my", "no", "not", "of", "on", "or", "our",
	"she", "so", "some", "such",
	"that", "the", "their", "them", "then", "there", "these",
	"they", "this", "to", "us", "was", "we", "what", "when",
	"which", "who", "will", "with", "you", "your",
}

// spanishStopWords — Elasticsearch/Lucene default Spanish stop words.
var spanishStopWords = []string{
	"a", "al", "algo", "algunas", "algunos", "ante", "antes",
	"como", "con", "contra", "cual", "cuando",
	"de", "del", "desde", "donde", "durante",
	"e", "el", "ella", "ellas", "ellos", "en", "entre", "era", "esa", "esas",
	"ese", "eso", "esos", "esta", "estaba", "estado", "estar", "estas",
	"este", "esto", "estos", "fue", "ha", "hasta", "hay",
	"la", "las", "le", "les", "lo", "los",
	"mas", "mi", "mia", "mias", "mio", "mios", "muy",
	"na", "ni", "no", "nos", "nosotros", "nuestro", "nuestra",
	"o", "otra", "otras", "otro", "otros",
	"para", "pero", "por", "que", "quien",
	"se", "si", "sin", "sino", "sobre", "somos", "son", "soy", "su", "sus",
	"te", "ti", "tu", "tus", "un", "una", "uno", "unos", "usted", "ustedes",
	"y", "ya", "yo",
}
