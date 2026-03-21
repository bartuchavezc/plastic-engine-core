package embeddings

import (
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"

	"plastic-engine-core/internal/core/ml"

	pebblestore "plastic-engine-core/internal/adapters/storage/pebble"

	pebbledb "github.com/cockroachdb/pebble"
)

// Key prefix for embedding vectors: V\x00<term> → []float32 binary (4 bytes × dim, little-endian)
const embeddingPrefix = "V\x00"

// PebbleEmbeddingStoreConfig configures a PebbleEmbeddingStore.
type PebbleEmbeddingStoreConfig struct {
	DataDir       string
	Dim           int
	CacheSize     int // LRU cache max entries (0 = disabled)
	CacheTTLSecs  int // LRU cache TTL in seconds (0 = 120)
	Cache         *pebbledb.Cache
}

// PebbleEmbeddingStore persists term embeddings in Pebble and implements ml.EmbeddingLookup.
type PebbleEmbeddingStore struct {
	db    *pebblestore.PebbleStore
	dim   int
	cache *embeddingLRU // nil = disabled
}

// Verify interface compliance at compile time.
var _ ml.EmbeddingLookup = (*PebbleEmbeddingStore)(nil)

// NewPebbleEmbeddingStore opens or creates an embedding store.
func NewPebbleEmbeddingStore(cfg PebbleEmbeddingStoreConfig) (*PebbleEmbeddingStore, error) {
	if cfg.Dim <= 0 {
		return nil, fmt.Errorf("embedding dim must be > 0, got %d", cfg.Dim)
	}

	dbPath := filepath.Join(cfg.DataDir, "embeddings")
	db, err := pebblestore.NewPebbleStoreWithConfig(dbPath, pebblestore.StoreConfig{
		SyncWrites:               false,
		MaxConcurrentCompactions: 1,
		MemTableSize:             4 * 1024 * 1024,
		Cache:                    cfg.Cache,
	})
	if err != nil {
		return nil, fmt.Errorf("open embedding store at %s: %w", dbPath, err)
	}

	s := &PebbleEmbeddingStore{
		db:  db,
		dim: cfg.Dim,
	}

	cacheSize := cfg.CacheSize
	if cacheSize <= 0 {
		cacheSize = 5000
	}
	ttlSecs := cfg.CacheTTLSecs
	if ttlSecs <= 0 {
		ttlSecs = 120
	}
	s.cache = newEmbeddingLRU(cacheSize, ttlSecs)

	return s, nil
}

// Get retrieves the embedding vector for a term. Returns nil, false if not found.
func (s *PebbleEmbeddingStore) Get(term string) ([]float32, bool) {
	// Check cache first
	if s.cache != nil {
		if vec, ok := s.cache.get(term); ok {
			return vec, true
		}
	}

	key := embeddingPrefix + term
	val, err := s.db.GetBytes(key)
	if err != nil || len(val) == 0 {
		return nil, false
	}

	vec := decodeVector(val, s.dim)
	if vec == nil {
		return nil, false
	}

	if s.cache != nil {
		s.cache.put(term, vec)
	}
	return vec, true
}

// CosineSimilarity computes cosine similarity between two terms' embeddings.
func (s *PebbleEmbeddingStore) CosineSimilarity(termA, termB string) (float64, bool) {
	vecA, okA := s.Get(termA)
	vecB, okB := s.Get(termB)
	if !okA || !okB {
		return 0, false
	}
	return cosine(vecA, vecB), true
}

// CentroidSimilarity computes cosine similarity between the centroid of query term embeddings
// and a candidate term's embedding.
func (s *PebbleEmbeddingStore) CentroidSimilarity(queryTerms []string, candidate string) (float64, bool) {
	if len(queryTerms) == 0 {
		return 0, false
	}

	candVec, ok := s.Get(candidate)
	if !ok {
		return 0, false
	}

	centroid := make([]float32, s.dim)
	count := 0
	for _, qt := range queryTerms {
		vec, ok := s.Get(qt)
		if !ok {
			continue
		}
		for i, v := range vec {
			centroid[i] += v
		}
		count++
	}
	if count == 0 {
		return 0, false
	}

	inv := float32(1.0 / float64(count))
	for i := range centroid {
		centroid[i] *= inv
	}

	return cosine(centroid, candVec), true
}

// BatchPut persists a batch of embeddings atomically.
func (s *PebbleEmbeddingStore) BatchPut(embeddings map[string][]float32) error {
	batch := s.db.NewBatch()
	for term, vec := range embeddings {
		if len(vec) != s.dim {
			batch.Close()
			return fmt.Errorf("embedding for %q has dim %d, expected %d", term, len(vec), s.dim)
		}
		key := embeddingPrefix + term
		_ = batch.SetBytes(key, encodeVector(vec))
	}
	err := batch.Commit()
	batch.Close()
	return err
}

// Close closes the underlying Pebble store.
func (s *PebbleEmbeddingStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// encodeVector encodes a float32 slice to little-endian binary.
func encodeVector(vec []float32) []byte {
	buf := make([]byte, len(vec)*4)
	for i, v := range vec {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}

// decodeVector decodes a little-endian binary buffer to float32 slice.
func decodeVector(b []byte, dim int) []float32 {
	if len(b) < dim*4 {
		return nil
	}
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return vec
}

// cosine computes cosine similarity between two vectors.
func cosine(a, b []float32) float64 {
	var dot, normA, normB float64
	for i := range a {
		ai, bi := float64(a[i]), float64(b[i])
		dot += ai * bi
		normA += ai * ai
		normB += bi * bi
	}
	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0
	}
	return dot / denom
}
