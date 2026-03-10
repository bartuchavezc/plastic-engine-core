package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"

	pebblestore "plastic-engine-core/internal/adapters/storage/pebble"

	pebbledb "github.com/cockroachdb/pebble"
)

// Pebble key prefixes for posting storage.
const (
	// postingPrefix: P\x00<field>\x00<term>\x00<docID> → binary(TF, positions)
	postingPrefix = "P\x00"

	// dfPrefix: PD\x00<field>\x00<term> → int64 DF (via MergeInt64)
	dfPrefix = "PD\x00"

	// metaTotalDocs: PM\x00total_docs → int64 total document count
	metaTotalDocs = "PM\x00total_docs"
)

// PostingStoreConfig configures PebblePostingStore.
type PostingStoreConfig struct {
	DataDir string

	// PebbleMemTableSize overrides the Pebble memtable size. 0 = default.
	PebbleMemTableSize int

	// MaxConcurrentCompactions limits concurrent Pebble compaction goroutines.
	// 0 = default (1).
	MaxConcurrentCompactions int

	// MemTableStopWritesThreshold: queued memtables before write stall. 0 = Pebble default (2).
	MemTableStopWritesThreshold int

	// L0CompactionThreshold: L0 sub-levels to trigger compaction. 0 = Pebble default (4).
	L0CompactionThreshold int

	// L0StopWritesThreshold: L0 sub-levels to hard-stall writes. 0 = Pebble default (12).
	L0StopWritesThreshold int

	// Cache is an optional shared block cache (nil = Pebble default 8MB).
	Cache *pebbledb.Cache
}

// PebblePostingStore stores posting lists directly in Pebble.
// Replaces the old MemSegment → DiskSegment → merge pipeline.
type PebblePostingStore struct {
	db        *pebblestore.PebbleStore
	totalDocs atomic.Int64
}

// NewPebblePostingStore opens or creates a posting store.
func NewPebblePostingStore(cfg PostingStoreConfig) (*PebblePostingStore, error) {
	dbPath := filepath.Join(cfg.DataDir, "postings")
	maxCompactions := cfg.MaxConcurrentCompactions
	if maxCompactions <= 0 {
		maxCompactions = 1
	}
	storeCfg := pebblestore.StoreConfig{
		SyncWrites:                  false,
		MaxConcurrentCompactions:    maxCompactions,
		MemTableStopWritesThreshold: cfg.MemTableStopWritesThreshold,
		L0CompactionThreshold:       cfg.L0CompactionThreshold,
		L0StopWritesThreshold:       cfg.L0StopWritesThreshold,
	}
	if cfg.PebbleMemTableSize > 0 {
		storeCfg.MemTableSize = cfg.PebbleMemTableSize
	}
	storeCfg.Cache = cfg.Cache

	db, err := pebblestore.NewPebbleStoreWithConfig(dbPath, storeCfg)
	if err != nil {
		return nil, fmt.Errorf("open posting store at %s: %w", dbPath, err)
	}

	store := &PebblePostingStore{db: db}

	// Recover totalDocs from Pebble
	if total, err := db.GetInt64(metaTotalDocs); err == nil {
		store.totalDocs.Store(total)
	}

	return store, nil
}

// IndexBatch indexes a batch of documents in a single Pebble batch commit.
// dfDeltas contains pre-aggregated document-frequency increments keyed by "field\x00term".
// This reduces MergeInt64 calls from O(total postings) to O(unique terms in batch).
func (s *PebblePostingStore) IndexBatch(docs []DocumentBatch, dfDeltas map[string]int64) error {
	if len(docs) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	keyBuf := make([]byte, 0, 256)

	for _, doc := range docs {
		for field, terms := range doc.FieldTerms {
			for _, tp := range terms {
				// Build posting key with reusable buffer: P\x00<field>\x00<term>\x00<docID>
				keyBuf = keyBuf[:0]
				keyBuf = append(keyBuf, postingPrefix...)
				keyBuf = append(keyBuf, field...)
				keyBuf = append(keyBuf, 0)
				keyBuf = append(keyBuf, tp.Term...)
				keyBuf = append(keyBuf, 0)
				keyBuf = append(keyBuf, doc.DocID...)

				val := encodePostingValue(tp.TF, tp.Positions)
				if err := batch.SetBytes(string(keyBuf), val); err != nil {
					batch.Close()
					return err
				}
			}
		}
	}

	// DF merges: one per unique term (pre-aggregated)
	for termKey, delta := range dfDeltas {
		keyBuf = keyBuf[:0]
		keyBuf = append(keyBuf, dfPrefix...)
		keyBuf = append(keyBuf, termKey...)
		if err := batch.MergeInt64(string(keyBuf), delta); err != nil {
			batch.Close()
			return err
		}
	}

	// Total docs increment
	if err := batch.MergeInt64(metaTotalDocs, int64(len(docs))); err != nil {
		batch.Close()
		return err
	}

	if err := batch.Commit(); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	s.totalDocs.Add(int64(len(docs)))
	return nil
}

// Search finds all postings for a field+term combination.
// Uses iterator-based scanning to avoid materializing a full []KeyValueBytes slice.
func (s *PebblePostingStore) Search(field, term string) ([]Hit, error) {
	prefix := postingPrefix + field + "\x00" + term + "\x00"
	termID := field + "\x00" + term
	prefixLen := len(prefix)

	var hits []Hit
	err := s.db.PrefixIterBytes(prefix, func(key, value []byte) bool {
		docID := string(key[prefixLen:])
		tf, err := decodePostingTFOnly(value)
		if err != nil {
			return true // skip corrupt entries
		}
		hits = append(hits, Hit{
			DocID:  docID,
			TermID: termID,
			TF:     tf,
		})
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("search prefix iter: %w", err)
	}
	return hits, nil
}

// SearchWithPositions finds all postings for a field+term with full position data.
// Use this for phrase/proximity queries that need position information.
func (s *PebblePostingStore) SearchWithPositions(field, term string) ([]Hit, error) {
	prefix := postingPrefix + field + "\x00" + term + "\x00"
	termID := field + "\x00" + term
	prefixLen := len(prefix)

	var hits []Hit
	err := s.db.PrefixIterBytes(prefix, func(key, value []byte) bool {
		docID := string(key[prefixLen:])
		tf, positions, err := decodePostingValue(value)
		if err != nil {
			return true // skip corrupt entries
		}
		hits = append(hits, Hit{
			DocID:     docID,
			TermID:    termID,
			TF:        tf,
			Positions: positions,
		})
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("search prefix iter: %w", err)
	}
	return hits, nil
}

// SearchByTermID searches using a termID (field\x00term format).
func (s *PebblePostingStore) SearchByTermID(termID string) ([]Hit, error) {
	parts := strings.SplitN(termID, "\x00", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid termID format: expected field\\x00term")
	}
	return s.Search(parts[0], parts[1])
}

// GetDF returns the document frequency for a field+term.
func (s *PebblePostingStore) GetDF(field, term string) int64 {
	key := dfPrefix + field + "\x00" + term
	val, err := s.db.GetInt64(key)
	if err != nil {
		return 0
	}
	return val
}

// GetDFByTermID returns the document frequency using a termID (field\x00term).
func (s *PebblePostingStore) GetDFByTermID(termID string) int64 {
	parts := strings.SplitN(termID, "\x00", 2)
	if len(parts) != 2 {
		return 0
	}
	return s.GetDF(parts[0], parts[1])
}

// GetTotalDocs returns the total number of indexed documents.
func (s *PebblePostingStore) GetTotalDocs() int64 {
	return s.totalDocs.Load()
}

// GetTermsWithPrefix returns unique termIDs (field\x00term) matching a field+prefix.
func (s *PebblePostingStore) GetTermsWithPrefix(field, prefix string) []string {
	// Scan posting keys: P\x00<field>\x00<prefix>...
	scanPrefix := postingPrefix + field + "\x00" + prefix

	seen := make(map[string]struct{})
	var terms []string

	_ = s.db.PrefixIterKeys(scanPrefix, func(key []byte) bool {
		// Key format: P\x00<field>\x00<term>\x00<docID>
		// We need to extract field\x00term as termID.
		// Strip the "P\x00" prefix, then find the last \x00 to separate term from docID.
		rest := key[len(postingPrefix):] // field\x00term\x00docID
		lastNull := bytes.LastIndexByte(rest, 0)
		if lastNull < 0 {
			return true
		}
		termID := string(rest[:lastNull]) // field\x00term
		if _, exists := seen[termID]; !exists {
			seen[termID] = struct{}{}
			terms = append(terms, termID)
		}
		return true
	})

	return terms
}

// SnapshotDF returns all document frequencies via iterator-based prefix scan.
// Returns a map of termID (field\x00term) → DF. Used by the cold epoch worker
// to avoid N individual point lookups during NPMI scoring.
// Uses PrefixIterBytes to avoid materializing a full []KeyValueBytes slice.
func (s *PebblePostingStore) SnapshotDF() map[string]int64 {
	pfxLen := len(dfPrefix)
	snapshot := make(map[string]int64, 8192)
	_ = s.db.PrefixIterBytes(dfPrefix, func(key, value []byte) bool {
		if len(value) == 8 {
			termID := string(key[pfxLen:])
			snapshot[termID] = int64(binary.BigEndian.Uint64(value))
		}
		return true
	})
	return snapshot
}

// Close persists totalDocs and closes the Pebble database.
func (s *PebblePostingStore) Close() error {
	// Persist totalDocs
	_ = s.db.SetInt64(metaTotalDocs, s.totalDocs.Load())
	return s.db.Close()
}

// encodePostingValue encodes TF and positions into binary format:
// TF (4 bytes uint32 BE) + posCount (2 bytes uint16 BE) + positions (4 bytes each uint32 BE)
func encodePostingValue(tf int, positions []int) []byte {
	posCount := len(positions)
	buf := make([]byte, 4+2+posCount*4)
	binary.BigEndian.PutUint32(buf[0:4], uint32(tf))
	binary.BigEndian.PutUint16(buf[4:6], uint16(posCount))
	for i, pos := range positions {
		binary.BigEndian.PutUint32(buf[6+i*4:6+(i+1)*4], uint32(pos))
	}
	return buf
}

// decodePostingTFOnly decodes only the TF from a binary posting value,
// skipping position data entirely. Used on the BM25 scoring path where
// positions are not needed, avoiding a []int allocation per hit.
func decodePostingTFOnly(data []byte) (int, error) {
	if len(data) < 6 {
		return 0, errors.New("posting value too short")
	}
	return int(binary.BigEndian.Uint32(data[0:4])), nil
}

// decodePostingValue decodes binary posting value into TF and positions.
func decodePostingValue(data []byte) (tf int, positions []int, err error) {
	if len(data) < 6 {
		return 0, nil, errors.New("posting value too short")
	}
	tf = int(binary.BigEndian.Uint32(data[0:4]))
	posCount := int(binary.BigEndian.Uint16(data[4:6]))
	if len(data) < 6+posCount*4 {
		return 0, nil, errors.New("posting value truncated")
	}
	positions = make([]int, posCount)
	for i := 0; i < posCount; i++ {
		positions[i] = int(binary.BigEndian.Uint32(data[6+i*4 : 6+(i+1)*4]))
	}
	return tf, positions, nil
}
