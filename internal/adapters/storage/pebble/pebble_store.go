package pebble

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	pebbledb "github.com/cockroachdb/pebble"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = pebbledb.ErrNotFound

// KeyValue represents a key-value pair from a scan operation.
type KeyValue struct {
	Key   string
	Value string
}

// BatchStore provides batch operations for atomic writes.
type BatchStore interface {
	Set(key string, value string) error
	SetBytes(key string, value []byte) error
	Delete(key string) error
	SetInt64(key string, value int64) error
	SetFloat64(key string, value float64) error
	Increment(key string, delta int64) error
	// MergeInt64 atomically adds delta to the int64 at key using Pebble's merge operator.
	MergeInt64(key string, delta int64) error
	Commit() error
	Close() error
}

// StoreConfig configures PebbleStore behavior.
type StoreConfig struct {
	// SyncWrites forces fsync on every write. Default false (NoSync) for better performance.
	// Only set to true if you need durability guarantees against power loss.
	SyncWrites bool

	// MaxConcurrentCompactions limits concurrent compaction goroutines.
	// Default 0 uses Pebble's default (runtime.NumCPU()).
	// For multiple shards, lower values reduce total goroutines.
	MaxConcurrentCompactions int

	// MemTableSize is the size of each memtable in bytes.
	// Default 0 uses Pebble's default (4MB).
	MemTableSize int

	// MemTableStopWritesThreshold is the number of queued memtables before
	// Pebble stalls writes. Default 0 uses Pebble's default (2).
	// Higher values (e.g. 4) allow more memtables to queue for flush before
	// blocking, smoothing out write throughput under sustained load.
	MemTableStopWritesThreshold int

	// L0CompactionThreshold is the number of L0 sub-levels that triggers compaction.
	// Default 0 uses Pebble's default (4).
	L0CompactionThreshold int

	// L0StopWritesThreshold is the number of L0 sub-levels at which Pebble
	// completely stalls writes. Default 0 uses Pebble's default (12).
	// Increasing this (e.g. 20) gives compaction more headroom before stalling,
	// trading higher read amplification for steadier write throughput.
	L0StopWritesThreshold int

	// DisableDiskHealthCheck disables Pebble's disk health monitoring goroutines.
	// Default false. Set true to reduce goroutine count (one less per open file).
	DisableDiskHealthCheck bool

	// Cache is an optional shared block cache. nil = Pebble creates an 8MB default.
	// The caller owns the lifecycle (Unref). PebbleStore will NOT close this cache.
	Cache *pebbledb.Cache
}

// DefaultStoreConfig returns the default configuration optimized for indexing performance.
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		SyncWrites:               false, // NoSync by default - WAL still provides crash recovery
		MaxConcurrentCompactions: 1,     // Limit compaction parallelism per shard
		MemTableSize:             0,     // Use Pebble default
		DisableDiskHealthCheck:   false, // Keep health checks enabled by default
	}
}

// PebbleStore wraps a Pebble database with convenient methods.
type PebbleStore struct {
	db        *pebbledb.DB
	path      string
	writeOpts *pebbledb.WriteOptions
}

// Int64AddMerger implements a merge operator that sums int64 values.
// This enables atomic increment operations without read-modify-write races.
var Int64AddMerger = &pebbledb.Merger{
	Name: "Int64AddMerger",
	Merge: func(key, value []byte) (pebbledb.ValueMerger, error) {
		return &int64ValueMerger{sum: bytesToInt64(value)}, nil
	},
}

type int64ValueMerger struct {
	sum int64
}

func (m *int64ValueMerger) MergeNewer(value []byte) error {
	m.sum += bytesToInt64(value)
	return nil
}

func (m *int64ValueMerger) MergeOlder(value []byte) error {
	m.sum += bytesToInt64(value)
	return nil
}

func (m *int64ValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return int64ToBytes(m.sum), nil, nil
}

func bytesToInt64(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func int64ToBytes(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

// NewPebbleStore opens or creates a Pebble database at the given path with default config.
func NewPebbleStore(path string) (*PebbleStore, error) {
	return NewPebbleStoreWithConfig(path, DefaultStoreConfig())
}

// NewPebbleStoreWithConfig opens or creates a Pebble database with custom configuration.
func NewPebbleStoreWithConfig(path string, cfg StoreConfig) (*PebbleStore, error) {
	opts := &pebbledb.Options{
		Merger: Int64AddMerger,
	}

	// Limit compaction parallelism to reduce goroutine count when running multiple shards
	if cfg.MaxConcurrentCompactions > 0 {
		opts.MaxConcurrentCompactions = func() int { return cfg.MaxConcurrentCompactions }
	}

	// Configure memtable size if specified
	if cfg.MemTableSize > 0 {
		opts.MemTableSize = uint64(cfg.MemTableSize)
	}

	// L0 write stall tuning: controls when Pebble throttles/blocks writes
	if cfg.MemTableStopWritesThreshold > 0 {
		opts.MemTableStopWritesThreshold = cfg.MemTableStopWritesThreshold
	}
	if cfg.L0CompactionThreshold > 0 {
		opts.L0CompactionThreshold = cfg.L0CompactionThreshold
	}
	if cfg.L0StopWritesThreshold > 0 {
		opts.L0StopWritesThreshold = cfg.L0StopWritesThreshold
	}

	if cfg.Cache != nil {
		opts.Cache = cfg.Cache
	}

	db, err := pebbledb.Open(path, opts)
	if err != nil {
		return nil, err
	}

	writeOpts := pebbledb.NoSync
	if cfg.SyncWrites {
		writeOpts = pebbledb.Sync
	}

	return &PebbleStore{
		db:        db,
		path:      path,
		writeOpts: writeOpts,
	}, nil
}

// Get retrieves a string value by key.
func (s *PebbleStore) Get(key string) (string, error) {
	value, closer, err := s.db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, pebbledb.ErrNotFound) {
			return "", err
		}

		return "", err
	}
	defer closer.Close()

	return string(value), nil
}

// GetBytes retrieves raw bytes by key.
func (s *PebbleStore) GetBytes(key string) ([]byte, error) {
	value, closer, err := s.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Set stores a string value at key.
func (s *PebbleStore) Set(key string, value string) error {
	return s.db.Set([]byte(key), []byte(value), s.writeOpts)
}

// SetBytes stores raw bytes at key.
func (s *PebbleStore) SetBytes(key string, value []byte) error {
	return s.db.Set([]byte(key), value, s.writeOpts)
}

// Delete removes a key from the store.
func (s *PebbleStore) Delete(key string) error {
	return s.db.Delete([]byte(key), s.writeOpts)
}

// MergeInt64 atomically adds delta to the int64 at key using Pebble's merge operator.
// This is safe for concurrent use - no read-modify-write race conditions.
func (s *PebbleStore) MergeInt64(key string, delta int64) error {
	return s.db.Merge([]byte(key), int64ToBytes(delta), s.writeOpts)
}

// Close closes the database.
func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// IsNotFound reports whether the error indicates absence of a key.
func IsNotFound(err error) bool {
	return errors.Is(err, pebbledb.ErrNotFound)
}

// --------------------------------------------------------------------------
// Numeric helpers
// --------------------------------------------------------------------------

// GetInt64 retrieves an int64 stored in big-endian format.
func (s *PebbleStore) GetInt64(key string) (int64, error) {
	data, err := s.GetBytes(key)
	if err != nil {
		return 0, err
	}
	if len(data) != 8 {
		return 0, errors.New("invalid int64 value size")
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

// SetInt64 stores an int64 in big-endian format.
func (s *PebbleStore) SetInt64(key string, value int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return s.SetBytes(key, buf)
}

// GetFloat64 retrieves a float64 stored as IEEE 754 big-endian bits.
func (s *PebbleStore) GetFloat64(key string) (float64, error) {
	data, err := s.GetBytes(key)
	if err != nil {
		return 0, err
	}
	if len(data) != 8 {
		return 0, errors.New("invalid float64 value size")
	}
	bits := binary.BigEndian.Uint64(data)
	return math.Float64frombits(bits), nil
}

// SetFloat64 stores a float64 as IEEE 754 big-endian bits.
func (s *PebbleStore) SetFloat64(key string, value float64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(value))
	return s.SetBytes(key, buf)
}

// Increment atomically adds delta to the int64 at key using the merge operator.
// This is safe for concurrent use - no read-modify-write race conditions.
// Note: The returned value is the delta applied, not the new total (use GetInt64 if needed).
func (s *PebbleStore) Increment(key string, delta int64) (int64, error) {
	if err := s.MergeInt64(key, delta); err != nil {
		return 0, err
	}
	return delta, nil
}

// --------------------------------------------------------------------------
// Prefix scanning
// --------------------------------------------------------------------------

// PrefixScan returns all key-value pairs whose key starts with prefix.
func (s *PebbleStore) PrefixScan(prefix string) ([]KeyValue, error) {
	prefixBytes := []byte(prefix)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefixBytes,
		UpperBound: prefixUpperBound(prefixBytes),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []KeyValue
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		val := string(iter.Value())
		results = append(results, KeyValue{Key: key, Value: val})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return results, nil
}

// PrefixScanKeys returns all keys that start with prefix (values ignored).
func (s *PebbleStore) PrefixScanKeys(prefix string) ([]string, error) {
	prefixBytes := []byte(prefix)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefixBytes,
		UpperBound: prefixUpperBound(prefixBytes),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}

// PrefixScanLimit returns up to limit key-value pairs matching prefix.
func (s *PebbleStore) PrefixScanLimit(prefix string, limit int) ([]KeyValue, error) {
	prefixBytes := []byte(prefix)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefixBytes,
		UpperBound: prefixUpperBound(prefixBytes),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []KeyValue
	for iter.First(); iter.Valid() && len(results) < limit; iter.Next() {
		key := string(iter.Key())
		val := string(iter.Value())
		results = append(results, KeyValue{Key: key, Value: val})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return results, nil
}

// KeyValueBytes represents a key-value pair with raw byte values.
type KeyValueBytes struct {
	Key   []byte
	Value []byte
}

// PrefixScanBytes returns all key-value pairs whose key starts with prefix,
// with values as raw []byte (avoids string conversion for binary data).
func (s *PebbleStore) PrefixScanBytes(prefix string) ([]KeyValueBytes, error) {
	prefixBytes := []byte(prefix)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefixBytes,
		UpperBound: prefixUpperBound(prefixBytes),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []KeyValueBytes
	for iter.First(); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		val := make([]byte, len(iter.Value()))
		copy(val, iter.Value())
		results = append(results, KeyValueBytes{Key: key, Value: val})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return results, nil
}

// PrefixIterBytes iterates over all key-value pairs matching prefix, calling fn
// for each. If fn returns false, iteration stops early. Key and value slices
// point to iterator-internal memory and are only valid for the duration of
// the callback — callers must copy if they need to retain them.
func (s *PebbleStore) PrefixIterBytes(prefix string, fn func(key, value []byte) bool) error {
	prefixBytes := []byte(prefix)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefixBytes,
		UpperBound: prefixUpperBound(prefixBytes),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if !fn(iter.Key(), iter.Value()) {
			break
		}
	}
	return iter.Error()
}

// PrefixIterKeys iterates over all keys matching prefix, calling fn for each.
// If fn returns false, iteration stops early. Keys passed to fn are only valid
// for the duration of the callback (they point to iterator-internal memory).
func (s *PebbleStore) PrefixIterKeys(prefix string, fn func(key []byte) bool) error {
	prefixBytes := []byte(prefix)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefixBytes,
		UpperBound: prefixUpperBound(prefixBytes),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if !fn(iter.Key()) {
			break
		}
	}
	return iter.Error()
}

// prefixUpperBound computes the exclusive upper bound for a prefix scan.
// It increments the last byte to form the upper bound.
func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] < 0xFF {
			upper[i]++
			return upper[:i+1]
		}
	}
	// All 0xFF bytes: no upper bound (scan to end)
	return nil
}

// --------------------------------------------------------------------------
// Bounded iterator (reusable across multiple prefix scans)
// --------------------------------------------------------------------------

// BoundedIterator wraps a Pebble iterator that can be repositioned with
// SetBounds, avoiding the overhead of creating/destroying an iterator per scan.
type BoundedIterator struct {
	iter *pebbledb.Iterator
}

// NewBoundedIterator creates a reusable iterator over the full keyspace.
// The caller must call Close when done.
func (s *PebbleStore) NewBoundedIterator() (*BoundedIterator, error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	return &BoundedIterator{iter: iter}, nil
}

// ScanPrefix repositions the iterator to scan all keys matching prefix,
// calling fn for each key-value pair. Key and value slices point to
// iterator-internal memory and are only valid for the duration of the callback.
// Return false from fn to stop early.
func (bi *BoundedIterator) ScanPrefix(prefix []byte, fn func(key, value []byte) bool) error {
	upper := prefixUpperBound(prefix)
	bi.iter.SetBounds(prefix, upper)
	for bi.iter.SeekGE(prefix); bi.iter.Valid(); bi.iter.Next() {
		if !fn(bi.iter.Key(), bi.iter.Value()) {
			break
		}
	}
	return bi.iter.Error()
}

// Close releases the underlying Pebble iterator.
func (bi *BoundedIterator) Close() error {
	return bi.iter.Close()
}

// --------------------------------------------------------------------------
// Batch operations
// --------------------------------------------------------------------------

// Batch groups multiple writes for atomic commit.
type Batch struct {
	batch     *pebbledb.Batch
	store     *PebbleStore
	writeOpts *pebbledb.WriteOptions
}

// NewBatch creates a new write batch.
func (s *PebbleStore) NewBatch() BatchStore {
	return &Batch{
		batch:     s.db.NewBatch(),
		store:     s,
		writeOpts: s.writeOpts,
	}
}

// Set adds a string key-value to the batch.
func (b *Batch) Set(key, value string) error {
	b.batch.Set([]byte(key), []byte(value), nil)
	return nil
}

// SetBytes adds a raw key-value to the batch.
func (b *Batch) SetBytes(key string, value []byte) error {
	b.batch.Set([]byte(key), value, nil)
	return nil
}

// SetInt64 adds an int64 to the batch.
func (b *Batch) SetInt64(key string, value int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	b.batch.Set([]byte(key), buf, nil)
	return nil
}

// SetFloat64 adds a float64 to the batch.
func (b *Batch) SetFloat64(key string, value float64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(value))
	b.batch.Set([]byte(key), buf, nil)
	return nil
}

// Delete adds a delete operation to the batch.
func (b *Batch) Delete(key string) error {
	b.batch.Delete([]byte(key), nil)
	return nil
}

// Increment adds an atomic increment operation to the batch using the merge operator.
// This is safe for concurrent use - no read-modify-write race conditions.
func (b *Batch) Increment(key string, delta int64) error {
	return b.MergeInt64(key, delta)
}

// MergeInt64 adds an atomic merge operation to the batch.
// When committed, this will atomically add delta to the existing value at key.
func (b *Batch) MergeInt64(key string, delta int64) error {
	return b.batch.Merge([]byte(key), int64ToBytes(delta), nil)
}

// Commit applies all operations atomically.
func (b *Batch) Commit() error {
	return b.batch.Commit(b.writeOpts)
}

// Close discards the batch without committing.
func (b *Batch) Close() error {
	return b.batch.Close()
}

// --------------------------------------------------------------------------
// Existence check
// --------------------------------------------------------------------------

// Exists returns true if the key exists.
func (s *PebbleStore) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
