package pebble

import (
	"encoding/binary"
	"errors"
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

// PebbleStore wraps a Pebble database with convenient methods.
type PebbleStore struct {
	db   *pebbledb.DB
	path string
}

// NewPebbleStore opens or creates a Pebble database at the given path.
func NewPebbleStore(path string) (*PebbleStore, error) {
	db, err := pebbledb.Open(path, nil)
	if err != nil {
		return nil, err
	}

	return &PebbleStore{db: db, path: path}, nil
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
	return s.db.Set([]byte(key), []byte(value), pebbledb.Sync)
}

// SetBytes stores raw bytes at key.
func (s *PebbleStore) SetBytes(key string, value []byte) error {
	return s.db.Set([]byte(key), value, pebbledb.Sync)
}

// Delete removes a key from the store.
func (s *PebbleStore) Delete(key string) error {
	return s.db.Delete([]byte(key), pebbledb.Sync)
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

// Increment atomically adds delta to the int64 at key (creates with delta if missing).
func (s *PebbleStore) Increment(key string, delta int64) (int64, error) {
	current, err := s.GetInt64(key)
	if err != nil && !IsNotFound(err) {
		return 0, err
	}
	newVal := current + delta
	if err := s.SetInt64(key, newVal); err != nil {
		return 0, err
	}
	return newVal, nil
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
// Batch operations
// --------------------------------------------------------------------------

// Batch groups multiple writes for atomic commit.
type Batch struct {
	batch *pebbledb.Batch
	store *PebbleStore
}

// NewBatch creates a new write batch.
func (s *PebbleStore) NewBatch() *Batch {
	return &Batch{
		batch: s.db.NewBatch(),
		store: s,
	}
}

// Set adds a string key-value to the batch.
func (b *Batch) Set(key, value string) {
	b.batch.Set([]byte(key), []byte(value), nil)
}

// SetBytes adds a raw key-value to the batch.
func (b *Batch) SetBytes(key string, value []byte) {
	b.batch.Set([]byte(key), value, nil)
}

// SetInt64 adds an int64 to the batch.
func (b *Batch) SetInt64(key string, value int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	b.batch.Set([]byte(key), buf, nil)
}

// SetFloat64 adds a float64 to the batch.
func (b *Batch) SetFloat64(key string, value float64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(value))
	b.batch.Set([]byte(key), buf, nil)
}

// Delete adds a delete operation to the batch.
func (b *Batch) Delete(key string) {
	b.batch.Delete([]byte(key), nil)
}

// Commit applies all operations atomically.
func (b *Batch) Commit() error {
	return b.batch.Commit(pebbledb.Sync)
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
