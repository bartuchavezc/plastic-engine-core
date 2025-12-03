package pebble

import (
	"errors"

	pebbledb "github.com/cockroachdb/pebble"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = pebbledb.ErrNotFound

type PebbleStore struct {
	db   *pebbledb.DB
	path string
}

func NewPebbleStore(path string) (*PebbleStore, error) {
	db, err := pebbledb.Open(path, nil)
	if err != nil {
		return nil, err
	}

	return &PebbleStore{db: db, path: path}, nil
}

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

func (s *PebbleStore) Set(key string, value string) error {
	return s.db.Set([]byte(key), []byte(value), pebbledb.Sync)
}

func (s *PebbleStore) Delete(key string) error {
	return s.db.Delete([]byte(key), pebbledb.Sync)
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// IsNotFound reports whether the error indicates absence of a key.
func IsNotFound(err error) bool {
	return errors.Is(err, pebbledb.ErrNotFound)
}
