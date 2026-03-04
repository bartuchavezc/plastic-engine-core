package knowledge

import (
	"fmt"
	"path/filepath"
	"sync"
)

// Store manages named knowledge graphs (Type B indices).
// Each graph is backed by its own Pebble instance.
type Store struct {
	mu      sync.RWMutex
	graphs  map[string]*AdjacencyMatrix
	dataDir string
}

// NewStore creates a new knowledge graph store.
func NewStore(dataDir string) *Store {
	return &Store{
		graphs:  make(map[string]*AdjacencyMatrix),
		dataDir: dataDir,
	}
}

// GetOrCreate returns an existing graph or creates a new one.
func (s *Store) GetOrCreate(name string) (*AdjacencyMatrix, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if am, ok := s.graphs[name]; ok {
		return am, nil
	}

	graphDir := filepath.Join(s.dataDir, "knowledge", name)
	am, err := NewAdjacencyMatrix(AdjacencyMatrixConfig{DataDir: graphDir})
	if err != nil {
		return nil, fmt.Errorf("create knowledge graph %q: %w", name, err)
	}
	s.graphs[name] = am
	return am, nil
}

// Get returns an existing graph.
func (s *Store) Get(name string) (*AdjacencyMatrix, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	am, ok := s.graphs[name]
	return am, ok
}

// Delete removes a knowledge graph and closes its resources.
func (s *Store) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	am, ok := s.graphs[name]
	if !ok {
		return nil
	}
	delete(s.graphs, name)
	return am.Close()
}

// List returns all knowledge graph names.
func (s *Store) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.graphs))
	for name := range s.graphs {
		names = append(names, name)
	}
	return names
}

// Close closes all knowledge graphs.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, am := range s.graphs {
		am.Close()
		delete(s.graphs, name)
	}
	return nil
}
