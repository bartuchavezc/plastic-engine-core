package models

import (
	"fmt"
	"sync"
	"time"
)

// ModelStore manages ONNX model sessions with on-demand loading and eviction.
type ModelStore struct {
	mu       sync.RWMutex
	sessions map[string]*ModelSession
	configs  map[string]ModelConfig
	rootDir  string // base directory for model files (e.g. data/models/)
}

// NewModelStore creates a model store rooted at the given directory.
func NewModelStore(rootDir string) *ModelStore {
	return &ModelStore{
		sessions: make(map[string]*ModelSession),
		configs:  make(map[string]ModelConfig),
		rootDir:  rootDir,
	}
}

// Register adds a model configuration. If Preload is true, the model is loaded immediately.
func (s *ModelStore) Register(cfg ModelConfig) error {
	s.mu.Lock()
	s.configs[cfg.Name] = cfg
	s.mu.Unlock()

	if cfg.Preload {
		_, err := s.Get(cfg.Name)
		return err
	}
	return nil
}

// Get returns a loaded model session, creating one on-demand if needed.
func (s *ModelStore) Get(name string) (*ModelSession, error) {
	// Fast path: check if already loaded
	s.mu.RLock()
	session, ok := s.sessions[name]
	s.mu.RUnlock()
	if ok {
		return session, nil
	}

	// Slow path: load the model
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if session, ok := s.sessions[name]; ok {
		return session, nil
	}

	cfg, ok := s.configs[name]
	if !ok {
		return nil, fmt.Errorf("model %q not registered", name)
	}

	session, err := newModelSession(cfg)
	if err != nil {
		return nil, err
	}

	s.sessions[name] = session
	return session, nil
}

// EvictIdle closes and removes sessions not used within maxAge.
func (s *ModelStore) EvictIdle(maxAge time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for name, session := range s.sessions {
		if session.LastUsed().Before(cutoff) {
			session.Close()
			delete(s.sessions, name)
		}
	}
}

// Close destroys all loaded sessions.
func (s *ModelStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, session := range s.sessions {
		session.Close()
		delete(s.sessions, name)
	}
}
