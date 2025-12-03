package shards

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/adapters/storage/pebble"
)

// Assignment describes how a shard should look when assigned to this node.
type Assignment struct {
	ID               string
	IndexID          string
	ShardKey         string
	ShardStrategy    indexes.ShardStrategy
	NeedsReplication bool
	Primary          bool
	RangeHint        string // placeholder for the shard's logical range
	Analyzer         string
	Tokenizer        string
	MappingVersion   int
	Fields           []indexes.FieldMapping
}

// Manager coordinates the lifecycle of shards owned by a search node.
type Manager struct {
	rootDir string

	mu     sync.RWMutex
	shards map[string]*Shard

	loadOnce sync.Once
	loadErr  error
}

// NewManager builds a Manager rooted in the given directory.
func NewManager(rootDir string) *Manager {
	return &Manager{
		rootDir: rootDir,
		shards:  make(map[string]*Shard),
	}
}

// Sync ensures that every assigned shard is available locally.
func (m *Manager) Sync(assignments []Assignment) error {
	m.loadOnce.Do(func() {
		m.loadErr = m.loadExistingShards()
	})
	if m.loadErr != nil {
		return m.loadErr
	}

	for _, assignment := range assignments {
		if err := m.ensureLocalShard(assignment); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) ensureLocalShard(assignment Assignment) error {
	if existing, ok := m.GetShard(assignment.ID); ok {
		if err := writeManifest(m.shardPath(assignment.ID), assignment); err != nil {
			return fmt.Errorf("update manifest for shard %s: %w", assignment.ID, err)
		}

		m.mu.Lock()
		existing.Info = assignment
		m.mu.Unlock()
		return nil
	}

	if assignment.NeedsReplication {
		go m.replicateShard(assignment)
		return nil
	}

	return m.openShard(assignment)
}

func (m *Manager) openShard(assignment Assignment) error {
	shardPath := m.shardPath(assignment.ID)

	if err := os.MkdirAll(shardPath, 0o755); err != nil {
		return fmt.Errorf("create shard directory %s: %w", shardPath, err)
	}

	if err := writeManifest(shardPath, assignment); err != nil {
		return fmt.Errorf("write manifest for shard %s: %w", assignment.ID, err)
	}

	store, err := pebble.NewPebbleStore(shardPath)
	if err != nil {
		return fmt.Errorf("open pebble for shard %s: %w", assignment.ID, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[assignment.ID] = &Shard{ID: assignment.ID, Store: store, Info: assignment}
	return nil
}

func (m *Manager) shardPath(id string) string {
	return filepath.Join(m.rootDir, id)
}

func (m *Manager) replicateShard(assignment Assignment) {
	// TODO: implement replication pipeline (snapshot, copy SSTables, validation, open Pebble, mark ready).
}

func (m *Manager) existsLocal(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.shards[id]
	return ok
}

// Close releases all open Pebble stores managed by the shard manager.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for id, shard := range m.shards {
		if shard.Store == nil {
			continue
		}
		if err := shard.Store.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close shard %s: %w", id, err)
		}
	}
	m.shards = make(map[string]*Shard)
	return firstErr
}

// ListShardIDs returns the identifiers of shards currently managed locally.
func (m *Manager) ListShardIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.shards))
	for id := range m.shards {
		ids = append(ids, id)
	}
	return ids
}

// GetShard returns the shard associated with the given identifier.
func (m *Manager) GetShard(id string) (*Shard, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sh, ok := m.shards[id]
	return sh, ok
}

func writeManifest(shardPath string, assignment Assignment) error {
	manifestPath := filepath.Join(shardPath, "manifest.json")
	data, err := json.MarshalIndent(assignment, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(manifestPath, data, 0o644)
}

func readManifest(shardPath string) (Assignment, error) {
	data, err := os.ReadFile(filepath.Join(shardPath, "manifest.json"))
	if err != nil {
		return Assignment{}, err
	}

	var assignment Assignment
	if err := json.Unmarshal(data, &assignment); err != nil {
		return Assignment{}, err
	}
	return assignment, nil
}

func (m *Manager) loadExistingShards() error {
	if m.rootDir == "" {
		return nil
	}

	if err := os.MkdirAll(m.rootDir, 0o755); err != nil {
		return fmt.Errorf("create data dir %s: %w", m.rootDir, err)
	}

	entries, err := os.ReadDir(m.rootDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read data dir %s: %w", m.rootDir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		shardPath := m.shardPath(id)

		assignment, err := readManifest(shardPath)
		if err != nil {
			continue
		}

		store, err := pebble.NewPebbleStore(shardPath)
		if err != nil {
			return fmt.Errorf("open existing shard %s: %w", id, err)
		}

		m.mu.Lock()
		m.shards[id] = &Shard{ID: id, Store: store, Info: assignment}
		m.mu.Unlock()
	}

	return nil
}
