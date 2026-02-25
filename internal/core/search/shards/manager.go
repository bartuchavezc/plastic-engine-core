package shards

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"plastic-engine-core/internal/core/cluster/indexes"
	"plastic-engine-core/internal/core/search/segment"
)

// ManagerConfig holds configuration for the shard manager.
type ManagerConfig struct {
	RootDir string
	// TermRegistryMergeConfig configures the global term registry.
	// If nil, DefaultMergeConfig() is used.
	TermRegistryMergeConfig *segment.MergeConfig

	// NodeResources overrides auto-detection of CPU and memory.
	// Leave zero to auto-detect from the OS (recommended).
	// The coordinator can push per-node overrides by populating this field.
	NodeResources segment.NodeResourceProfile
}

// Note: Migrated from pebble to segment-based storage

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

	// Global term registry shared by all shards (reduces goroutines and memory)
	globalRegistry segment.TermRegistry
	registryOnce   sync.Once
	registryErr    error
	registryConfig *segment.MergeConfig

	// nodeConfig holds auto-tuned (or coordinator-overridden) resource configuration.
	nodeConfig    segment.NodeConfig
	nodeResources segment.NodeResourceProfile // kept for re-tune on Sync

	loadOnce sync.Once
	loadErr  error
}

// NewManager builds a Manager rooted in the given directory.
func NewManager(rootDir string) *Manager {
	return NewManagerWithConfig(ManagerConfig{RootDir: rootDir})
}

// NewManagerWithConfig builds a Manager with the given configuration.
// Node resources (CPU, memory) are auto-detected unless overridden in config.NodeResources.
func NewManagerWithConfig(config ManagerConfig) *Manager {
	nodeCfg := segment.TuneForNode(config.NodeResources)
	log.Printf("[autotune] cpus=%d mem=%dMB shards=%d → %s",
		config.NodeResources.CPUCount, config.NodeResources.MemoryLimitMB,
		config.NodeResources.ShardCount, nodeCfg)

	return &Manager{
		rootDir:        config.RootDir,
		shards:         make(map[string]*Shard),
		registryConfig: config.TermRegistryMergeConfig,
		nodeConfig:     nodeCfg,
		nodeResources:  config.NodeResources,
	}
}

// NodeConfig returns the auto-tuned resource configuration for this node.
// Callers creating ShardWorkers should use NodeConfig().WorkerMaxWorkers,
// WorkerMaxBatchSize, WorkerQueueCapacity to build a ShardWorkerConfig.
func (m *Manager) NodeConfig() segment.NodeConfig {
	return m.nodeConfig
}

// ensureGlobalRegistry creates the global term registry if not already created.
func (m *Manager) ensureGlobalRegistry() error {
	m.registryOnce.Do(func() {
		registryDir := filepath.Join(m.rootDir, "_global_registry")
		if err := os.MkdirAll(registryDir, 0o755); err != nil {
			m.registryErr = fmt.Errorf("create global registry dir: %w", err)
			return
		}

		registry, err := segment.NewPebbleTermRegistry(segment.PebbleRegistryConfig{
			DataDir: registryDir,
		})
		if err != nil {
			m.registryErr = fmt.Errorf("create global registry: %w", err)
			return
		}
		m.globalRegistry = registry
	})
	return m.registryErr
}

// Sync ensures that every assigned shard is available locally.
// On the first call with assignments, re-tunes node config using the real shard count
// (total shards assigned to this node across all indices).
func (m *Manager) Sync(assignments []Assignment) error {
	m.loadOnce.Do(func() {
		m.loadErr = m.loadExistingShards()
	})
	if m.loadErr != nil {
		return m.loadErr
	}

	// Re-tune with real shard count if it differs from the initial guess.
	// This happens once: the first Sync after startup tells us how many shards
	// the coordinator assigned to this node (across all indices).
	if len(assignments) > 0 {
		m.retune(len(assignments))
	}

	for _, assignment := range assignments {
		if err := m.ensureLocalShard(assignment); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) ensureLocalShard(assignment Assignment) error {
	// Ensure global registry exists
	if err := m.ensureGlobalRegistry(); err != nil {
		return err
	}

	if existing, ok := m.GetShard(assignment.ID); ok {
		if err := writeManifest(m.shardPath(assignment.ID), assignment); err != nil {
			return fmt.Errorf("update manifest for shard %s: %w", assignment.ID, err)
		}

		m.mu.Lock()
		existing.Info = assignment
		// Reopen segment manager if it's closed (e.g., after restart)
		if existing.Segments == nil {
			segmentMgr, err := segment.NewManager(m.segmentConfig(m.shardPath(assignment.ID)))
			if err != nil {
				m.mu.Unlock()
				return fmt.Errorf("reopen segment manager for shard %s: %w", assignment.ID, err)
			}
			existing.Segments = segmentMgr
		}
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

	segmentMgr, err := segment.NewManager(m.segmentConfig(shardPath))
	if err != nil {
		return fmt.Errorf("open segment manager for shard %s: %w", assignment.ID, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[assignment.ID] = &Shard{ID: assignment.ID, Segments: segmentMgr, Info: assignment}
	return nil
}

// retune recalculates node config with the real shard count.
// Updates nodeConfig and propagates new flush thresholds to already-opened shards.
func (m *Manager) retune(shardCount int) {
	profile := m.nodeResources
	if profile.ShardCount == shardCount {
		return // already tuned with correct count
	}
	profile.ShardCount = shardCount
	newCfg := segment.TuneForNode(profile)
	log.Printf("[autotune] retune: shards=%d → %s", shardCount, newCfg)
	m.nodeConfig = newCfg

	// Propagate to already-opened segment managers (from loadExistingShards).
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, shard := range m.shards {
		if shard.Segments != nil {
			shard.Segments.UpdateFlushThresholds(newCfg.FlushThresholdBytes, newCfg.FlushThreshold)
		}
	}
}

// segmentConfig builds a segment.Config for a shard using auto-tuned node config.
func (m *Manager) segmentConfig(dataDir string) segment.Config {
	cfg := m.nodeConfig.SegmentCfg()
	cfg.DataDir = dataDir
	cfg.TermRegistry = m.globalRegistry
	return cfg
}

func (m *Manager) shardPath(id string) string {
	return filepath.Join(m.rootDir, id)
}

func (m *Manager) replicateShard(assignment Assignment) {
	// TODO: implement replication pipeline (snapshot, copy segments, validation, open segment manager, mark ready).
}

func (m *Manager) existsLocal(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.shards[id]
	return ok
}

// Close releases all segment managers managed by the shard manager.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for id, shard := range m.shards {
		if shard.Segments == nil {
			continue
		}
		if err := shard.Segments.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close shard %s: %w", id, err)
		}
	}
	m.shards = make(map[string]*Shard)

	// Close global registry after all shards are closed
	if m.globalRegistry != nil {
		if err := m.globalRegistry.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close global registry: %w", err)
		}
		m.globalRegistry = nil
	}

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

// UnloadIndex closes and removes all shards belonging to the specified index.
// It also deletes the shard directories from disk.
func (m *Manager) UnloadIndex(indexID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	var unloadedCount int

	// Find and remove all shards for this index
	for id, shard := range m.shards {
		if shard.Info.IndexID != indexID {
			continue
		}

		// Close the segment manager
		if shard.Segments != nil {
			if err := shard.Segments.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close shard %s: %w", id, err)
			}
		}

		// Remove from memory
		delete(m.shards, id)

		// Delete the shard directory
		shardPath := m.shardPath(id)
		if err := os.RemoveAll(shardPath); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("remove shard directory %s: %w", shardPath, err)
		}

		unloadedCount++
	}

	return firstErr
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

	// Initialize global registry first
	if err := m.ensureGlobalRegistry(); err != nil {
		return err
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
		// Skip the global registry directory
		if id == "_global_registry" {
			continue
		}
		shardPath := m.shardPath(id)

		assignment, err := readManifest(shardPath)
		if err != nil {
			continue
		}

		segmentMgr, err := segment.NewManager(m.segmentConfig(shardPath))
		if err != nil {
			return fmt.Errorf("open existing shard %s: %w", id, err)
		}

		m.mu.Lock()
		m.shards[id] = &Shard{ID: id, Segments: segmentMgr, Info: assignment}
		m.mu.Unlock()
	}

	return nil
}
