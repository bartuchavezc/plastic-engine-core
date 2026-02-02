package mappings

import (
	"context"
	"sync"
)

// CachedRepository wraps a Repository with an in-memory cache to reduce
// database queries on the hot path. Cache is invalidated on write operations.
type CachedRepository struct {
	underlying Repository
	mu         sync.RWMutex
	cache      map[string]cachedMapping
}

type cachedMapping struct {
	mapping Mapping
	version int
}

// NewCachedRepository creates a new cached wrapper around the given repository.
func NewCachedRepository(underlying Repository) *CachedRepository {
	return &CachedRepository{
		underlying: underlying,
		cache:      make(map[string]cachedMapping),
	}
}

// GetMapping retrieves the mapping for an index, using the cache when available.
func (r *CachedRepository) GetMapping(ctx context.Context, indexID string) (Mapping, error) {
	// Fast path: read lock
	r.mu.RLock()
	if cached, ok := r.cache[indexID]; ok {
		r.mu.RUnlock()
		return cached.mapping, nil
	}
	r.mu.RUnlock()

	// Slow path: fetch from underlying repository and cache
	mapping, err := r.underlying.GetMapping(ctx, indexID)
	if err != nil {
		return Mapping{}, err
	}

	r.mu.Lock()
	r.cache[indexID] = cachedMapping{mapping: mapping, version: mapping.Version}
	r.mu.Unlock()

	return mapping, nil
}

// SaveMapping creates or updates a complete mapping and invalidates the cache.
func (r *CachedRepository) SaveMapping(ctx context.Context, mapping Mapping) error {
	if err := r.underlying.SaveMapping(ctx, mapping); err != nil {
		return err
	}
	r.Invalidate(mapping.IndexID)
	return nil
}

// AddFields adds new fields to an existing mapping and invalidates the cache.
func (r *CachedRepository) AddFields(ctx context.Context, indexID string, fields []Field) error {
	if err := r.underlying.AddFields(ctx, indexID, fields); err != nil {
		return err
	}
	r.Invalidate(indexID)
	return nil
}

// UpdateDynamic changes the dynamic mode for a mapping and invalidates the cache.
func (r *CachedRepository) UpdateDynamic(ctx context.Context, indexID string, mode DynamicMode) error {
	if err := r.underlying.UpdateDynamic(ctx, indexID, mode); err != nil {
		return err
	}
	r.Invalidate(indexID)
	return nil
}

// Invalidate removes a specific index from the cache.
func (r *CachedRepository) Invalidate(indexID string) {
	r.mu.Lock()
	delete(r.cache, indexID)
	r.mu.Unlock()
}

// InvalidateAll clears the entire cache.
func (r *CachedRepository) InvalidateAll() {
	r.mu.Lock()
	r.cache = make(map[string]cachedMapping)
	r.mu.Unlock()
}

// CacheSize returns the current number of cached mappings.
func (r *CachedRepository) CacheSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.cache)
}
