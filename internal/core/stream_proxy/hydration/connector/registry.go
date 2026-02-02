package connector

import (
	"fmt"
	"sync"

	"plastic-engine-core/internal/core/stream_proxy/hydration"
)

// Factory creates a connector from settings.
type Factory func(settings map[string]string) (hydration.Connector, error)

// Registry manages connector factories.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// NewRegistry creates a registry with default connectors registered.
func NewRegistry() *Registry {
	r := &Registry{
		factories: make(map[string]Factory),
	}

	// Register built-in connectors
	r.Register(HTTPConnectorName, func(settings map[string]string) (hydration.Connector, error) {
		return NewHTTPConnectorFromSettings(settings), nil
	})

	return r
}

// Register adds a connector factory.
func (r *Registry) Register(name string, factory Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

// Create instantiates a connector by name.
func (r *Registry) Create(name string, settings map[string]string) (hydration.Connector, error) {
	r.mu.RLock()
	factory, ok := r.factories[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", hydration.ErrConnectorNotFound, name)
	}

	return factory(settings)
}

// Available returns the list of registered connector names.
func (r *Registry) Available() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.factories))
	for name := range r.factories {
		names = append(names, name)
	}
	return names
}

// DefaultRegistry is the global connector registry.
var DefaultRegistry = NewRegistry()

// RegisterConnector adds a connector factory to the default registry.
func RegisterConnector(name string, factory Factory) {
	DefaultRegistry.Register(name, factory)
}

// CreateConnector instantiates a connector from the default registry.
func CreateConnector(name string, settings map[string]string) (hydration.Connector, error) {
	return DefaultRegistry.Create(name, settings)
}

