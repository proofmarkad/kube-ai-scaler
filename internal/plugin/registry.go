package plugin

import (
	"fmt"
	"sync"
)

var (
	mu       sync.RWMutex
	registry = make(map[string]func() SignalPlugin)
)

// Register adds a plugin factory to the registry.
// Called in init() by each plugin package.
func Register(name string, factory func() SignalPlugin) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("signal plugin %q already registered", name))
	}
	registry[name] = factory
}

// Get returns a new instance of the named plugin.
func Get(name string) (SignalPlugin, error) {
	mu.RLock()
	defer mu.RUnlock()
	factory, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown signal plugin %q; available: %v", name, List())
	}
	return factory(), nil
}

// List returns all registered plugin names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

// ResetForTesting clears the registry. Only for use in tests.
func ResetForTesting() {
	mu.Lock()
	defer mu.Unlock()
	registry = make(map[string]func() SignalPlugin)
}
