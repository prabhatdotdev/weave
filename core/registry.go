// Copyright 2025 PrabhatDotDev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"sort"
	"sync"
)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]BrokerFactory)
)

// Register registers a broker factory for a backend name.
func Register(name string, factory BrokerFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[name]; exists {
		panic("weave: broker already registered: " + name)
	}
	registry[name] = factory
}

// New creates a new MessageBroker using the backend specified in config.
func New(config *Config) (MessageBroker, error) {
	if config == nil {
		config = DefaultConfig()
	}

	registryMu.RLock()
	factory, exists := registry[config.Backend]
	registryMu.RUnlock()

	if !exists {
		return nil, &ErrUnknownBackend{Backend: config.Backend}
	}

	return factory(config)
}

// NewWithBackend creates a new MessageBroker with explicit backend name.
func NewWithBackend(backend string, config *Config) (MessageBroker, error) {
	if config == nil {
		config = DefaultConfig()
	}
	config.Backend = backend
	return New(config)
}

// AvailableBackends returns a sorted list of registered backend names.
func AvailableBackends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	backends := make([]string, 0, len(registry))
	for name := range registry {
		backends = append(backends, name)
	}
	sort.Strings(backends)
	return backends
}

// IsBackendAvailable checks if a backend is registered.
func IsBackendAvailable(name string) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	_, exists := registry[name]
	return exists
}

// MustNew creates a new MessageBroker or panics if creation fails.
func MustNew(config *Config) MessageBroker {
	broker, err := New(config)
	if err != nil {
		panic("weave: failed to create broker: " + err.Error())
	}
	return broker
}
