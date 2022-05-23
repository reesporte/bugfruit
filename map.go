package bugfruit

import "sync"

// muMap is a map that has a RWMutex on it.
type muMap struct {
	data map[string]*datum
	mu   sync.RWMutex
}

func newMuMap() muMap {
	return muMap{
		data: make(map[string]*datum),
	}
}

// Store sets a key/value pair in the map.
func (m *muMap) Store(key string, value *datum) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Load returns a key/value pair from the map,
// if it can, and whether the key exists in the map.
func (m *muMap) Load(key string) (*datum, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[key]
	return val, ok
}

// Load returns a key/value pair from the map,
// if it can, and whether the key exists in the map.
// It deletes the key from the map if it existed.
func (m *muMap) LoadAndDelete(key string) (*datum, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.data[key]
	if ok {
		delete(m.data, key)
	}
	return val, ok
}

// RLock locks muMap for reading.
func (m *muMap) RLock() {
	m.mu.RLock()
}

// RUnlock unlocks muMap for reading.
func (m *muMap) RUnlock() {
	m.mu.RUnlock()
}
