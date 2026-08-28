package rt

import (
	"maps"
	"sync/atomic"
)

// snapshot is an immutable point-in-time view of all cached feeds.
// Once published via atomic.Pointer.Store it is never mutated.
type snapshot struct {
	feeds map[string][]Entity
}

// Cache exposes lock-free reads of GTFS-RT entity slices via atomic snapshots.
//
// Writers build a new snapshot and swap the pointer; readers Load the pointer
// once and traverse the map with no mutex on the hot path.
type Cache struct {
	ptr atomic.Pointer[snapshot]
}

// NewCache returns an empty RT entity cache.
func NewCache() *Cache {
	c := &Cache{}
	c.ptr.Store(&snapshot{feeds: make(map[string][]Entity)})
	return c
}

// Get returns the entity slice for key. The slice is read-only — callers must
// not mutate it. The second value is false if the key has not been loaded yet.
func (c *Cache) Get(key string) ([]Entity, bool) {
	entities, ok := c.ptr.Load().feeds[key]
	return entities, ok
}

// Len returns how many feed keys are in the current snapshot.
func (c *Cache) Len() int {
	return len(c.ptr.Load().feeds)
}

// Merge atomically publishes updates onto the current snapshot. Keys in updates
// replace or add entries; all other keys are carried forward unchanged.
func (c *Cache) Merge(updates map[string][]Entity) {
	if len(updates) == 0 {
		return
	}

	for {
		old := c.ptr.Load()
		nextFeeds := maps.Clone(old.feeds)

		for key, entities := range updates {
			dup := make([]Entity, len(entities))
			copy(dup, entities)
			nextFeeds[key] = dup
		}

		next := &snapshot{feeds: nextFeeds}
		if c.ptr.CompareAndSwap(old, next) {
			return
		}
	}
}
