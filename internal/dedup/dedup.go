package dedup

import (
	"sync"
	"time"
)

type Deduplicator struct {
	ttl        time.Duration
	recentKeys map[uint64]time.Time
	mux        sync.Mutex
}

func New(ttl time.Duration) *Deduplicator {
	return &Deduplicator{
		ttl:        ttl,
		recentKeys: make(map[uint64]time.Time),
	}
}

func (d *Deduplicator) Try(key uint64) error {
	d.mux.Lock()
	defer d.mux.Unlock()

	if _, exists := d.recentKeys[key]; exists {
		return ErrDuplicateChunk
	}

	d.recentKeys[key] = time.Now()
	time.AfterFunc(d.ttl, func() {
		d.mux.Lock()
		defer d.mux.Unlock()
		delete(d.recentKeys, key)
	})

	return nil
}
