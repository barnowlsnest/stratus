package stream

import (
	"sync"

	"github.com/barnowlsnest/stratus/internal/storage"
)

// recent holds the newest records the stream has cached, each in the slot its
// ID maps to, so it always holds a run of consecutive IDs ending at the newest.
// Tails that are caught up read only these records, and reading them takes a
// shared lock. Reading through the LRU takes its exclusive lock, because a read
// moves each record to the front of its usage list, and with 512 tails waiting
// on that lock it was 37% of the stream's CPU.
type recent struct {
	records []*storage.Record
	mu      sync.RWMutex
}

func newRecent(size int) *recent {
	return &recent{records: make([]*storage.Record, max(size, 1))}
}

// put stores records, which must be in ID order, under one lock.
func (r *recent) put(records ...*storage.Record) {
	r.mu.Lock()
	defer r.mu.Unlock()

	size := uint64(len(r.records))
	for _, record := range records {
		r.records[record.ID%size] = record
	}
}

// appendRange appends the records fromID through toID to dst in ID order and
// returns the extended slice. It stops at the first ID it does not hold, so the
// next ID to read elsewhere is fromID plus the number of records it appended.
func (r *recent) appendRange(fromID, toID uint64, dst []*storage.Record) []*storage.Record {
	if fromID > toID {
		return dst
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	size := uint64(len(r.records))
	for id := fromID; ; id++ {
		record := r.records[id%size]
		if record == nil || record.ID != id {
			return dst
		}

		dst = append(dst, record)

		// Checked here rather than in the loop condition so a range ending at
		// the largest ID stops instead of wrapping around to zero.
		if id == toID {
			return dst
		}
	}
}
