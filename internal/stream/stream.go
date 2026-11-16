package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"

	"github.com/barnowlsnest/stratus/internal/storage"
)

type (
	Info struct {
		FirstID       uint64
		LastID        uint64
		CachedRecords uint64
		FSRecords     uint64
	}

	Stream struct {
		subscriberBuffer int
		// cacheSize is how many of the newest records Start and ReconcileCache
		// load. Zero loads the whole WAL range.
		cacheSize int
		storage   *storage.Storage
		lru       *lru.LRU[storage.Record]
		stop      context.CancelFunc
		started   atomic.Bool
		// lastCached is the highest ID the cache-fill loop has put into the LRU.
		// The loop receives IDs in order with no gaps, so every ID up to it has
		// been cached; reads stop here so they never miss on a fresh record.
		lastCached      atomic.Uint64
		mu              sync.RWMutex
		newDataReadySig chan struct{}
		startedChan     chan struct{}
	}

	// Item is a single stream entry.
	Item struct {
		// ID is the LSN assigned by the WAL. Zero on the write path; populated on reads.
		ID       uint64
		DedupKey uint64
		RawBytes []byte
	}

	Range struct {
		first uint64
		last  uint64
	}

	AddResult struct {
		StreamRange Range
		AddedRange  Range
		DedupCount  uint64
	}

	DelResult struct {
		StreamRange  Range
		DeletedRange Range
	}

	Option func(*Stream)
)

func (r Range) First() uint64 { return r.first }

func (r Range) Last() uint64 { return r.last }

func NewItem(dedupKey uint64, rawBytes []byte) *Item {
	return &Item{
		DedupKey: dedupKey,
		RawBytes: rawBytes,
	}
}

func WithStorage(store *storage.Storage) Option {
	return func(s *Stream) {
		s.storage = store
	}
}

func WithCache(cache *lru.LRU[storage.Record]) Option {
	return func(s *Stream) {
		s.lru = cache
	}
}

func WithSubscriberBuffer(buffer int) Option {
	return func(s *Stream) {
		s.subscriberBuffer = buffer
	}
}

// WithCacheSize sets how many of the newest records Start and ReconcileCache
// load into the cache. Pass the LRU's capacity: older records would only be
// evicted again by the newer ones. Zero or less loads the whole WAL range.
func WithCacheSize(size int) Option {
	return func(s *Stream) {
		s.cacheSize = size
	}
}

func New(opts ...Option) (*Stream, error) {
	s := &Stream{
		startedChan:     make(chan struct{}),
		newDataReadySig: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}

	if err := s.validate(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Stream) DataReady() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.newDataReadySig
}

func (s *Stream) Start(ctx context.Context) error {
	if s.IsStarted() {
		return ErrAlreadyStarted
	}

	pCtx, pCancel := context.WithCancel(ctx)
	s.stop = pCancel

	first, last := s.storage.Range()

	if first > 0 && last > 0 {
		if err := s.fetchAndCacheRecords(pCtx, first, last); err != nil {
			return err
		}
	}
	s.lastCached.Store(last)

	records, err := s.storage.Subscribe(pCtx, last, s.subscriberBuffer)
	if err != nil {
		return err
	}

	s.mu.Lock()
	close(s.startedChan)
	s.mu.Unlock()
	s.started.Store(true)
	for r := range records {
		s.cacheRecord(r)
		s.cacheWaiting(records)
		s.notify()
	}

	s.started.Swap(false)
	s.notify()

	return nil
}

func (s *Stream) Stop(ctx context.Context) {
	if !s.IsStarted() {
		return
	}

	s.stop()
	s.storage.WaitSubscribersDone(ctx)
	s.mu.Lock()
	s.startedChan = make(chan struct{})
	s.mu.Unlock()
	s.started.Swap(false)
}

func (s *Stream) WaitForStart(ctx context.Context, timeout time.Duration) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-tCtx.Done():
		return tCtx.Err()
	case <-s.startedSignal():
		return nil
	}
}

func (s *Stream) IsStarted() bool {
	return s.started.Load()
}

func (s *Stream) Add(ctx context.Context, records []*storage.Record) (AddResult, error) {
	res, err := s.storage.Write(ctx, records)
	if err != nil {
		return AddResult{}, err
	}

	if res.DuplicatesCount == uint64(len(records)) {
		return AddResult{}, ErrAllSkipped
	}

	addedLast := res.IDs[len(res.IDs)-1]
	s.waitCached(ctx, addedLast)

	result := AddResult{
		StreamRange: s.streamRange(),
		AddedRange: Range{
			first: res.IDs[0],
			last:  addedLast,
		},
		DedupCount: res.DuplicatesCount,
	}

	return result, nil
}

// cacheRecord puts one record from the cache-fill loop into the LRU and moves
// lastCached up to it.
func (s *Stream) cacheRecord(r *storage.Record) {
	s.lru.Put(r.ID, r)
	s.lastCached.Store(r.ID)
}

// cacheWaiting caches every record already waiting in records without blocking,
// so one notify covers the whole batch. Every notify wakes every ReadOffset tail
// and each wake becomes one gRPC message per tail, so with many tails one notify
// per record made socket writes and the notify lock the bottleneck.
func (s *Stream) cacheWaiting(records <-chan *storage.Record) {
	for {
		select {
		case r, ok := <-records:
			if !ok {
				return
			}
			s.cacheRecord(r)
		default:
			return
		}
	}
}

// waitCached blocks until the cache-fill loop has put id into the cache, ctx
// ends, or the stream stops. The records are already durable in every case, so
// giving up early is not an error: Add still reports what it wrote.
func (s *Stream) waitCached(ctx context.Context, id uint64) {
	for {
		ready := s.DataReady()
		if s.lastCached.Load() >= id || !s.IsStarted() {
			return
		}

		select {
		case <-ready:
		case <-ctx.Done():
			return
		}
	}
}

// streamRange is the range reads can return: from the WAL's first ID up to the
// last ID in the cache.
func (s *Stream) streamRange() Range {
	first, _ := s.storage.Range()

	return Range{first: first, last: s.lastCached.Load()}
}

func (s *Stream) Info() Info {
	firstID, walLastID := s.storage.Range()
	visible := s.streamRange()
	cacheEntriesCount := max(s.lru.Len(), 0)

	return Info{
		FirstID:       visible.first,
		LastID:        visible.last,
		FSRecords:     walLastID - firstID + 1,
		CachedRecords: uint64(cacheEntriesCount),
	}
}

func (s *Stream) Get(ctx context.Context, fromID, toID uint64) ([]*storage.Record, error) {
	first, last, err := s.storage.ClampRange(fromID, toID, s.lastCached.Load())
	if err != nil {
		return nil, err
	}

	records := make([]*storage.Record, 0, last-first+1)
	for id := first; id <= last; id++ {
		r, err := s.lru.Get(id)
		switch {
		case err == nil:
			records = append(records, r)
		case errors.Is(err, lru.ErrCacheMiss):
			// Each WAL reader scans its segment from the start, so reading the
			// misses one ID at a time cost one segment scan per record. Read the
			// rest of the range in a single pass instead.
			return s.readAndCacheRange(ctx, records, id, last)
		default:
			return nil, err
		}
	}

	return records, nil
}

func (s *Stream) Del(ctx context.Context, upTo uint64) (DelResult, error) {
	oldFirst, oldLast := s.storage.Range()
	if err := s.storage.Del(ctx, upTo); err != nil {
		return DelResult{}, err
	}

	newFirst, _ := s.storage.Range()
	evictBelow := newFirst
	if newFirst == 0 {
		evictBelow = oldLast + 1
	}

	result := DelResult{
		StreamRange: s.streamRange(),
	}
	if evictBelow <= oldFirst {
		return result, nil
	}

	result.DeletedRange = Range{first: oldFirst, last: evictBelow - 1}

	keys := make([]uint64, 0, evictBelow-oldFirst)
	for id := oldFirst; id < evictBelow; id++ {
		keys = append(keys, id)
	}

	s.lru.Delete(keys...)
	s.notify()

	return result, nil
}

// ReconcileCache rebuilds the in-memory cache so it mirrors what is currently
// on disk. It empties the cache before repopulating it from the persisted
// range, guaranteeing that no entries deleted from disk survive as stale cache
// entries afterward.
func (s *Stream) ReconcileCache(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.lru.Clear()
		firstID, lastID := s.storage.Range()
		return s.fetchAndCacheRecords(ctx, firstID, lastID)
	}
}

// readAndCacheRange reads [fromID, toID] from the WAL in one pass, caches each
// record and appends it to records.
func (s *Stream) readAndCacheRange(ctx context.Context, records []*storage.Record, fromID, toID uint64) ([]*storage.Record, error) {
	err := s.storage.ReadEach(ctx, fromID, toID, func(r *storage.Record) error {
		s.lru.Put(r.ID, r)
		records = append(records, r)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

// fetchAndCacheRecords loads the newest cacheSize records of [fromID, toID]
// into the cache. Loading more would only have the newer records evict them.
func (s *Stream) fetchAndCacheRecords(ctx context.Context, fromID, toID uint64) error {
	if s.cacheSize > 0 && toID-fromID+1 > uint64(s.cacheSize) {
		fromID = toID - uint64(s.cacheSize) + 1
	}

	return s.storage.ReadEach(ctx, fromID, toID, func(r *storage.Record) error {
		s.lru.Put(r.ID, r)
		return nil
	})
}

func (s *Stream) validate() error {
	switch {
	case s.storage == nil:
		return ErrNilStorage
	case s.lru == nil:
		return ErrNilCache
	default:
		return nil
	}
}

func (s *Stream) notify() {
	s.mu.Lock()
	prev := s.newDataReadySig
	s.newDataReadySig = make(chan struct{})
	s.mu.Unlock()
	close(prev)
}

func (s *Stream) startedSignal() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startedChan
}
