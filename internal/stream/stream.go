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

// DefaultMaxReadBytes is the encoded-size budget of one GetBatch. gRPC encodes
// each message into a pooled buffer of 256 B, 4 KB, 16 KB, 32 KB or 1 MB, and a
// lagging tail holds one. Staying under 32 KiB keeps that buffer at 32 KB: with
// 2048 lagging tails, 1024-record messages took 1 MB buffers each and peaked at
// 3.4 GiB RSS, against 427 MiB with messages under 32 KB, at the same CPU.
const DefaultMaxReadBytes = 32_000

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
		// maxReadBytes is the largest encoded size GetBatch returns.
		maxReadBytes int
		storage      *storage.Storage
		lru          *lru.LRU[storage.Record]
		// recent holds the newest subscriberBuffer records for caught-up tails.
		recent  *recent
		stop    context.CancelFunc
		started atomic.Bool
		// lastCached is the highest ID the cache-fill loop has put into the LRU.
		// The loop receives IDs in order with no gaps, so every ID up to it has
		// been cached; reads stop here so they never miss on a fresh record.
		lastCached atomic.Uint64
		// watchers are the tails' wake channels; see Watch.
		watchers        watchers
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

// WithSubscriberBuffer sets how many new records may wait for the cache-fill
// loop, and how many of the newest records caught-up tails read without the
// LRU's lock.
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

// WithMaxReadBytes sets the largest encoded size of one GetBatch: each record's
// payload plus an upper bound on its protobuf overhead. Keep it under the
// clients' gRPC receive limit (4 MiB by default). Zero or less uses
// DefaultMaxReadBytes.
func WithMaxReadBytes(size int) Option {
	return func(s *Stream) {
		s.maxReadBytes = size
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

	s.recent = newRecent(s.subscriberBuffer)
	if s.maxReadBytes <= 0 {
		s.maxReadBytes = DefaultMaxReadBytes
	}

	return s, nil
}

// DataReady returns a channel that is closed on the next change. Every caller
// shares it, so it suits a few waiters like Add; tails use Watch.
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
	limit := max(s.subscriberBuffer, 1)
	batch := make([]*storage.Record, 0, limit)
	for r := range records {
		batch = receiveWaiting(records, append(batch, r), limit)
		s.cacheBatch(batch)
		// Drop the references so the batch does not keep evicted records alive.
		clear(batch)
		batch = batch[:0]
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

// cacheBatch puts a batch from the cache-fill loop into the LRU and the recent
// records, then moves lastCached up to its last record. Reads stop at
// lastCached, so every record a read can ask for is in place before it moves.
func (s *Stream) cacheBatch(batch []*storage.Record) {
	for _, r := range batch {
		s.lru.Put(r.ID, r)
	}

	s.recent.put(batch...)
	s.lastCached.Store(batch[len(batch)-1].ID)
}

// receiveWaiting appends every record already waiting in records to batch
// without blocking, so one notify covers the whole batch. Every notify wakes
// every ReadOffset tail and each wake becomes one gRPC message per tail, so with
// many tails one notify per record made socket writes and the notify lock the
// bottleneck. It stops once batch holds limit records, so records arriving as
// fast as they are received cannot hold back the notify or grow batch forever.
func receiveWaiting(records <-chan *storage.Record, batch []*storage.Record, limit int) []*storage.Record {
	for len(batch) < limit {
		select {
		case r, ok := <-records:
			if !ok {
				return batch
			}
			batch = append(batch, r)
		default:
			return batch
		}
	}

	return batch
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

// FirstID is the first ID reads can return. It skips the cache lock that Info
// takes to count entries, so ReadOffset can check it on every loop: with many
// tails that count was a bigger CPU cost than the cache read itself.
func (s *Stream) FirstID() uint64 {
	first, _ := s.storage.Range()

	return first
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

// Get appends the records fromID through toID to dst and returns the extended
// slice. A caller reading repeatedly, like a tail, can pass the previous result
// back as dst[:0] so each read does not allocate a new slice: with 512 tails
// those slices were 68% of the bytes the stream allocated. On an error it
// returns dst with its original length, so the caller keeps the slice: a tail
// waiting for new records gets ErrOutOfBounds on every check.
func (s *Stream) Get(ctx context.Context, fromID, toID uint64, dst []*storage.Record) ([]*storage.Record, error) {
	first, last, err := s.storage.ClampRange(fromID, toID, s.lastCached.Load())
	if err != nil {
		return dst, err
	}

	return s.getRange(ctx, first, last, dst, &readBudget{})
}

// GetBatch is Get for a read sent as one message, such as a tail's. It reads at
// most the storage's max read batch size records, and stops before the record
// that would take their encoded size past maxReadBytes; the first record is always
// returned, however large, so a reader never stalls on it. A reader far behind
// thus gets its backlog in bounded batches: in one message it held the whole
// backlog in server memory and failed past the client's receive limit.
func (s *Stream) GetBatch(ctx context.Context, fromID, toID uint64, dst []*storage.Record) ([]*storage.Record, error) {
	first, last, err := s.storage.ClampBatch(fromID, toID, s.lastCached.Load())
	if err != nil {
		return dst, err
	}

	return s.getRange(ctx, first, last, dst, &readBudget{limit: s.maxReadBytes})
}

// getRange appends the records first through last to dst, an already clamped
// range, until budget stops taking them.
func (s *Stream) getRange(
	ctx context.Context, first, last uint64, dst []*storage.Record, budget *readBudget,
) ([]*storage.Record, error) {
	// The next ID to read is always base plus the length of records. Should
	// base wrap below zero, adding the length wraps it back.
	start := len(dst)
	base := first - uint64(start)
	records := growRecords(dst, last-first+1)

	// A caught-up tail finds its whole range among the recent records.
	records = s.recent.appendRange(first, last, records)
	records, fits := budget.fit(records, start)
	next := base + uint64(len(records))
	if !fits || next > last {
		return records, nil
	}

	// One range lookup takes the cache lock once per read. A lookup per ID took
	// it once per record, and with many tails waking on the same batch the lock
	// was the main CPU cost.
	cached, err := s.lru.GetRange(next, last, records)
	switch {
	case err == nil:
		cached, _ = budget.fit(cached, len(records))

		return cached, nil
	case errors.Is(err, lru.ErrCacheMiss):
		cached, fits = budget.fit(cached, len(records))
		if !fits {
			return cached, nil
		}

		// Read the rest of the range from the first miss with one WAL reader,
		// which costs one open and one index lookup instead of one per record.
		read, readErr := s.readRange(ctx, cached, base+uint64(len(cached)), last, budget)
		if readErr != nil {
			return records[:start], readErr
		}

		return read, nil
	default:
		return records[:start], err
	}
}

// growRecords returns dst with room for n more records, so a reused dst with
// enough room is not reallocated and a new one is allocated once at its final
// size.
func growRecords(dst []*storage.Record, n uint64) []*storage.Record {
	if uint64(cap(dst)) >= uint64(len(dst))+n {
		return dst
	}

	grown := make([]*storage.Record, len(dst), uint64(len(dst))+n)
	copy(grown, dst)

	return grown
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

	// DeleteRange costs at most one pass over the cache, where listing every
	// deleted ID cost memory and lock time that grew with the deleted range.
	s.lru.DeleteRange(oldFirst, evictBelow-1)
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

// readRange reads [fromID, toID] from the WAL in one pass and appends each
// record to records. The records are not cached: a reader far behind would
// otherwise fill the cache with old records and evict the newest ones, which
// every live tail reads, sending all of them to the WAL as well.
// It stops once budget refuses a record, so a reader far behind does not read
// its whole backlog into memory.
func (s *Stream) readRange(
	ctx context.Context, records []*storage.Record, fromID, toID uint64, budget *readBudget,
) ([]*storage.Record, error) {
	err := s.storage.ReadEach(ctx, fromID, toID, func(r *storage.Record) error {
		if !budget.take(len(r.Bytes)) {
			return errBudgetSpent
		}
		records = append(records, r)

		return nil
	})
	if err != nil && !errors.Is(err, errBudgetSpent) {
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
		s.recent.put(r)

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

	s.watchers.wake()
}

func (s *Stream) startedSignal() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startedChan
}
