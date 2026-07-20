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
		storage          *storage.Storage
		lru              *lru.LRU[storage.Record]
		stop             context.CancelFunc
		started          atomic.Bool
		mu               sync.RWMutex
		newDataReadySig  chan struct{}
		startedChan      chan struct{}
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

func WithStorage(storage *storage.Storage) Option {
	return func(s *Stream) {
		s.storage = storage
	}
}

func WithCache(lru *lru.LRU[storage.Record]) Option {
	return func(s *Stream) {
		s.lru = lru
	}
}

func WithSubscriberBuffer(buffer int) Option {
	return func(s *Stream) {
		s.subscriberBuffer = buffer
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

	records, err := s.storage.Subscribe(pCtx, last, s.subscriberBuffer)
	if err != nil {
		return err
	}

	s.mu.Lock()
	close(s.startedChan)
	s.mu.Unlock()
	s.started.Store(true)
	for r := range records {
		s.lru.Put(r.ID, r)
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

	first, last := s.storage.Range()
	result := AddResult{
		StreamRange: Range{
			first: first,
			last:  last,
		},
		AddedRange: Range{
			first: res.IDs[0],
			last:  res.IDs[len(res.IDs)-1],
		},
		DedupCount: res.DuplicatesCount,
	}

	return result, nil
}

func (s *Stream) Info() Info {
	firstID, lastID := s.storage.Range()
	cacheEntriesCount := s.lru.Len()

	return Info{
		FirstID:       firstID,
		LastID:        lastID,
		FSRecords:     lastID - firstID + 1,
		CachedRecords: uint64(cacheEntriesCount),
	}
}

func (s *Stream) Get(ctx context.Context, fromID, toID uint64) ([]*storage.Record, error) {
	first, last, err := s.storage.ClampRange(fromID, toID)
	if err != nil {
		return nil, err
	}

	records := make([]*storage.Record, 0, last-first+1)
	for id := first; id <= last; id++ {
		r, err := s.lazyLoadRecord(ctx, id)
		if err != nil {
			return nil, err
		}

		records = append(records, r)
	}

	return records, nil
}

func (s *Stream) Del(ctx context.Context, upTo uint64) (DelResult, error) {
	oldFirst, oldLast := s.storage.Range()
	if err := s.storage.Del(ctx, upTo); err != nil {
		return DelResult{}, err
	}

	newFirst, newLast := s.storage.Range()
	evictBelow := newFirst
	if newFirst == 0 {
		evictBelow = oldLast + 1
	}

	result := DelResult{
		StreamRange: Range{first: newFirst, last: newLast},
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

func (s *Stream) ReCache(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		firstID, lastID := s.storage.Range()
		return s.fetchAndCacheRecords(ctx, firstID, lastID)
	}
}

func (s *Stream) lazyLoadRecord(ctx context.Context, id uint64) (*storage.Record, error) {
	record, err := s.lru.Get(id)
	if err != nil {
		switch {
		case errors.Is(err, lru.ErrCacheMiss):
			return s.readAndCacheRecord(ctx, id)
		default:
			return nil, err
		}
	}

	return record, nil
}

func (s *Stream) readAndCacheRecord(ctx context.Context, id uint64) (*storage.Record, error) {
	records, err := s.storage.Read(ctx, id, id)
	if err != nil {
		return nil, err
	}

	r := records[0]
	s.lru.Put(id, r)

	return r, nil
}

func (s *Stream) fetchAndCacheRecords(ctx context.Context, fromID, toID uint64) error {
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
