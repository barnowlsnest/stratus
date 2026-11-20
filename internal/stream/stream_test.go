package stream_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
)

const startTimeout = 5 * time.Second

type StreamSuite struct {
	suite.Suite
	nextKey uint64
}

func TestStreamSuite(t *testing.T) {
	suite.Run(t, new(StreamSuite))
}

// started is a running stream over a fresh WAL, with direct handles on its
// storage and cache so tests can check what reached each.
type started struct {
	stream  *stream.Stream
	storage *storage.Storage
	cache   *lru.LRU[storage.Record]
}

// newRecords builds count records with dedup keys unique within the suite.
func (s *StreamSuite) newRecords(count int) []*storage.Record {
	records := make([]*storage.Record, 0, count)
	for range count {
		s.nextKey++
		records = append(records, &storage.Record{DedupKey: s.nextKey, Bytes: fmt.Appendf(nil, "rec-%d", s.nextKey)})
	}

	return records
}

// start writes preload records straight to storage, then starts a stream over
// it and waits until the cache is warm. The stream is stopped on cleanup.
func (s *StreamSuite) start(preload int) *started {
	s.T().Helper()

	return s.startSized(preload, 1024, 0)
}

// startSized is start with an LRU of capacity and a stream that preloads the
// newest cacheSize records (zero preloads all of them).
func (s *StreamSuite) startSized(preload, capacity, cacheSize int) *started {
	s.T().Helper()

	return s.startBuffered(preload, capacity, cacheSize, 1)
}

// startBuffered is startSized with a subscriber buffer of buffer, which also
// sets how many of the newest records the stream keeps for caught-up tails.
func (s *StreamSuite) startBuffered(preload, capacity, cacheSize, buffer int) *started {
	s.T().Helper()

	return s.startWith(startConfig{preload: s.newRecords(preload), capacity: capacity, cacheSize: cacheSize, buffer: buffer})
}

// startConfig is what startWith builds a stream from; zero read limits keep the
// storage's and the stream's defaults.
type startConfig struct {
	preload      []*storage.Record
	capacity     int
	cacheSize    int
	buffer       int
	maxReadBytes int
	maxReadBatch uint64
}

// startWith writes cfg.preload straight to storage, then starts a stream over
// it as cfg describes and waits until the cache is warm.
func (s *StreamSuite) startWith(cfg startConfig) *started {
	s.T().Helper()

	w, _, err := wal.Open(s.T().TempDir())
	s.Require().NoError(err)

	st, err := storage.New(
		storage.WithWAL(w),
		storage.WithDeduplicator(dedup.New(0)),
		storage.WithMaxReadBatchSize(cfg.maxReadBatch),
	)
	s.Require().NoError(err)

	if len(cfg.preload) > 0 {
		_, err = st.Write(context.Background(), cfg.preload)
		s.Require().NoError(err)
	}

	cache := lru.New[storage.Record](cfg.capacity)
	str, err := stream.New(
		stream.WithStorage(st),
		stream.WithCache(cache),
		stream.WithCacheSize(cfg.cacheSize),
		stream.WithSubscriberBuffer(cfg.buffer),
		stream.WithMaxReadBytes(cfg.maxReadBytes),
	)
	s.Require().NoError(err)

	var wg sync.WaitGroup
	wg.Go(func() { _ = str.Start(context.Background()) })
	s.Require().NoError(str.WaitForStart(context.Background(), startTimeout))

	s.T().Cleanup(func() {
		str.Stop(context.Background())
		wg.Wait()
		_ = w.Close()
	})

	return &started{stream: str, storage: st, cache: cache}
}

// Add returns only once every record it wrote is already in the cache, and the
// stream range it reports ends at the last cached ID.
func (s *StreamSuite) TestAddReturnsOnceCached() {
	tests := []struct {
		name    string
		preload int
		batch   int
	}{
		{name: "empty stream, one record", preload: 0, batch: 1},
		{name: "empty stream, batch", preload: 0, batch: 5},
		{name: "warm stream, batch", preload: 3, batch: 5},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			str := s.start(tc.preload)

			actual, err := str.stream.Add(context.Background(), s.newRecords(tc.batch))
			s.Require().NoError(err)

			expectedLast := uint64(tc.preload + tc.batch)
			s.Equal(expectedLast, actual.AddedRange.Last())
			s.Equal(expectedLast, actual.StreamRange.Last())
			s.Equal(expectedLast, str.stream.Info().LastID)

			for id := actual.AddedRange.First(); id <= actual.AddedRange.Last(); id++ {
				_, cacheErr := str.cache.Get(id)
				s.NoError(cacheErr, "record %d must be cached when Add returns", id)
			}
		})
	}
}

// Reads stop at the last cached ID even when the WAL already holds more records.
func (s *StreamSuite) TestGetStopsAtLastCached() {
	str := s.start(5)

	// With the stream stopped, records written straight to storage reach the WAL
	// but never the cache.
	str.stream.Stop(context.Background())
	_, err := str.storage.Write(context.Background(), s.newRecords(3))
	s.Require().NoError(err)

	tests := []struct {
		expectedErr error
		name        string
		fromID      uint64
		toID        uint64
		expectedIDs []uint64
	}{
		{name: "range past the cache is clamped", fromID: 1, toID: 8, expectedIDs: []uint64{1, 2, 3, 4, 5}},
		{name: "open end stops at the cache", fromID: 4, toID: 0, expectedIDs: []uint64{4, 5}},
		{name: "range beyond the cache is out of bounds", fromID: 6, toID: 8, expectedErr: storage.ErrOutOfBounds},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			records, getErr := str.stream.Get(context.Background(), tc.fromID, tc.toID, nil)
			if tc.expectedErr != nil {
				s.Require().ErrorIs(getErr, tc.expectedErr)

				return
			}

			s.Require().NoError(getErr)
			actual := make([]uint64, 0, len(records))
			for _, r := range records {
				actual = append(actual, r.ID)
			}
			s.Equal(tc.expectedIDs, actual)
		})
	}

	info := str.stream.Info()
	s.Equal(uint64(5), info.LastID)
	s.Equal(uint64(8), info.FSRecords)
}

// When the stream has stopped, Add still writes and returns its normal result
// without waiting for a cache that will never be filled.
func (s *StreamSuite) TestAddAfterStopReturnsResult() {
	str := s.start(2)
	str.stream.Stop(context.Background())

	actual, err := str.stream.Add(context.Background(), s.newRecords(3))
	s.Require().NoError(err)
	s.Equal(uint64(3), actual.AddedRange.First())
	s.Equal(uint64(5), actual.AddedRange.Last())
	s.Equal(uint64(2), actual.StreamRange.Last())
}

// cachedIDs lists which of the IDs 1..upTo are in the cache.
func cachedIDs(cache *lru.LRU[storage.Record], upTo uint64) []uint64 {
	var ids []uint64
	for id := uint64(1); id <= upTo; id++ {
		if _, err := cache.Get(id); err == nil {
			ids = append(ids, id)
		}
	}

	return ids
}

// Start loads only the newest cacheSize records, the ones a cache of that size
// would keep anyway.
func (s *StreamSuite) TestStartLoadsNewestRecords() {
	tests := []struct {
		name        string
		preload     int
		cacheSize   int
		expectedIDs []uint64
	}{
		{name: "more records than the cache", preload: 10, cacheSize: 4, expectedIDs: []uint64{7, 8, 9, 10}},
		{name: "fewer records than the cache", preload: 3, cacheSize: 4, expectedIDs: []uint64{1, 2, 3}},
		{name: "zero size loads everything", preload: 5, cacheSize: 0, expectedIDs: []uint64{1, 2, 3, 4, 5}},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			str := s.startSized(tc.preload, 1024, tc.cacheSize)

			actual := cachedIDs(str.cache, uint64(tc.preload))
			s.Equal(tc.expectedIDs, actual)
		})
	}
}

// Records no longer in the cache are read from the WAL, in order and with the
// payloads that were written, and are not cached again. Records that were
// already cached stay cached.
func (s *StreamSuite) TestGetReadsEvictedRecords() {
	tests := []struct {
		name        string
		fromID      uint64
		toID        uint64
		expectedIDs []uint64
	}{
		{name: "all evicted", fromID: 1, toID: 3, expectedIDs: []uint64{1, 2, 3}},
		{name: "evicted then cached", fromID: 5, toID: 8, expectedIDs: []uint64{5, 6, 7, 8}},
		{name: "all cached", fromID: 8, toID: 10, expectedIDs: []uint64{8, 9, 10}},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			// Preloading 4 of 10 records caches only IDs 7..10. The LRU is large
			// enough that nothing read back is evicted.
			str := s.startSized(10, 1024, 4)

			records, err := str.stream.Get(context.Background(), tc.fromID, tc.toID, nil)
			s.Require().NoError(err)

			actual := make([]uint64, 0, len(records))
			for _, r := range records {
				actual = append(actual, r.ID)

				expected, readErr := str.storage.Read(context.Background(), r.ID, r.ID)
				s.Require().NoError(readErr)
				s.Equal(expected[0].Bytes, r.Bytes)
			}
			s.Equal(tc.expectedIDs, actual)

			// Only the preloaded IDs 7..10 are cached, whatever Get read.
			s.Equal([]uint64{7, 8, 9, 10}, cachedIDs(str.cache, 10))
		})
	}
}

// Get returns the right records, in order and after what dst already holds,
// whether they come from the newest records kept for tails, from the LRU or
// from the WAL. With a buffer of 8 the stream keeps IDs 13..20 for tails, and
// an LRU of 2 has evicted most of the older ones.
func (s *StreamSuite) TestGetAppendsFromRecentRecords() {
	str := s.startBuffered(0, 2, 0, 8)
	for range 4 {
		_, err := str.stream.Add(context.Background(), s.newRecords(5))
		s.Require().NoError(err)
	}

	prefix := &storage.Record{ID: 999}

	tests := []struct {
		name        string
		fromID      uint64
		toID        uint64
		expectedIDs []uint64
	}{
		{name: "only recent records", fromID: 15, toID: 20, expectedIDs: []uint64{999, 15, 16, 17, 18, 19, 20}},
		{name: "older and recent records", fromID: 10, toID: 14, expectedIDs: []uint64{999, 10, 11, 12, 13, 14}},
		{name: "only older records", fromID: 1, toID: 4, expectedIDs: []uint64{999, 1, 2, 3, 4}},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			records, err := str.stream.Get(context.Background(), tc.fromID, tc.toID, []*storage.Record{prefix})
			s.Require().NoError(err)
			s.Require().Same(prefix, records[0])

			actual := make([]uint64, 0, len(records))
			for _, r := range records {
				actual = append(actual, r.ID)
				if r == prefix {
					continue
				}

				expected, readErr := str.storage.Read(context.Background(), r.ID, r.ID)
				s.Require().NoError(readErr)
				s.Equal(expected[0].Bytes, r.Bytes)
			}
			s.Equal(tc.expectedIDs, actual)
		})
	}

	// A read past the newest record fails and hands dst back, so a tail waiting
	// for new records keeps its slice.
	dst := []*storage.Record{prefix}
	actual, err := str.stream.Get(context.Background(), 21, 0, dst)
	s.Require().ErrorIs(err, storage.ErrOutOfBounds)
	s.Equal(dst, actual)
}

// A watcher is woken by each change after Watch, keeps at most one pending
// wake-up so the stream never blocks on it, and is not woken after stop.
func (s *StreamSuite) TestWatch() {
	str := s.start(0)
	wake, stop := str.stream.Watch()

	received := func() bool {
		select {
		case <-wake:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	}

	tests := []struct {
		name     string
		adds     int
		stop     bool
		expected []bool
	}{
		{name: "one add wakes once", adds: 1, expected: []bool{true, false}},
		{name: "unread adds leave one wake-up", adds: 3, expected: []bool{true, false}},
		{name: "no wake-up after stop", adds: 1, stop: true, expected: []bool{false}},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			if tc.stop {
				stop()
				stop()
			}

			for range tc.adds {
				_, err := str.stream.Add(context.Background(), s.newRecords(1))
				s.Require().NoError(err)
			}

			actual := make([]bool, 0, len(tc.expected))
			for range tc.expected {
				actual = append(actual, received())
			}
			s.Equal(tc.expected, actual)
		})
	}
}

// Del removes the deleted IDs from the cache and keeps the record at upTo, both
// when the deleted range is narrower than the cache and when it is wider and
// only partly cached.
func (s *StreamSuite) TestDelEvictsDeletedRecords() {
	tests := []struct {
		name              string
		expectedCachedIDs []uint64
		preload           int
		capacity          int
		cacheSize         int
		upTo              uint64
		expectedDeleted   [2]uint64
		expectedStream    [2]uint64
	}{
		{
			// All 10 records are cached and 3 are deleted.
			name: "range narrower than the cache", preload: 10, capacity: 1024, cacheSize: 0, upTo: 4,
			expectedDeleted: [2]uint64{1, 3}, expectedStream: [2]uint64{4, 10},
			expectedCachedIDs: []uint64{4, 5, 6, 7, 8, 9, 10},
		},
		{
			// Only IDs 7..10 are cached and 8 are deleted, so 7 and 8 must go.
			name: "range wider than the cache", preload: 10, capacity: 4, cacheSize: 4, upTo: 9,
			expectedDeleted: [2]uint64{1, 8}, expectedStream: [2]uint64{9, 10},
			expectedCachedIDs: []uint64{9, 10},
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			str := s.startSized(tc.preload, tc.capacity, tc.cacheSize)

			actual, err := str.stream.Del(context.Background(), tc.upTo)
			s.Require().NoError(err)

			s.Equal(tc.expectedDeleted, [2]uint64{actual.DeletedRange.First(), actual.DeletedRange.Last()})
			s.Equal(tc.expectedStream, [2]uint64{actual.StreamRange.First(), actual.StreamRange.Last()})
			s.Equal(tc.expectedCachedIDs, cachedIDs(str.cache, uint64(tc.preload)))
			s.Equal(tc.expectedStream[0], str.stream.FirstID())
		})
	}
}

// TestFirstIDFromEmptyStart covers the empty-WAL path: firstID stays 0 until
// the first cacheBatch, then tracks deletes without consulting the WAL.
func (s *StreamSuite) TestFirstIDFromEmptyStart() {
	str := s.start(0)
	s.Equal(uint64(0), str.stream.FirstID())

	_, err := str.stream.Add(context.Background(), s.newRecords(3))
	s.Require().NoError(err)
	s.Equal(uint64(1), str.stream.FirstID())

	_, err = str.stream.Del(context.Background(), 3)
	s.Require().NoError(err)
	s.Equal(uint64(3), str.stream.FirstID())
}

// newSizedRecords builds count records of size bytes each, with dedup keys
// unique within the suite.
func (s *StreamSuite) newSizedRecords(count, size int) []*storage.Record {
	records := make([]*storage.Record, 0, count)
	for range count {
		s.nextKey++
		records = append(records, &storage.Record{DedupKey: s.nextKey, Bytes: make([]byte, size)})
	}

	return records
}

func recordIDs(records []*storage.Record) []uint64 {
	ids := make([]uint64, 0, len(records))
	for _, r := range records {
		ids = append(ids, r.ID)
	}

	return ids
}

// GetBatch stops at the max read batch size and before the record that would
// take the encoded size past maxReadBytes, whether the records come from the
// newest records kept for tails, from the LRU or from the WAL, and always
// returns the first record even when it alone is over the budget. All records
// have 10-byte payloads, which the budget counts as 30 bytes with overhead.
func (s *StreamSuite) TestGetBatchLimits() {
	const (
		recent = iota
		cached
		evicted
	)

	tests := []struct {
		name         string
		source       int
		fromID       uint64
		maxReadBatch uint64
		maxReadBytes int
		expectedIDs  []uint64
	}{
		{name: "recent, cut by bytes", source: recent, fromID: 1, maxReadBatch: 1024, maxReadBytes: 95, expectedIDs: []uint64{1, 2, 3}},
		{name: "recent, budget exactly met", source: recent, fromID: 5, maxReadBatch: 1024, maxReadBytes: 90, expectedIDs: []uint64{5, 6, 7}},
		{name: "recent, record over budget", source: recent, fromID: 1, maxReadBatch: 1024, maxReadBytes: 5, expectedIDs: []uint64{1}},
		{name: "recent, cut by count", source: recent, fromID: 1, maxReadBatch: 4, maxReadBytes: 1 << 20, expectedIDs: []uint64{1, 2, 3, 4}},
		{name: "cached, cut by bytes", source: cached, fromID: 1, maxReadBatch: 1024, maxReadBytes: 95, expectedIDs: []uint64{1, 2, 3}},
		{name: "cached, record over budget", source: cached, fromID: 1, maxReadBatch: 1024, maxReadBytes: 5, expectedIDs: []uint64{1}},
		{name: "cached, cut by count", source: cached, fromID: 1, maxReadBatch: 4, maxReadBytes: 1 << 20, expectedIDs: []uint64{1, 2, 3, 4}},
		{name: "evicted, cut by bytes", source: evicted, fromID: 1, maxReadBatch: 1024, maxReadBytes: 95, expectedIDs: []uint64{1, 2, 3}},
		{name: "evicted, record over budget", source: evicted, fromID: 1, maxReadBatch: 1024, maxReadBytes: 5, expectedIDs: []uint64{1}},
		{name: "evicted, cut by count", source: evicted, fromID: 1, maxReadBatch: 4, maxReadBytes: 1 << 20, expectedIDs: []uint64{1, 2, 3, 4}},
		{
			name: "evicted, whole range fits", source: evicted, fromID: 3, maxReadBatch: 1024, maxReadBytes: 1 << 20,
			expectedIDs: []uint64{3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			// recent keeps all 10 records for tails; cached keeps only ID 10 there
			// and the rest in the LRU; evicted preloads only IDs 7..10, so reads
			// from ID 1 go to the WAL.
			cfg := startConfig{
				preload:      s.newSizedRecords(10, 10),
				capacity:     1024,
				buffer:       64,
				maxReadBatch: tc.maxReadBatch,
				maxReadBytes: tc.maxReadBytes,
			}
			switch tc.source {
			case cached:
				cfg.buffer = 1
			case evicted:
				cfg.buffer, cfg.cacheSize = 1, 4
			}
			str := s.startWith(cfg)

			actual, err := str.stream.GetBatch(context.Background(), tc.fromID, ^uint64(0), nil)
			s.Require().NoError(err)
			s.Equal(tc.expectedIDs, recordIDs(actual))
		})
	}
}

// Get, used by ReadRange, is held to neither GetBatch limit.
func (s *StreamSuite) TestGetIgnoresBatchLimits() {
	str := s.startWith(startConfig{
		preload:      s.newSizedRecords(10, 10),
		capacity:     1024,
		buffer:       1,
		cacheSize:    4,
		maxReadBatch: 2,
		maxReadBytes: 5,
	})

	actual, err := str.stream.Get(context.Background(), 1, 10, nil)
	s.Require().NoError(err)
	s.Equal([]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, recordIDs(actual))
}
