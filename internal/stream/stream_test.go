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

	w, _, err := wal.Open(s.T().TempDir())
	s.Require().NoError(err)

	st, err := storage.New(storage.WithWAL(w), storage.WithDeduplicator(dedup.New(0)))
	s.Require().NoError(err)

	if preload > 0 {
		_, err = st.Write(context.Background(), s.newRecords(preload))
		s.Require().NoError(err)
	}

	cache := lru.New[storage.Record](capacity)
	str, err := stream.New(
		stream.WithStorage(st),
		stream.WithCache(cache),
		stream.WithCacheSize(cacheSize),
		stream.WithSubscriberBuffer(1),
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
			records, getErr := str.stream.Get(context.Background(), tc.fromID, tc.toID)
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
// payloads that were written, and are cached again.
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

			records, err := str.stream.Get(context.Background(), tc.fromID, tc.toID)
			s.Require().NoError(err)

			actual := make([]uint64, 0, len(records))
			for _, r := range records {
				actual = append(actual, r.ID)

				expected, readErr := str.storage.Read(context.Background(), r.ID, r.ID)
				s.Require().NoError(readErr)
				s.Equal(expected[0].Bytes, r.Bytes)

				_, cacheErr := str.cache.Get(r.ID)
				s.NoError(cacheErr, "record %d must be cached after Get", r.ID)
			}
			s.Equal(tc.expectedIDs, actual)
		})
	}
}
