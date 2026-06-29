package stream

import (
	"context"
	"testing"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/preloader"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/stretchr/testify/suite"
)

type StreamTestSuite struct {
	suite.Suite
}

func TestStreamSuite(t *testing.T) {
	suite.Run(t, new(StreamTestSuite))
}

func (s *StreamTestSuite) newStream() (*Stream, error) {
	dir := s.T().TempDir()
	w, r, err := wal.Open(dir,
		wal.WithBatchSize(8),
		wal.WithSyncPolicy(wal.SyncBatched),
		wal.WithFlushInterval(5*time.Second),
	)
	if err != nil {
		return nil, err
	}

	d := dedup.New(time.Minute)

	stor, err := storage.New(
		storage.WithWAL(w),
		storage.WithMaxReadBatchSize(8),
		storage.WithDeduplicator(d),
	)
	if err != nil {
		return nil, err
	}

	p, err := preloader.New(
		preloader.WithStorage(stor),
		preloader.WithCache(lru.New[storage.Record](128)),
	)
	if err != nil {
		return nil, err
	}

	go func() {
		_ = p.Start(s.T().Context(), r.FirstLSN, r.LastLSN)
	}()

	if err := p.WaitStarted(s.T().Context(), 5*time.Second); err != nil {
		return nil, err
	}

	in, err := ingester.New(ingester.WithStorage(stor))
	if err != nil {
		return nil, err
	}

	c, err := p.Cache()
	if err != nil {
		return nil, err
	}

	return New(WithIngester(in), WithCache(c))
}

func (s *StreamTestSuite) Test() {
	stream, err := s.newStream()
	s.Require().NoError(err)

	ctx := context.Background()
	firstA, lastA, err := stream.Add(ctx, &Item{DedupKey: 1, RawBytes: []byte(`{"op":"set","k":"a"}`)})
	s.Require().NoError(err)
	s.Require().Equal(firstA, lastA)

	firstB, lastB, err := stream.Add(ctx, &Item{DedupKey: 2, RawBytes: []byte(`{"op":"set","k":"b"}`)})
	s.Require().NoError(err)
	s.Require().Equal(firstB, lastB)

	items, err := stream.Range(ctx, firstA, lastB)
	s.Require().NoError(err)
	s.Require().Len(items, 2)
	s.Require().Equal([]byte(`{"op":"set","k":"a"}`), items[0].RawBytes)
	s.Require().Equal([]byte(`{"op":"set","k":"b"}`), items[1].RawBytes)
}

func (s *StreamTestSuite) TestAddReturnsSingleIDAsRange() {
	stream, err := s.newStream()
	s.Require().NoError(err)

	ctx := context.Background()

	first, last, err := stream.Add(ctx, NewItem(101, []byte(`{"op":"set","k":"x"}`)))
	s.Require().NoError(err)
	s.Require().Equal(first, last)

	actual, err := stream.Read(ctx, last)
	s.Require().NoError(err)
	s.Require().Equal(last, actual.ID)
}

func (s *StreamTestSuite) TestAddNReturnsBatchRange() {
	stream, err := s.newStream()
	s.Require().NoError(err)

	ctx := context.Background()

	input := []*Item{
		NewItem(201, []byte(`{"op":"set","k":"a"}`)),
		NewItem(202, []byte(`{"op":"set","k":"b"}`)),
		NewItem(203, []byte(`{"op":"set","k":"c"}`)),
	}
	first, last, err := stream.AddN(ctx, input...)
	s.Require().NoError(err)

	expectedSpan := uint64(2) // 3 contiguous LSNs
	s.Require().Equal(expectedSpan, last-first)

	actual, err := stream.Range(ctx, first, last)
	s.Require().NoError(err)
	s.Require().Len(actual, 3)
	s.Require().Equal(first, actual[0].ID)
	s.Require().Equal(last, actual[2].ID)
}
