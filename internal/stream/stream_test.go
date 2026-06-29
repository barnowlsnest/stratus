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

func (s *StreamTestSuite) Test() {
	dir := s.T().TempDir()
	w, r, err := wal.Open(dir,
		wal.WithBatchSize(8),
		wal.WithSyncPolicy(wal.SyncBatched),
		wal.WithFlushInterval(5*time.Second),
	)
	s.Require().NoError(err)
	
	d := dedup.New(time.Minute)
	
	stor, err := storage.New(
		storage.WithWAL(w),
		storage.WithMaxReadBatchSize(8),
		storage.WithDeduplicator(d),
	)
	s.Require().NoError(err)
	
	p, err := preloader.New(
		preloader.WithStorage(stor),
		preloader.WithCache(lru.New[storage.Record](128)),
	)
	s.Require().NoError(err)
	
	go func() {
		err = p.Start(s.T().Context(), r.FirstLSN, r.LastLSN)
		s.Require().NoError(err)
	}()
	
	<-time.After(1 * time.Second)
	
	in, err := ingester.New(ingester.WithStorage(stor))
	s.Require().NoError(err)
	
	c, err := p.Cache()
	s.Require().NoError(err)
	
	stream, err := New(WithIngester(in), WithCache(c))
	s.Require().NoError(err)
	
	ctx := context.Background()
	stream.Add(ctx, &Item{DedupKey: uint64(time.Now().UTC().UnixMicro()), RawBytes: []byte("A")})
	<-time.After(56 * time.Millisecond)
	stream.Add(ctx, &Item{DedupKey: uint64(time.Now().UTC().UnixMicro()), RawBytes: []byte("B")})
	items, err := stream.Range(ctx, 1, 2)
	s.Require().NoError(err)
	for _, item := range items {
		s.T().Logf("item: %v", item)
	}
	
	<-time.After(30 * time.Second)
	
}
