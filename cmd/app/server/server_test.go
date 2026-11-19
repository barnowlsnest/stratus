package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	stratusv1api "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

const readOffsetWait = 5 * time.Second

type ServerSuite struct {
	suite.Suite
	nextKey uint64
}

func TestServerSuite(t *testing.T) {
	suite.Run(t, new(ServerSuite))
}

func (s *ServerSuite) newRecords(count int) []*storage.Record {
	records := make([]*storage.Record, 0, count)
	for range count {
		s.nextKey++
		records = append(records, &storage.Record{DedupKey: s.nextKey, Bytes: fmt.Appendf(nil, "rec-%d", s.nextKey)})
	}

	return records
}

// Each output record carries the ID and payload of the record it came from.
func (s *ServerSuite) TestToOutputRecords() {
	tests := []struct {
		name  string
		input []*storage.Record
	}{
		{name: "no records", input: nil},
		{name: "one record", input: []*storage.Record{{ID: 7, Bytes: []byte("a")}}},
		{name: "several records", input: []*storage.Record{{ID: 1, Bytes: []byte("a")}, {ID: 2, Bytes: []byte("bb")}, {ID: 3, Bytes: []byte("c")}}},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			actual := toOutputRecords(tc.input)
			s.Require().Len(actual, len(tc.input))
			for i, expected := range tc.input {
				s.Equal(expected.ID, actual[i].GetId())
				s.Equal(expected.Bytes, actual[i].GetRawData())
			}
		})
	}
}

// startServer runs a stream over a fresh WAL and serves it over an in-memory
// connection, returning the stream and a connected client.
func (s *ServerSuite) startServer() (*stream.Stream, *stratusv1.Client) {
	s.T().Helper()

	w, _, err := wal.Open(s.T().TempDir())
	s.Require().NoError(err)

	st, err := storage.New(storage.WithWAL(w), storage.WithDeduplicator(dedup.New(0)))
	s.Require().NoError(err)

	str, err := stream.New(
		stream.WithStorage(st),
		stream.WithCache(lru.New[storage.Record](1024)),
		stream.WithSubscriberBuffer(16),
	)
	s.Require().NoError(err)

	var wg sync.WaitGroup
	wg.Go(func() { _ = str.Start(context.Background()) })
	s.Require().NoError(str.WaitForStart(context.Background(), readOffsetWait))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	stratusv1api.RegisterStreamServiceServer(srv, New(str))
	wg.Go(func() { _ = srv.Serve(lis) })

	client, err := stratusv1.Dial("passthrough:///bufnet", stratusv1.WithInsecure(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	s.Require().NoError(err)

	s.T().Cleanup(func() {
		_ = client.Close()
		srv.Stop()
		str.Stop(context.Background())
		wg.Wait()
		_ = w.Close()
	})

	return str, client
}

// receiveIDs reads count records from ch, or fails once readOffsetWait passes.
func (s *ServerSuite) receiveIDs(ch <-chan stratusv1.OutputRecord, count int) []uint64 {
	s.T().Helper()

	ids := make([]uint64, 0, count)
	deadline := time.After(readOffsetWait)
	for len(ids) < count {
		select {
		case r, ok := <-ch:
			if !ok {
				return ids
			}
			ids = append(ids, r.ID)
		case <-deadline:
			return ids
		}
	}

	return ids
}

// The client's ReadOffset buffers a bounded number of records whatever
// max_records is, so a huge max_records still delivers records. Sizing the
// buffer by max_records made this one panic.
func (s *ServerSuite) TestReadOffsetHugeMaxRecords() {
	str, client := s.startServer()
	_, err := str.Add(context.Background(), s.newRecords(5))
	s.Require().NoError(err)

	ch, err := client.ReadOffset(context.Background(), 1, 1<<40, readOffsetWait)
	s.Require().NoError(err)
	s.Equal([]uint64{1, 2, 3, 4, 5}, s.receiveIDs(ch, 5))
}

// busyStream always has the next record ready, so a tail reading it never
// catches up and never waits for a wake-up.
type busyStream struct {
	Stream
}

func (busyStream) Get(_ context.Context, fromID, _ uint64, dst []*storage.Record) ([]*storage.Record, error) {
	return append(dst, &storage.Record{ID: fromID}), nil
}

func (busyStream) FirstID() uint64 { return 1 }

func (busyStream) Watch() (wake <-chan struct{}, stop func()) {
	return make(chan struct{}), func() {}
}

// countingSender is a ReadOffset stream that accepts every response.
type countingSender struct {
	grpc.ServerStreamingServer[stratusv1api.ReadResponse]
	sent int
}

func (c *countingSender) Context() context.Context { return context.Background() }

func (c *countingSender) Send(*stratusv1api.ReadResponse) error {
	c.sent++

	return nil
}

// A tail that always has records to read still stops at its timeout. It used
// to check the timeout only while waiting, which such a tail never does.
func (s *ServerSuite) TestReadOffsetStopsAtTimeoutWhileBusy() {
	srv := New(busyStream{})
	sender := &countingSender{}
	req := &stratusv1api.ReadOffsetRequest{
		StartId:    1,
		MaxRecords: 1 << 40,
		Timeout:    durationpb.New(readOffsetMinTimeout),
	}

	done := make(chan error, 1)
	go func() { done <- srv.ReadOffset(req, sender) }()

	select {
	case err := <-done:
		s.NoError(err)
		s.Positive(sender.sent)
	case <-time.After(readOffsetWait):
		s.Fail("ReadOffset kept reading past its timeout")
	}
}

// Every open tail receives records added while it waits, not only the ones
// there when it started, and each tail is woken on its own channel.
func (s *ServerSuite) TestReadOffsetTailsNewRecords() {
	tests := []struct {
		name  string
		tails int
	}{
		{name: "one tail", tails: 1},
		{name: "several tails", tails: 3},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			str, client := s.startServer()
			_, err := str.Add(context.Background(), s.newRecords(5))
			s.Require().NoError(err)

			// max_records of 11 sends IDs 1..10; see ReadOffset.
			tails := make([]<-chan stratusv1.OutputRecord, 0, tc.tails)
			for range tc.tails {
				ch, readErr := client.ReadOffset(context.Background(), 1, 11, readOffsetWait)
				s.Require().NoError(readErr)
				tails = append(tails, ch)
			}

			// Every tail has read what existed and is waiting before more arrive.
			for _, ch := range tails {
				s.Equal([]uint64{1, 2, 3, 4, 5}, s.receiveIDs(ch, 5))
			}

			_, err = str.Add(context.Background(), s.newRecords(5))
			s.Require().NoError(err)

			for _, ch := range tails {
				s.Equal([]uint64{6, 7, 8, 9, 10}, s.receiveIDs(ch, 5))
			}
		})
	}
}
