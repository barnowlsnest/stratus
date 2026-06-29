package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/preloader"
	"github.com/barnowlsnest/stratus/internal/server"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
	stratusv1 "github.com/barnowlsnest/stratus/pkg/client/gen/stratus/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func newTestClient(t *testing.T) stratusv1.StreamServiceClient {
	t.Helper()

	w, r, err := wal.Open(t.TempDir(), wal.WithBatchSize(8))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	store, err := storage.New(
		storage.WithWAL(w),
		storage.WithDeduplicator(dedup.New(time.Minute)),
		storage.WithMaxReadBatchSize(1024),
	)
	require.NoError(t, err)

	pre, err := preloader.New(
		preloader.WithStorage(store),
		preloader.WithCache(lru.New[storage.Record](1024)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = pre.Start(ctx, r.FirstLSN, r.LastLSN) }()
	require.NoError(t, pre.WaitStarted(ctx, 5*time.Second))

	cache, err := pre.Cache()
	require.NoError(t, err)
	in, err := ingester.New(ingester.WithStorage(store))
	require.NoError(t, err)
	st, err := stream.New(stream.WithIngester(in), stream.WithCache(cache))
	require.NoError(t, err)
	srv, err := server.New(server.WithStream(st))
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer()
	srv.Register(g)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return stratusv1.NewStreamServiceClient(conn)
}

func recordReq(dedupKey uint64, payload string) *stratusv1.WriteRequest {
	return &stratusv1.WriteRequest{Payload: &stratusv1.WriteRequest_Record{
		Record: &stratusv1.Record{DedupKey: dedupKey, Payload: []byte(payload)},
	}}
}

func batchReq(records ...*stratusv1.Record) *stratusv1.WriteRequest {
	return &stratusv1.WriteRequest{Payload: &stratusv1.WriteRequest_Batch{
		Batch: &stratusv1.RecordBatch{Records: records},
	}}
}

func TestServerWrite(t *testing.T) {
	tests := []struct {
		name         string
		requests     []*stratusv1.WriteRequest // sent in order; assertions use the last response
		expectedCode codes.Code
		check        func(t *testing.T, actual *stratusv1.WriteResponse)
	}{
		{
			name:         "single returns start equal end",
			requests:     []*stratusv1.WriteRequest{recordReq(1, `{"op":"set","k":"a"}`)},
			expectedCode: codes.OK,
			check: func(t *testing.T, actual *stratusv1.WriteResponse) {
				require.Equal(t, actual.GetStart(), actual.GetEnd())
			},
		},
		{
			name: "batch returns contiguous range",
			requests: []*stratusv1.WriteRequest{batchReq(
				&stratusv1.Record{DedupKey: 10, Payload: []byte(`{"op":"set","k":"a"}`)},
				&stratusv1.Record{DedupKey: 11, Payload: []byte(`{"op":"set","k":"b"}`)},
				&stratusv1.Record{DedupKey: 12, Payload: []byte(`{"op":"set","k":"c"}`)},
			)},
			expectedCode: codes.OK,
			check: func(t *testing.T, actual *stratusv1.WriteResponse) {
				expectedSpan := uint64(2)
				require.Equal(t, expectedSpan, actual.GetEnd()-actual.GetStart())
			},
		},
		{
			name: "duplicate returns AlreadyExists",
			requests: []*stratusv1.WriteRequest{
				recordReq(99, `{"op":"set","k":"d"}`),
				recordReq(99, `{"op":"set","k":"d"}`),
			},
			expectedCode: codes.AlreadyExists,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpc := newTestClient(t)
			ws, err := rpc.Write(context.Background())
			require.NoError(t, err)

			var actual *stratusv1.WriteResponse
			for _, req := range tc.requests {
				require.NoError(t, ws.Send(req))
				actual, err = ws.Recv()
			}

			require.Equal(t, tc.expectedCode, status.Code(err))
			if tc.check != nil && err == nil {
				tc.check(t, actual)
			}
		})
	}
}

func TestServerRead(t *testing.T) {
	rpc := newTestClient(t)
	ctx := context.Background()

	// Seed three records and capture the assigned range.
	ws, err := rpc.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, ws.Send(batchReq(
		&stratusv1.Record{DedupKey: 21, Payload: []byte(`{"op":"set","k":"a"}`)},
		&stratusv1.Record{DedupKey: 22, Payload: []byte(`{"op":"set","k":"b"}`)},
		&stratusv1.Record{DedupKey: 23, Payload: []byte(`{"op":"set","k":"c"}`)},
	)))
	written, err := ws.Recv()
	require.NoError(t, err)

	tests := []struct {
		name        string
		query       *stratusv1.ReadRequest
		expectedLen int
		expectedID  uint64
	}{
		{
			name:        "read by id",
			query:       &stratusv1.ReadRequest{Query: &stratusv1.ReadRequest_Id{Id: written.GetStart()}},
			expectedLen: 1,
			expectedID:  written.GetStart(),
		},
		{
			name: "read full range",
			query: &stratusv1.ReadRequest{Query: &stratusv1.ReadRequest_Range{
				Range: &stratusv1.Range{First: written.GetStart(), Last: written.GetEnd()},
			}},
			expectedLen: 3,
			expectedID:  written.GetStart(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs, err := rpc.Read(ctx)
			require.NoError(t, err)
			require.NoError(t, rs.Send(tc.query))

			actual, err := rs.Recv()
			require.NoError(t, err)
			require.Len(t, actual.GetEntries(), tc.expectedLen)
			require.Equal(t, tc.expectedID, actual.GetEntries()[0].GetId())
		})
	}
}
