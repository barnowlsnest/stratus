package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/cmd/server"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/preloader"
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

func newTestStream(t *testing.T) *stream.Stream {
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

	return st
}

func newTestClient(t *testing.T) stratusv1.StreamServiceClient {
	t.Helper()

	srv, err := server.New(server.WithStream(newTestStream(t)))
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

// freeAddr reserves an ephemeral port and returns its address. The listener is
// closed before returning so Run can bind it; the reuse window is acceptable in
// tests.
func freeAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	return addr
}

// TestServerShutdownWithIdleStream verifies Run returns promptly on context
// cancellation even when a client holds an open but idle stream, rather than
// blocking forever in GracefulStop.
func TestServerShutdownWithIdleStream(t *testing.T) {
	addr := freeAddr(t)
	grace := 200 * time.Millisecond
	srv, err := server.New(
		server.WithStream(newTestStream(t)),
		server.WithAddr(addr),
		server.WithShutdownGrace(grace),
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(runCtx) }()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Open a write stream on an independent context and keep it idle: never
	// send, never close. This mimics a client that just stays connected and
	// must not block the server's shutdown.
	ws, err := stratusv1.NewStreamServiceClient(conn).Write(context.Background())
	require.NoError(t, err)
	// Force the stream to be established server-side by exchanging headers.
	require.NoError(t, ws.Send(recordReq(1, `{"op":"set","k":"a"}`)))
	_, err = ws.Recv()
	require.NoError(t, err)

	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(grace + 5*time.Second):
		t.Fatal("Run did not return after context cancellation; shutdown hung on idle stream")
	}
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
		name        string
		requests    []*stratusv1.WriteRequest // sent in order; assertions use the last response
		expectedErr stratusv1.Err             // ERR_UNKNOWN means success expected
		check       func(t *testing.T, actual *stratusv1.WriteResponse)
	}{
		{
			name:     "single returns start equal end",
			requests: []*stratusv1.WriteRequest{recordReq(1, `{"op":"set","k":"a"}`)},
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
			check: func(t *testing.T, actual *stratusv1.WriteResponse) {
				expectedSpan := uint64(2)
				require.Equal(t, expectedSpan, actual.GetEnd()-actual.GetStart())
			},
		},
		{
			name: "duplicate returns in-stream already exists",
			requests: []*stratusv1.WriteRequest{
				recordReq(99, `{"op":"set","k":"d"}`),
				recordReq(99, `{"op":"set","k":"d"}`),
			},
			expectedErr: stratusv1.Err_ERR_ALREADY_EXISTS,
		},
		{
			name:        "empty request returns in-stream invalid argument",
			requests:    []*stratusv1.WriteRequest{{}},
			expectedErr: stratusv1.Err_ERR_INVALID_ARGUMENT,
		},
		{
			name: "all-duplicate batch returns in-stream already exists",
			requests: []*stratusv1.WriteRequest{
				batchReq(
					&stratusv1.Record{DedupKey: 101, Payload: []byte(`{"op":"set","k":"e"}`)},
					&stratusv1.Record{DedupKey: 102, Payload: []byte(`{"op":"set","k":"f"}`)},
				),
				batchReq(
					&stratusv1.Record{DedupKey: 101, Payload: []byte(`{"op":"set","k":"e"}`)},
					&stratusv1.Record{DedupKey: 102, Payload: []byte(`{"op":"set","k":"f"}`)},
				),
			},
			expectedErr: stratusv1.Err_ERR_ALREADY_EXISTS,
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
				require.NoError(t, err)
			}

			if tc.expectedErr != stratusv1.Err_ERR_UNKNOWN {
				require.NotNil(t, actual.GetError())
				require.Equal(t, tc.expectedErr, actual.GetError().GetCode())

				// The stream must survive the error: a fresh unique write succeeds.
				require.NoError(t, ws.Send(recordReq(1000, `{"op":"set","k":"recovery"}`)))
				recovered, err := ws.Recv()
				require.NoError(t, err)
				require.Nil(t, recovered.GetError())

				return
			}

			require.Nil(t, actual.GetError())
			if tc.check != nil {
				tc.check(t, actual)
			}
		})
	}
}

// TestServerWriteCancelTerminatesStream verifies the fatal path: cancelling
// the client's stream context terminates the RPC (as opposed to recoverable
// errors, which are reported in-stream and keep the RPC alive).
func TestServerWriteCancelTerminatesStream(t *testing.T) {
	rpc := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ws, err := rpc.Write(ctx)
	require.NoError(t, err)

	require.NoError(t, ws.Send(recordReq(201, `{"op":"set","k":"a"}`)))
	actual, err := ws.Recv()
	require.NoError(t, err)
	require.Nil(t, actual.GetError())

	cancel()

	_, err = ws.Recv()
	require.Error(t, err)
	require.Equal(t, codes.Canceled, status.Code(err))
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

// TestServerReadInStreamErrors verifies recoverable read failures are reported
// on the stream (with WAL bounds) and the stream stays usable afterwards.
func TestServerReadInStreamErrors(t *testing.T) {
	rpc := newTestClient(t)
	ctx := context.Background()

	ws, err := rpc.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, ws.Send(batchReq(
		&stratusv1.Record{DedupKey: 31, Payload: []byte(`{"op":"set","k":"a"}`)},
		&stratusv1.Record{DedupKey: 32, Payload: []byte(`{"op":"set","k":"b"}`)},
	)))
	written, err := ws.Recv()
	require.NoError(t, err)

	rs, err := rpc.Read(ctx)
	require.NoError(t, err)

	// Range reaching past the last LSN is clamped to the available window and
	// returns the available entries.
	require.NoError(t, rs.Send(&stratusv1.ReadRequest{Query: &stratusv1.ReadRequest_Range{
		Range: &stratusv1.Range{First: written.GetStart(), Last: written.GetEnd() + 100},
	}}))
	actual, err := rs.Recv()
	require.NoError(t, err)
	require.Nil(t, actual.GetError())
	require.Len(t, actual.GetEntries(), 2)

	// Range entirely past the last LSN → in-stream OUT_OF_RANGE with bounds.
	require.NoError(t, rs.Send(&stratusv1.ReadRequest{Query: &stratusv1.ReadRequest_Range{
		Range: &stratusv1.Range{First: written.GetEnd() + 100, Last: written.GetEnd() + 200},
	}}))
	actual, err = rs.Recv()
	require.NoError(t, err)
	require.NotNil(t, actual.GetError())
	require.Equal(t, stratusv1.Err_ERR_OUT_OF_RANGE, actual.GetError().GetCode())
	require.Equal(t, written.GetStart(), actual.GetError().GetFirstLsn())
	require.Equal(t, written.GetEnd(), actual.GetError().GetLastLsn())

	// Empty query → in-stream INVALID_ARGUMENT.
	require.NoError(t, rs.Send(&stratusv1.ReadRequest{}))
	actual, err = rs.Recv()
	require.NoError(t, err)
	require.NotNil(t, actual.GetError())
	require.Equal(t, stratusv1.Err_ERR_INVALID_ARGUMENT, actual.GetError().GetCode())

	// The same stream still serves a valid query.
	require.NoError(t, rs.Send(&stratusv1.ReadRequest{Query: &stratusv1.ReadRequest_Id{Id: written.GetStart()}}))
	actual, err = rs.Recv()
	require.NoError(t, err)
	require.Nil(t, actual.GetError())
	require.Len(t, actual.GetEntries(), 1)
}
