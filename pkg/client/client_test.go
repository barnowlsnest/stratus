package client_test

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
	"github.com/barnowlsnest/stratus/pkg/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func dialOption(t *testing.T) grpc.DialOption {
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

	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
}

func TestClientRoundTrip(t *testing.T) {
	ctx := context.Background()
	c, err := client.New(ctx, "passthrough:///bufnet",
		dialOption(t),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	id, err := c.Write(ctx, 1, []byte(`{"op":"set","k":"a"}`))
	require.NoError(t, err)

	actual, err := c.Read(ctx, id)
	require.NoError(t, err)
	require.Equal(t, id, actual.ID)
	expectedPayload := []byte(`{"op":"set","k":"a"}`)
	require.Equal(t, expectedPayload, actual.Payload)

	first, last, err := c.WriteBatch(ctx, []client.Record{
		{DedupKey: 2, Payload: []byte(`{"op":"set","k":"b"}`)},
		{DedupKey: 3, Payload: []byte(`{"op":"set","k":"c"}`)},
	})
	require.NoError(t, err)
	expectedSpan := uint64(1)
	require.Equal(t, expectedSpan, last-first)

	actualRange, err := c.Range(ctx, first, last)
	require.NoError(t, err)
	require.Len(t, actualRange, 2)
	require.Equal(t, []byte(`{"op":"set","k":"b"}`), actualRange[0].Payload)
	require.Equal(t, []byte(`{"op":"set","k":"c"}`), actualRange[1].Payload)

	_, err = c.Write(ctx, 1, []byte(`{"op":"set","k":"a"}`))
	require.True(t, client.IsDuplicate(err))
}
