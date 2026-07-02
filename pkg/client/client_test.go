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

func TestClientTruncate(t *testing.T) {
	ctx := context.Background()
	c, err := client.New(ctx, "passthrough:///bufnet",
		dialOption(t),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	const records = 5
	for i := range records {
		_, err := c.Write(ctx, uint64(i+1), []byte(`{"op":"set","k":"v"}`))
		require.NoError(t, err)
	}

	first, last, err := c.Truncate(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(3), last)

	// Records beyond the truncation point stay readable.
	survivor, err := c.Read(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, uint64(4), survivor.ID)

	meta, err := c.GetMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), meta.TruncateClaimsCount)
	require.Positive(t, meta.LastTruncateClaimAtUnix)
}

func TestClientGetMetadata(t *testing.T) {
	ctx := context.Background()
	c, err := client.New(ctx, "passthrough:///bufnet",
		dialOption(t),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	single := []byte(`{"op":"set","k":"alpha"}`)
	_, err = c.Write(ctx, 1, single)
	require.NoError(t, err)

	batch := []client.Record{
		{DedupKey: 2, Payload: []byte(`{"op":"set","k":"beta"}`)},
		{DedupKey: 3, Payload: []byte(`{"op":"set","k":"gamma"}`)},
	}
	_, _, err = c.WriteBatch(ctx, batch)
	require.NoError(t, err)

	actual, err := c.GetMetadata(ctx)
	require.NoError(t, err)

	expectedBytes := uint64(len(single) + len(batch[0].Payload) + len(batch[1].Payload))
	require.Equal(t, uint64(3), actual.WritesCount)
	require.Equal(t, expectedBytes, actual.BytesWritten)
	require.Zero(t, actual.DuplicatesCount)
	require.Equal(t, uint64(1), actual.FirstID)
	require.Equal(t, uint64(3), actual.LastID)
	require.Equal(t, uint64(3), actual.StorageSize)
	require.Positive(t, actual.WritesPerSecond)
	require.Positive(t, actual.DurationSeconds)
}

// TestClientInStreamErrors verifies recoverable failures surface as typed
// errors carrying WAL bounds, and that both streams survive them.
func TestClientInStreamErrors(t *testing.T) {
	ctx := context.Background()
	c, err := client.New(ctx, "passthrough:///bufnet",
		dialOption(t),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	first, last, err := c.WriteBatch(ctx, []client.Record{
		{DedupKey: 41, Payload: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 42, Payload: []byte(`{"op":"set","k":"b"}`)},
	})
	require.NoError(t, err)

	// A Range reaching past the tail is clamped to the available window.
	clamped, err := c.Range(ctx, first, last+100)
	require.NoError(t, err)
	require.Len(t, clamped, 2)

	// A Range entirely past the tail → ErrOutOfRange carrying the bounds.
	_, err = c.Range(ctx, last+100, last+200)
	require.ErrorIs(t, err, client.ErrOutOfRange)

	var streamErr *client.Error
	require.ErrorAs(t, err, &streamErr)
	require.Equal(t, first, streamErr.First)
	require.Equal(t, last, streamErr.Last)

	// The read stream survives: the same client serves a valid Range.
	actual, err := c.Range(ctx, first, last)
	require.NoError(t, err)
	require.Len(t, actual, 2)

	// Duplicate write → ErrAlreadyExists; write stream survives.
	_, err = c.Write(ctx, 41, []byte(`{"op":"set","k":"a"}`))
	require.ErrorIs(t, err, client.ErrAlreadyExists)
	require.True(t, client.IsDuplicate(err))

	recoveredID, err := c.Write(ctx, 43, []byte(`{"op":"set","k":"c"}`))
	require.NoError(t, err)

	// Read past the tail → ErrOutOfRange with bounds (LSNs are gapless, so an
	// id beyond the last LSN is out-of-range, not missing). The recovery write
	// above advanced the tail, so Last is its LSN.
	_, err = c.Read(ctx, recoveredID+100)
	require.ErrorIs(t, err, client.ErrOutOfRange)

	var readErr *client.Error
	require.ErrorAs(t, err, &readErr)
	require.Equal(t, first, readErr.First)
	require.Equal(t, recoveredID, readErr.Last)

	// All-duplicate batch write → ErrAlreadyExists; write stream survives.
	_, _, err = c.WriteBatch(ctx, []client.Record{
		{DedupKey: 41, Payload: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 42, Payload: []byte(`{"op":"set","k":"b"}`)},
	})
	require.ErrorIs(t, err, client.ErrAlreadyExists)
	require.True(t, client.IsDuplicate(err))

	_, err = c.Write(ctx, 44, []byte(`{"op":"set","k":"d"}`))
	require.NoError(t, err)
}
