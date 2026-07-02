package preloader_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/preloader"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/stretchr/testify/require"
)

// newCutCache builds a cache over a WAL holding LSNs 1..26 cut at 24, so the
// available window is exactly [24, 26] — the post-compaction state a client
// sees after the server has reclaimed old records.
func newCutCache(t *testing.T) *preloader.Cache {
	t.Helper()

	w, _, err := wal.Open(t.TempDir(), wal.WithBatchSize(8))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	store, err := storage.New(
		storage.WithWAL(w),
		storage.WithDeduplicator(dedup.New(time.Minute)),
		storage.WithMaxReadBatchSize(1024),
	)
	require.NoError(t, err)

	ctx := context.Background()
	for i := 1; i <= 26; i++ {
		payload := fmt.Appendf(nil, `{"op":"set","k":"key-%d"}`, i)
		_, err := store.Write(ctx, &storage.Record{DedupKey: uint64(i), Bytes: payload})
		require.NoError(t, err)
	}
	require.NoError(t, store.Cut(ctx, 24))

	first, last := store.Boundry()
	require.Equal(t, uint64(24), first)
	require.Equal(t, uint64(26), last)

	pre, err := preloader.New(
		preloader.WithStorage(store),
		preloader.WithCache(lru.New[storage.Record](1024)),
	)
	require.NoError(t, err)

	preCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = pre.Start(preCtx, first, last) }()
	require.NoError(t, pre.WaitStarted(ctx, 5*time.Second))

	cache, err := pre.Cache()
	require.NoError(t, err)

	return cache
}

// TestDeleteEmptyingWALDoesNotPanic guards the cache eviction after a cut that
// reclaims every remaining record: the WAL reports a zero first LSN, and the
// eviction range must not underflow into a huge slice capacity.
func TestDeleteEmptyingWALDoesNotPanic(t *testing.T) {
	cache := newCutCache(t)
	ctx := context.Background()

	require.NoError(t, cache.Delete(ctx, 100))

	first := cache.Metadata().FirstID
	require.Zero(t, first)
}

// TestRangeRecordsClampsToAvailableWindow pins the read contract after a cut:
// ranges overlapping the available window return the available records, zero
// ids mean "from beginning" / "to end", and only fully-disjoint ranges are out
// of bounds.
func TestRangeRecordsClampsToAvailableWindow(t *testing.T) {
	tests := []struct {
		name          string
		fromID        uint64
		toID          uint64
		expectedFirst uint64
		expectedLast  uint64
		expectedErr   error
	}{
		{
			name:          "overlapping range clamps to window",
			fromID:        1,
			toID:          26,
			expectedFirst: 24,
			expectedLast:  26,
		},
		{
			name:          "zero sentinels mean full window",
			fromID:        0,
			toID:          0,
			expectedFirst: 24,
			expectedLast:  26,
		},
		{
			name:          "from zero clamps to first available",
			fromID:        0,
			toID:          26,
			expectedFirst: 24,
			expectedLast:  26,
		},
		{
			name:          "interior subrange returned as-is",
			fromID:        25,
			toID:          26,
			expectedFirst: 25,
			expectedLast:  26,
		},
		{
			name:        "range fully below window is out of bounds",
			fromID:      1,
			toID:        3,
			expectedErr: storage.ErrOutOfBounds,
		},
		{
			name:        "range fully above window is out of bounds",
			fromID:      27,
			toID:        30,
			expectedErr: storage.ErrOutOfBounds,
		},
	}

	cache := newCutCache(t)
	ctx := context.Background()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := cache.RangeRecords(ctx, tc.fromID, tc.toID)

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)

				return
			}

			require.NoError(t, err)
			expectedLen := int(tc.expectedLast - tc.expectedFirst + 1)
			require.Len(t, actual, expectedLen)
			require.Equal(t, tc.expectedFirst, actual[0].DedupKey)
			require.Equal(t, tc.expectedLast, actual[len(actual)-1].DedupKey)
		})
	}
}
