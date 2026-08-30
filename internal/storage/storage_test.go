package storage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/storage"
)

type StorageSuite struct {
	suite.Suite
}

func TestStorageSuite(t *testing.T) {
	suite.Run(t, new(StorageSuite))
}

// newStorage opens a fresh WAL-backed storage and appends count records.
func (s *StorageSuite) newStorage(count int) *storage.Storage {
	s.T().Helper()

	w, _, err := wal.Open(s.T().TempDir())
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = w.Close() })

	st, err := storage.New(
		storage.WithWAL(w),
		storage.WithDeduplicator(dedup.New(0)),
	)
	s.Require().NoError(err)

	if count == 0 {
		return st
	}

	records := make([]*storage.Record, 0, count)
	for i := 1; i <= count; i++ {
		records = append(records, &storage.Record{DedupKey: uint64(i), Bytes: fmt.Appendf(nil, "rec-%d", i)})
	}
	_, err = st.Write(context.Background(), records)
	s.Require().NoError(err)

	return st
}

// ReadEach must walk ranges larger than the read cap (default 64) that Read rejects.
func (s *StorageSuite) TestReadEachUnbounded() {
	tests := []struct {
		name  string
		count int
	}{
		{name: "under cap", count: 10},
		{name: "at cap", count: 64},
		{name: "over cap", count: 100},
	}

	for _, actual := range tests {
		s.Run(actual.name, func() {
			st := s.newStorage(actual.count)

			var seen []uint64
			err := st.ReadEach(context.Background(), 1, uint64(actual.count), func(r *storage.Record) error {
				seen = append(seen, r.ID)
				return nil
			})

			s.Require().NoError(err)
			s.Len(seen, actual.count)
		})
	}
}

// The batch read cap still applies to Read.
func (s *StorageSuite) TestReadStillCapped() {
	st := s.newStorage(100)

	_, err := st.Read(context.Background(), 1, 100)
	s.Require().ErrorIs(err, storage.ErrTooLongRangeToRead)
}
