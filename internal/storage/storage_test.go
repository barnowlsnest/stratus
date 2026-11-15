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

// ClampRange never extends a range past the ceiling, even when the WAL holds more.
func (s *StorageSuite) TestClampRangeCeiling() {
	tests := []struct {
		expectedErr  error
		name         string
		fromID       uint64
		toID         uint64
		ceiling      uint64
		expectedFrom uint64
		expectedTo   uint64
	}{
		{name: "ceiling above WAL end", fromID: 3, toID: 20, ceiling: 50, expectedFrom: 3, expectedTo: 10},
		{name: "ceiling at WAL end", fromID: 3, toID: 20, ceiling: 10, expectedFrom: 3, expectedTo: 10},
		{name: "ceiling inside range", fromID: 3, toID: 20, ceiling: 6, expectedFrom: 3, expectedTo: 6},
		{name: "zero end resolves to ceiling", fromID: 0, toID: 0, ceiling: 6, expectedFrom: 1, expectedTo: 6},
		{name: "range above ceiling", fromID: 7, toID: 9, ceiling: 6, expectedErr: storage.ErrOutOfBounds},
		{name: "nothing below ceiling", fromID: 1, toID: 5, ceiling: 0, expectedErr: storage.ErrOutOfBounds},
	}

	st := s.newStorage(10)
	for _, tc := range tests {
		s.Run(tc.name, func() {
			actualFrom, actualTo, err := st.ClampRange(tc.fromID, tc.toID, tc.ceiling)
			if tc.expectedErr != nil {
				s.Require().ErrorIs(err, tc.expectedErr)

				return
			}

			s.Require().NoError(err)
			s.Equal(tc.expectedFrom, actualFrom)
			s.Equal(tc.expectedTo, actualTo)
		})
	}
}

// The batch read cap still applies to Read.
func (s *StorageSuite) TestReadStillCapped() {
	st := s.newStorage(100)

	_, err := st.Read(context.Background(), 1, 100)
	s.Require().ErrorIs(err, storage.ErrTooLongRangeToRead)
}
