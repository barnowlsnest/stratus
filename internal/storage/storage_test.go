package storage

import (
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/stretchr/testify/suite"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.!-=@#$%^&*()}|{;:?<>[]~"

type StorageTestSuite struct {
	suite.Suite
	storage *Storage
}

func TestStorageSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}

func (s *StorageTestSuite) SetupTest() {
	dir := filepath.Clean(s.T().TempDir())
	w, _, err := wal.Open(dir,
		wal.WithBatchSize(64),
		wal.WithMaxSegmentSize(64*1024),
		wal.WithMaxRecordSize(256*1024),
		wal.WithSyncPolicy(wal.SyncBatched),
		wal.WithFlushInterval(10*time.Second),
	)
	s.Require().NoError(err)
	s.storage, err = New(
		WithWAL(w),
		WithDeduplicator(dedup.New(time.Second)),
		WithMaxReadBatchSize(8),
	)
	s.Require().NoError(err)
	s.Require().NoError(err)
}

func (s *StorageTestSuite) TestWrite() {
	ctx := s.T().Context()
	record := s.newRecord(16)
	id, err := s.storage.Write(ctx, &Record{1, record})
	s.Require().NoError(err)
	s.Require().NotZero(id)

	err = s.storage.wal.Replay(1, func(entry wal.Entry) error {
		s.Require().Equal(id, entry.LSN)
		s.Require().Equal(record, entry.Payload)
		return nil
	})

	s.Require().NoError(err)
}

func (s *StorageTestSuite) TestWrite_RejectOnZeroKey() {
	ctx := s.T().Context()
	id, err := s.storage.Write(ctx, &Record{0, s.newRecord(8)})
	s.Require().ErrorIs(err, ErrEmptyDedupKey)
	s.Require().Zero(id)
}

func (s *StorageTestSuite) TestWrite_RejectOnEmptyChunk() {
	ctx := s.T().Context()
	id, err := s.storage.Write(ctx, &Record{2, nil})
	s.Require().ErrorIs(err, ErrEmptyRecord)
	s.Require().Zero(id)
}

func (s *StorageTestSuite) TestWrite_RejectDuplicate() {
	ctx := s.T().Context()
	id1, err1 := s.storage.Write(ctx, &Record{2, s.newRecord(8)})
	s.Require().NoError(err1)
	s.Require().NotZero(id1)
	id2, err2 := s.storage.Write(ctx, &Record{2, s.newRecord(12)})
	s.Require().ErrorIs(err2, dedup.ErrDuplicateChunk)
	s.Require().Zero(id2)
}

func (s *StorageTestSuite) TestRead() {
	ctx := s.T().Context()
	record := s.newRecord(16)
	id, err := s.storage.Write(ctx, &Record{1, record})
	s.Require().NoError(err)
	s.Require().NotZero(id)

	readChunk, err := s.storage.Read(ctx, id)
	s.Require().NoError(err)
	s.Require().Equal(id, readChunk.DedupKey)
	s.Require().Equal(record, readChunk.Bytes)
}

func (s *StorageTestSuite) TestWriteBatchExcludesDuplicates() {
	ctx := s.T().Context()

	seed := []*Record{
		{DedupKey: 1, Bytes: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 2, Bytes: []byte(`{"op":"set","k":"b"}`)},
	}
	_, err := s.storage.WriteBatch(ctx, seed)
	s.Require().NoError(err)

	input := []*Record{
		{DedupKey: 1, Bytes: []byte(`{"op":"set","k":"a"}`)}, // duplicate
		{DedupKey: 3, Bytes: []byte(`{"op":"set","k":"c"}`)}, // new
	}
	result, err := s.storage.WriteBatch(ctx, input)
	s.Require().NoError(err)

	expectedDups := uint64(1)
	expectedWritten := 1
	s.Require().Equal(expectedDups, result.DuplicatesCount)
	s.Require().Len(result.IDs, expectedWritten)

	actual, err := s.storage.Read(ctx, result.IDs[0])
	s.Require().NoError(err)
	expectedPayload := []byte(`{"op":"set","k":"c"}`)
	s.Require().Equal(expectedPayload, actual.Bytes)
}

func (s *StorageTestSuite) TestWriteBatchAllDuplicatesReturnsResultNotNil() {
	ctx := s.T().Context()

	seed := []*Record{
		{DedupKey: 1, Bytes: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 2, Bytes: []byte(`{"op":"set","k":"b"}`)},
	}
	_, err := s.storage.WriteBatch(ctx, seed)
	s.Require().NoError(err)

	input := []*Record{
		{DedupKey: 1, Bytes: []byte(`{"op":"set","k":"a"}`)}, // duplicate
		{DedupKey: 2, Bytes: []byte(`{"op":"set","k":"b"}`)}, // duplicate
	}
	result, err := s.storage.WriteBatch(ctx, input)
	s.Require().NoError(err)
	s.Require().NotNil(result)

	expectedDups := uint64(2)
	s.Require().Equal(expectedDups, result.DuplicatesCount)
	s.Require().Empty(result.IDs)
	s.Require().Zero(result.WrittenCount)
}

func (s *StorageTestSuite) TestWriteBatchCountsDuplicateRecordsNotUniqueKeys() {
	ctx := s.T().Context()

	seed := []*Record{
		{DedupKey: 7, Bytes: []byte(`{"op":"set","k":"a"}`)},
	}
	_, err := s.storage.WriteBatch(ctx, seed)
	s.Require().NoError(err)

	// All-duplicate batch with a repeated key: two skipped records, one key.
	allDup := []*Record{
		{DedupKey: 7, Bytes: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 7, Bytes: []byte(`{"op":"set","k":"a"}`)},
	}
	result, err := s.storage.WriteBatch(ctx, allDup)
	s.Require().NoError(err)
	s.Require().NotNil(result)

	expectedDups := uint64(2)
	s.Require().Equal(expectedDups, result.DuplicatesCount)
	s.Require().Empty(result.IDs)

	// Mixed batch on the normal path: two skipped records sharing a key, one written.
	mixed := []*Record{
		{DedupKey: 7, Bytes: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 7, Bytes: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 8, Bytes: []byte(`{"op":"set","k":"b"}`)},
	}
	result, err = s.storage.WriteBatch(ctx, mixed)
	s.Require().NoError(err)
	s.Require().Equal(expectedDups, result.DuplicatesCount)
	s.Require().Len(result.IDs, 1)
}

func (s *StorageTestSuite) TestReadBatchReturnsDistinctPayloads() {
	ctx := s.T().Context()

	input := []*Record{
		{DedupKey: 1, Bytes: []byte(`{"op":"set","k":"a"}`)},
		{DedupKey: 2, Bytes: []byte(`{"op":"set","k":"b"}`)},
		{DedupKey: 3, Bytes: []byte(`{"op":"set","k":"c"}`)},
	}
	result, err := s.storage.WriteBatch(ctx, input)
	s.Require().NoError(err)
	s.Require().Len(result.IDs, 3)

	ids := result.IDs
	actual, err := s.storage.ReadBatch(ctx, ids[0], ids[len(ids)-1])
	s.Require().NoError(err)
	s.Require().Len(actual, 3)

	// Each record must own its payload; a reused reader buffer would make them
	// all equal to the last entry.
	for i, want := range input {
		s.Require().Equal(want.Bytes, actual[i].Bytes)
	}
}

func (s *StorageTestSuite) newRecord(length int) []byte {
	s.T().Helper()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]byte, length)
	for i := range length {
		result[i] = charset[r.Intn(len(charset))]
	}

	return result
}
