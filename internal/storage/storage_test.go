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
func (s *StorageTestSuite) TearDownTest() {
	_ = s.storage.Close()
}

func (s *StorageTestSuite) TestWrite() {
	ctx := s.T().Context()
	chunk := s.newChunk(16)
	id, err := s.storage.Write(ctx, &Chunk{1, chunk})
	s.Require().NoError(err)
	s.Require().NotZero(id)
	
	err = s.storage.wal.Replay(1, func(entry wal.Entry) error {
		s.Require().Equal(id, entry.LSN)
		s.Require().Equal(chunk, entry.Payload)
		return nil
	})
	
	s.Require().NoError(err)
}

func (s *StorageTestSuite) TestWrite_RejectOnZeroKey() {
	ctx := s.T().Context()
	id, err := s.storage.Write(ctx, &Chunk{0, s.newChunk(8)})
	s.Require().ErrorIs(err, ErrEmptyDedupKey)
	s.Require().Zero(id)
}

func (s *StorageTestSuite) TestWrite_RejectOnEmptyChunk() {
	ctx := s.T().Context()
	id, err := s.storage.Write(ctx, &Chunk{2, nil})
	s.Require().ErrorIs(err, ErrEmptyChunk)
	s.Require().Zero(id)
}

func (s *StorageTestSuite) TestWrite_RejectDuplicate() {
	ctx := s.T().Context()
	id1, err1 := s.storage.Write(ctx, &Chunk{2, s.newChunk(8)})
	s.Require().NoError(err1)
	s.Require().NotZero(id1)
	id2, err2 := s.storage.Write(ctx, &Chunk{2, s.newChunk(12)})
	s.Require().ErrorIs(err2, dedup.ErrDuplicateChunk)
	s.Require().Zero(id2)
}

func (s *StorageTestSuite) TestRead() {
	ctx := s.T().Context()
	chunk := s.newChunk(16)
	id, err := s.storage.Write(ctx, &Chunk{1, chunk})
	s.Require().NoError(err)
	s.Require().NotZero(id)
	
	readChunk, err := s.storage.Read(ctx, id)
	s.Require().NoError(err)
	s.Require().Equal(id, readChunk.Key)
	s.Require().Equal(chunk, readChunk.Bytes)
}

func (s *StorageTestSuite) newChunk(length int) []byte {
	s.T().Helper()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[r.Intn(len(charset))]
	}
	
	return result
}
