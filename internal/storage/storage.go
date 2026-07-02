package storage

import (
	"bytes"
	"context"
	"io"
	"sync"
	
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/internal/dedup"
)

const defaultReadBatchSize = 64

type (
	Record struct {
		DedupKey uint64
		Bytes    []byte
	}
	
	Storage struct {
		maxReadBatchSize int
		wal              *wal.WAL
		dup              *dedup.Deduplicator
		subsWG           sync.WaitGroup
	}
	
	BatchWriteResult struct {
		DuplicatesCount uint64
		WrittenCount    uint64
		WrittenBytes    uint64
		IDs             []uint64
	}
	
	Option func(*Storage)
)

func WithWAL(w *wal.WAL) Option {
	return func(s *Storage) {
		s.wal = w
	}
}

func WithDeduplicator(d *dedup.Deduplicator) Option {
	return func(s *Storage) {
		s.dup = d
	}
}

func WithMaxReadBatchSize(size int) Option {
	return func(s *Storage) {
		s.maxReadBatchSize = size
	}
}

func New(opts ...Option) (*Storage, error) {
	s := new(Storage)
	for _, opt := range opts {
		opt(s)
	}
	
	if err := s.validate(); err != nil {
		return nil, err
	}
	
	s.applyDefaults()
	
	return s, nil
}

func (s *Storage) validate() error {
	switch {
	case s == nil:
		return ErrNilStorage
	case s.wal == nil:
		return ErrNilOption
	case s.dup == nil:
		return ErrNilOption
	default:
		return nil
	}
}

func (s *Storage) applyDefaults() {
	if s.maxReadBatchSize == 0 {
		s.maxReadBatchSize = defaultReadBatchSize
	}
}

func (r *Record) validate() error {
	switch {
	case r == nil:
		return ErrNilRecord
	case r.DedupKey == 0:
		return ErrEmptyDedupKey
	case len(r.Bytes) == 0:
		return ErrEmptyRecord
	default:
		return nil
	}
}

func (s *Storage) Boundry() (first, last uint64) {
	return s.wal.FirstLSN(), s.wal.LastLSN()
}

func (s *Storage) Write(ctx context.Context, record *Record) (uint64, error) {
	if err := record.validate(); err != nil {
		return 0, err
	}
	
	if err := s.dup.Try(record.DedupKey); err != nil {
		return 0, err
	}
	
	id, err := s.wal.Append(ctx, record.Bytes)
	if err != nil {
		return 0, err
	}
	
	return id, nil
}

func (s *Storage) WriteBatch(ctx context.Context, records []*Record) (*BatchWriteResult, error) {
	if len(records) == 0 {
		return nil, ErrEmptyRecord
	}
	
	dupMap := make(map[uint64][]*Record)
	var written uint64
	batch := make([][]byte, 0, len(records))
	for _, record := range records {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		
		if err := record.validate(); err != nil {
			return nil, err
		}
		
		if err := s.dup.Try(record.DedupKey); err != nil {
			dupRecords, exists := dupMap[record.DedupKey]
			if !exists {
				dupRecords = make([]*Record, 0, len(records))
			}
			dupMap[record.DedupKey] = append(dupRecords, record)
			
			continue
		}
		
		batch = append(batch, record.Bytes)
		written += uint64(len(record.Bytes))
	}
	
	if len(batch) == 0 {
		return nil, nil
	}
	
	ids, err := s.wal.AppendBatch(ctx, batch)
	if err != nil {
		return nil, err
	}
	
	return &BatchWriteResult{
		IDs:             ids,
		DuplicatesCount: uint64(len(dupMap)),
		WrittenCount:    uint64(len(batch)),
		WrittenBytes:    written,
	}, nil
}

func (s *Storage) Read(ctx context.Context, atID uint64) (*Record, error) {
	if atID == 0 {
		atID = s.wal.FirstLSN()
	}
	
	switch {
	case s.wal.LastLSN() < atID:
		return nil, ErrOutOfBounds
	case s.wal.FirstLSN() > atID:
		return nil, ErrOutOfBounds
	}
	
	reader, err := s.wal.NewReader(atID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	
	if ok := reader.Next(); !ok {
		return nil, io.EOF
	}
	
	entry, err := reader.Entry(), reader.Err()
	if err != nil {
		return nil, err
	}
	
	chunk := &Record{
		DedupKey: atID,
		Bytes:    bytes.Clone(entry.Payload),
	}
	
	return chunk, nil
}

func (s *Storage) ReadBatch(ctx context.Context, fromID, toID uint64) ([]*Record, error) {
	if fromID == 0 {
		fromID = s.wal.FirstLSN()
	}
	if toID == 0 {
		toID = s.wal.LastLSN()
	}
	
	length := toID - fromID + 1
	
	switch {
	case toID < fromID:
		return nil, ErrOutOfBounds
	case int(length) > s.maxReadBatchSize:
		return nil, ErrTooLongRangeToRead
	case s.wal.LastLSN() < fromID:
		return nil, ErrOutOfBounds
	case s.wal.FirstLSN() > toID:
		return nil, ErrOutOfBounds
	}
	
	reader, err := s.wal.NewReader(fromID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	
	batch := make([]*Record, 0, toID-fromID+1)
	for reader.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		
		entry, err := reader.Entry(), reader.Err()
		if err != nil {
			return nil, err
		}
		
		if entry.LSN > toID {
			break
		}
		
		chunk := &Record{
			DedupKey: entry.LSN,
			Bytes:    bytes.Clone(entry.Payload),
		}
		
		batch = append(batch, chunk)
	}
	
	return batch, nil
}

func (s *Storage) Cut(ctx context.Context, toID uint64) error {
	if toID == 0 {
		toID = s.wal.LastLSN()
	}
	if s.wal.FirstLSN() > toID {
		return ErrOutOfBounds
	}
	
	return s.wal.CutOffsetContext(ctx, toID)
}

func (s *Storage) Subscribe(ctx context.Context, fromID uint64, buffer int) (records <-chan *Record, err error) {
	if buffer <= 0 {
		buffer = 1
	}
	
	f, err := s.wal.Follower(fromID, wal.WithFollow())
	if err != nil {
		closedCh := make(chan *Record)
		close(closedCh)
		return closedCh, err
	}
	
	recordsCh := make(chan *Record, buffer)
	s.subsWG.Go(func() {
		defer close(recordsCh)
		for entry := range f.RecordsChan(ctx) {
			recordsCh <- &Record{DedupKey: entry.LSN, Bytes: bytes.Clone(entry.Payload)}
		}
		
		if errClose := f.Close(); errClose != nil {
			sharedlog.Error(errClose)
		}
		if errFollower := f.Err(); errFollower != nil {
			sharedlog.Error(errFollower)
		}
	})
	
	return recordsCh, nil
}

func (s *Storage) WaitForSubs() error {
	s.subsWG.Wait()
	return nil
}

func (s *Storage) Range() (first, last uint64) {
	return s.wal.FirstLSN(), s.wal.LastLSN()
}
