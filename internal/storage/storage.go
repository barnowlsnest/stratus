package storage

import (
	"context"
	"io"
	
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/internal/dedup"
)

const defaultReadBatchSize = 64

type (
	Chunk struct {
		Key   uint64
		Bytes []byte
	}
	
	Storage struct {
		maxReadBatchSize uint64
		wal              *wal.WAL
		dup              *dedup.Deduplicator
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

func WithMaxReadBatchSize(size uint64) Option {
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

func (ch *Chunk) validate() error {
	switch {
	case ch == nil:
		return ErrNilChunk
	case ch.Key == 0:
		return ErrEmptyDedupKey
	case len(ch.Bytes) == 0:
		return ErrEmptyChunk
	default:
		return nil
	}
}

func (s *Storage) Write(ctx context.Context, chunk *Chunk) (uint64, error) {
	if err := chunk.validate(); err != nil {
		return 0, err
	}
	
	if err := s.dup.Try(chunk.Key); err != nil {
		return 0, err
	}
	
	id, err := s.wal.Append(ctx, chunk.Bytes)
	if err != nil {
		return 0, err
	}
	
	return id, nil
}

func (s *Storage) WriteBatch(ctx context.Context, chunks []*Chunk) (ids []uint64, dups int, err error) {
	if len(chunks) == 0 {
		return nil, 0, ErrEmptyChunk
	}
	
	batch := make([][]byte, len(chunks))
	for i, chunk := range chunks {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}
		
		if err = chunk.validate(); err != nil {
			return nil, 0, err
		}
		
		if err = s.dup.Try(chunk.Key); err != nil {
			dups++
			continue
		}
		
		batch[i] = chunk.Bytes
	}
	
	ids, err = s.wal.AppendBatch(ctx, batch)
	if err != nil {
		return nil, 0, err
	}
	
	return ids, dups, nil
}

func (s *Storage) Read(ctx context.Context, atID uint64) (*Chunk, error) {
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
	
	chunk := &Chunk{
		Key:   atID,
		Bytes: entry.Payload,
	}
	
	return chunk, nil
}

func (s *Storage) ReadBatch(ctx context.Context, fromID, toID uint64) ([]*Chunk, error) {
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
	case length > s.maxReadBatchSize:
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
	
	batch := make([]*Chunk, 0, toID-fromID+1)
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
		
		chunk := &Chunk{
			Key:   entry.LSN,
			Bytes: entry.Payload,
		}
		
		batch = append(batch, chunk)
	}
	
	return batch, nil
}

func (s *Storage) Truncate(ctx context.Context, toID uint64) error {
	if toID == 0 {
		toID = s.wal.LastLSN()
	}
	if s.wal.FirstLSN() > toID {
		return ErrOutOfBounds
	}
	
	return s.wal.TruncateContext(ctx, toID)
}

func (s *Storage) Close() error {
	return s.wal.Close()
}
