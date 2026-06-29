package stream

import (
	"context"

	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/preloader"
	"github.com/barnowlsnest/stratus/internal/storage"
)

type (
	Stream struct {
		ingester *ingester.Ingester
		cache    *preloader.Cache
	}

	// Item is a single stream entry.
	Item struct {
		// ID is the LSN assigned by the WAL. Zero on the write path; populated on reads.
		ID       uint64
		DedupKey uint64
		RawBytes []byte
	}

	Option func(*Stream)
)

func NewItem(dedupKey uint64, rawBytes []byte) *Item {
	return &Item{
		DedupKey: dedupKey,
		RawBytes: rawBytes,
	}
}

func WithIngester(ingester *ingester.Ingester) Option {
	return func(s *Stream) {
		s.ingester = ingester
	}
}

func WithCache(cache *preloader.Cache) Option {
	return func(s *Stream) {
		s.cache = cache
	}
}

func New(opts ...Option) (*Stream, error) {
	s := &Stream{}
	for _, opt := range opts {
		opt(s)
	}

	if err := s.validate(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Stream) validate() error {
	switch {
	case s.ingester == nil:
		return ErrNilIngester
	case s.cache == nil:
		return ErrNilCache
	default:
		return nil
	}
}

func (s *Stream) Add(ctx context.Context, item *Item) (first, last uint64, err error) {
	record := &storage.Record{
		DedupKey: item.DedupKey,
		Bytes:    item.RawBytes,
	}

	id, err := s.ingester.Write(ctx, record)
	if err != nil {
		return 0, 0, err
	}

	return id, id, nil
}

func (s *Stream) AddN(ctx context.Context, items ...*Item) (first, last uint64, err error) {
	records := make([]*storage.Record, len(items))
	for i, item := range items {
		records[i] = &storage.Record{
			DedupKey: item.DedupKey,
			Bytes:    item.RawBytes,
		}
	}

	ids, err := s.ingester.WriteBatch(ctx, records)
	if err != nil {
		return 0, 0, err
	}

	return ids[0], ids[len(ids)-1], nil
}

func (s *Stream) Read(ctx context.Context, id uint64) (*Item, error) {
	r, err := s.cache.GetRecord(ctx, id)
	if err != nil {
		return nil, err
	}

	return &Item{
		ID:       r.DedupKey,
		DedupKey: r.DedupKey,
		RawBytes: r.Bytes,
	}, nil
}

func (s *Stream) Range(ctx context.Context, first, last uint64) ([]*Item, error) {
	records, err := s.cache.RangeRecords(ctx, first, last)
	if err != nil {
		return nil, err
	}

	items := make([]*Item, len(records))
	for i, record := range records {
		items[i] = &Item{
			ID:       record.DedupKey,
			DedupKey: record.DedupKey,
			RawBytes: record.Bytes,
		}
	}

	return items, nil
}
