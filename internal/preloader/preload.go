package preloader

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
	
	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/stratus/internal/storage"
	"golang.org/x/sync/errgroup"
)

const defaultSubscriberBufferSize = 1

type (
	Preloader struct {
		subscriberBuffer int
		storage          *storage.Storage
		lru              *lru.LRU[storage.Record]
		logger           *logger.Logger
		cancel           context.CancelFunc
		started          atomic.Bool
	}
	
	Cache struct {
		pre *Preloader
	}
	
	Option func(*Preloader)
)

func WithStorage(appender *storage.Storage) Option {
	return func(p *Preloader) {
		p.storage = appender
	}
}

func WithCache(lru *lru.LRU[storage.Record]) Option {
	return func(p *Preloader) {
		p.lru = lru
	}
}

func WithSubscriberBuffer(buffer int) Option {
	return func(p *Preloader) {
		p.subscriberBuffer = buffer
	}
}

func WithLogger(logger *logger.Logger) Option {
	return func(p *Preloader) {
		p.logger = logger
	}
}

func New(opts ...Option) (*Preloader, error) {
	p := &Preloader{}
	for _, opt := range opts {
		opt(p)
	}
	
	if err := p.validate(); err != nil {
		return nil, err
	}
	
	if p.subscriberBuffer <= 0 {
		p.subscriberBuffer = defaultSubscriberBufferSize
	}
	
	return p, nil
}

func (p *Preloader) validate() error {
	switch {
	case p.storage == nil:
		return ErrMissingAppender
	case p.lru == nil:
		return ErrMissingCache
	default:
		return nil
	}
}

func (p *Preloader) IsStarted() bool {
	return p.started.Load()
}

func (p *Preloader) Start(ctx context.Context, first, last uint64) error {
	if p.IsStarted() {
		return ErrAlreadyStarted
	}
	
	pCtx, pCancel := context.WithCancel(ctx)
	p.cancel = pCancel
	
	if err := p.fetchAndCacheRecords(pCtx, first, last); err != nil {
		return err
	}
	
	records, err := p.storage.Subscribe(pCtx, last, p.subscriberBuffer)
	if err != nil {
		return err
	}
	
	p.started.Store(true)
	for r := range records {
		p.lru.Put(r.DedupKey, r)
	}
	p.started.Swap(false)
	
	return nil
}

func (p *Preloader) WaitStarted(ctx context.Context, timeout time.Duration) error {
	started, stop := make(chan struct{}), make(chan struct{})
	var eg errgroup.Group
	eg.Go(func() error {
		defer close(started)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-stop:
				return nil
			default:
				if p.IsStarted() {
					return nil
				}
			}
		}
	})
	eg.Go(func() error {
		select {
		case <-started:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeout):
			return errors.New("timeout waiting for preloader to start")
		}
	})
	
	return eg.Wait()
}

func (p *Preloader) Stop() {
	p.cancel()
}

func (p *Preloader) Cache() (*Cache, error) {
	if !p.IsStarted() {
		return nil, ErrNotStarted
	}
	
	return &Cache{pre: p}, nil
}

func (cache *Cache) GetRecord(ctx context.Context, id uint64) (*storage.Record, error) {
	return cache.pre.lazyLoadRecord(ctx, id)
}

func (cache *Cache) RangeRecords(ctx context.Context, fromID, toID uint64) ([]*storage.Record, error) {
	if err := cache.pre.fetchAndCacheRecords(ctx, fromID, toID); err != nil {
		return nil, err
	}
	
	records := make([]*storage.Record, 0, toID-fromID+1)
	for id := fromID; id <= toID; id++ {
		r, err := cache.pre.lazyLoadRecord(ctx, id)
		if err != nil {
			return nil, err
		}
		
		records = append(records, r)
	}
	
	return records, nil
}

func (p *Preloader) lazyLoadRecord(ctx context.Context, id uint64) (*storage.Record, error) {
	record, err := p.lru.Get(id)
	if err != nil {
		switch {
		case errors.Is(err, lru.ErrCacheMiss):
			return p.readAndCacheRecord(ctx, id)
		default:
			return nil, err
		}
	}
	
	return record, nil
}

func (p *Preloader) readAndCacheRecord(ctx context.Context, id uint64) (*storage.Record, error) {
	r, err := p.storage.Read(ctx, id)
	if err != nil {
		return nil, err
	}
	
	p.lru.Put(r.DedupKey, r)
	
	return r, nil
}

func (p *Preloader) fetchAndCacheRecords(ctx context.Context, fromID, toID uint64) error {
	records, err := p.storage.ReadBatch(ctx, fromID, toID)
	if err != nil {
		return err
	}
	
	for _, r := range records {
		p.lru.Put(r.DedupKey, r)
	}
	
	return nil
}
