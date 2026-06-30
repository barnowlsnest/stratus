package ingester

import (
	"context"
	"sync"
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/stratus/internal/storage"
)

type (
	Ingester struct {
		mux       sync.Mutex
		startTime time.Time
		metadata  *Metadata
		storage   *storage.Storage
		logger    *logger.Logger
	}

	Metadata struct {
		BytesWritten    uint64
		DuplicatesCount uint64
		WritesCount     uint64
		WritesPerSecond float64
		Duration        time.Duration
	}

	Option func(*Ingester)
)

func New(opts ...Option) (*Ingester, error) {
	i := &Ingester{
		startTime: time.Now(),
		logger:    sharedlog.Logger(),
		metadata:  &Metadata{},
	}

	for _, opt := range opts {
		opt(i)
	}

	if err := i.validate(); err != nil {
		return nil, err
	}

	return i, nil
}

func WithStorage(storage *storage.Storage) Option {
	return func(i *Ingester) {
		i.storage = storage
	}
}

func WithLogger(logger *logger.Logger) Option {
	return func(i *Ingester) {
		i.logger = logger
	}
}

func (in *Ingester) validate() error {
	switch in.storage {
	case nil:
		return ErrNilStorage
	default:
		return nil
	}
}

func (in *Ingester) Metadata() *Metadata {
	in.mux.Lock()
	defer in.mux.Unlock()
	in.metadata.Duration = time.Since(in.startTime)

	return in.metadata
}

func (in *Ingester) Write(ctx context.Context, record *storage.Record) (uint64, error) {
	id, err := in.storage.Write(ctx, record)
	if err != nil {
		in.logger.Error("failed to write record", sharedlog.F("error", err))
		return 0, err
	}

	in.mux.Lock()
	defer in.mux.Unlock()

	in.metadata.WritesCount++
	in.metadata.BytesWritten += uint64(len(record.Bytes))
	in.metadata.WritesPerSecond = float64(in.metadata.WritesCount) / time.Since(in.startTime).Seconds()

	return id, nil
}

func (in *Ingester) WriteBatch(ctx context.Context, records []*storage.Record) ([]uint64, error) {
	res, err := in.storage.WriteBatch(ctx, records)
	if err != nil {
		in.logger.Error("failed to write batch", sharedlog.F("error", err))
		return nil, err
	}

	if res.DuplicatesCount > 0 {
		in.logger.Debug("found duplicates in batch", sharedlog.F("skipped", res.DuplicatesCount))
	}

	if res.DuplicatesCount == uint64(len(records)) {
		in.logger.Error("all records in batch were duplicates, nothing written")
		return nil, ErrAllSkipped
	}

	var written uint64
	for _, record := range records {
		written += uint64(len(record.Bytes))
	}

	in.mux.Lock()
	defer in.mux.Unlock()

	in.metadata.DuplicatesCount += res.DuplicatesCount
	in.metadata.BytesWritten += res.WrittenBytes
	in.metadata.WritesCount += res.WrittenCount
	in.metadata.WritesPerSecond = float64(in.metadata.WritesCount) / time.Since(in.startTime).Seconds()

	return res.IDs, err
}
