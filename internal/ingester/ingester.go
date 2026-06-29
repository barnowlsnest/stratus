package ingester

import (
	"context"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/stratus/internal/storage"
)

type (
	Ingester struct {
		storage *storage.Storage
		logger  *logger.Logger
	}

	Option func(*Ingester)
)

func New(opts ...Option) (*Ingester, error) {
	i := &Ingester{
		logger: sharedlog.Logger(),
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

func (in Ingester) validate() error {
	switch in.storage {
	case nil:
		return ErrNilStorage
	default:
		return nil
	}
}

func (in Ingester) Write(ctx context.Context, record *storage.Record) (uint64, error) {
	id, err := in.storage.Write(ctx, record)
	if err != nil {
		in.logger.Error("failed to write record", sharedlog.F("error", err))
		return 0, err
	}

	return id, nil
}

func (in Ingester) WriteBatch(ctx context.Context, records []*storage.Record) ([]uint64, error) {
	written, skipped, err := in.storage.WriteBatch(ctx, records)
	if err != nil {
		in.logger.Error("failed to write batch", sharedlog.F("error", err))
		return nil, err
	}

	if skipped > 0 {
		in.logger.Debug("found duplicates in batch", sharedlog.F("skipped", skipped))
	}

	if skipped == len(records) {
		in.logger.Error("all records in batch were duplicates, nothing written")
		return nil, ErrAllSkipped
	}

	return written, err
}

func (in Ingester) FirstRecordID() uint64 {
	first, _ := in.storage.Range()

	return first
}
