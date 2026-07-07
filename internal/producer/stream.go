package producer

import (
	"context"

	"github.com/barnowlsnest/stratus/internal/storage"
)

type Stream interface {
	Range(ctx context.Context, fromID, toID uint64) ([]*storage.Record, error)
}
