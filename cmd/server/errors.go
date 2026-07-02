package server

import (
	"context"
	"errors"
	"io"

	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/storage"
	stratusv1 "github.com/barnowlsnest/stratus/pkg/client/gen/stratus/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNilStream         = errors.New("server: nil stream")
	ErrNoAddr            = errors.New("server: no listen address")
	ErrEmptyWriteRequest = errors.New("server: write request has no payload")
	ErrEmptyReadRequest  = errors.New("server: read request has no query")
)

// toStatus maps a domain error to a gRPC status error.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, dedup.ErrDuplicateChunk), errors.Is(err, ingester.ErrAllSkipped):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, storage.ErrOutOfBounds):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, io.EOF):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, storage.ErrTooLongRangeToRead):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrEmptyRecord),
		errors.Is(err, storage.ErrEmptyDedupKey),
		errors.Is(err, storage.ErrNilRecord),
		errors.Is(err, ErrEmptyWriteRequest),
		errors.Is(err, ErrEmptyReadRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// toError maps a recoverable domain error to an in-stream Error the client can
// act on without losing the stream. It returns nil for fatal errors — the
// caller must then terminate the RPC via toStatus. firstLSN/lastLSN are the
// WAL bounds at error time, attached for out-of-range and not-found so the
// client can correct its query.
func toError(err error, firstLSN, lastLSN uint64) *stratusv1.Error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, dedup.ErrDuplicateChunk), errors.Is(err, ingester.ErrAllSkipped):
		return &stratusv1.Error{
			Code:    stratusv1.Err_ERR_ALREADY_EXISTS,
			Message: err.Error(),
		}
	case errors.Is(err, storage.ErrOutOfBounds):
		return &stratusv1.Error{
			Code:     stratusv1.Err_ERR_OUT_OF_RANGE,
			Message:  err.Error(),
			FirstLsn: firstLSN,
			LastLsn:  lastLSN,
		}
	// Unreachable via the public API today: LSNs are gapless, so a past-tail id
	// hits storage.ErrOutOfBounds first. Kept for a genuine WAL read-miss.
	case errors.Is(err, io.EOF):
		return &stratusv1.Error{
			Code:     stratusv1.Err_ERR_NOT_FOUND,
			Message:  err.Error(),
			FirstLsn: firstLSN,
			LastLsn:  lastLSN,
		}
	case errors.Is(err, storage.ErrTooLongRangeToRead),
		errors.Is(err, storage.ErrEmptyRecord),
		errors.Is(err, storage.ErrEmptyDedupKey),
		errors.Is(err, storage.ErrNilRecord),
		errors.Is(err, ErrEmptyWriteRequest),
		errors.Is(err, ErrEmptyReadRequest):
		return &stratusv1.Error{
			Code:    stratusv1.Err_ERR_INVALID_ARGUMENT,
			Message: err.Error(),
		}
	default:
		return nil
	}
}
