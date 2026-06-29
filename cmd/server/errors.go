package server

import (
	"context"
	"errors"
	"io"

	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/storage"
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
