package client

import (
	"errors"

	stratusv1 "github.com/barnowlsnest/stratus/pkg/client/gen/stratus/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrNotFound is returned by Read when the record does not exist.
	ErrNotFound = errors.New("client: record not found")
	// ErrOutOfRange is returned when a query exceeds the available WAL bounds.
	ErrOutOfRange = errors.New("client: query out of range")
	// ErrAlreadyExists is returned when a write is rejected as a duplicate.
	ErrAlreadyExists = errors.New("client: record already exists")
	// ErrInvalidArgument is returned when the server rejects a malformed request.
	ErrInvalidArgument = errors.New("client: invalid argument")
)

// Error is a recoverable server-side failure of a single request. The
// underlying stream stays connected and usable. First/Last carry the available
// WAL bounds when the server attached them (out-of-range and not-found).
type Error struct {
	Code    stratusv1.Err
	Message string
	First   uint64
	Last    uint64
}

// Error returns the server-provided message.
func (e *Error) Error() string {
	return e.Message
}

// Is matches the package sentinel for the error's code, so callers can use
// errors.Is(err, client.ErrOutOfRange) and friends.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrNotFound:
		return e.Code == stratusv1.Err_ERR_NOT_FOUND
	case ErrOutOfRange:
		return e.Code == stratusv1.Err_ERR_OUT_OF_RANGE
	case ErrAlreadyExists:
		return e.Code == stratusv1.Err_ERR_ALREADY_EXISTS
	case ErrInvalidArgument:
		return e.Code == stratusv1.Err_ERR_INVALID_ARGUMENT
	default:
		return false
	}
}

// newError converts an in-stream protobuf error to a typed client error.
func newError(pb *stratusv1.Error) *Error {
	return &Error{
		Code:    pb.GetCode(),
		Message: pb.GetMessage(),
		First:   pb.GetFirstLsn(),
		Last:    pb.GetLastLsn(),
	}
}

// IsDuplicate reports whether err is a duplicate-key rejection.
func IsDuplicate(err error) bool {
	return errors.Is(err, ErrAlreadyExists) || status.Code(err) == codes.AlreadyExists
}

// IsNotFound reports whether err means the record does not exist.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound) || status.Code(err) == codes.NotFound
}
