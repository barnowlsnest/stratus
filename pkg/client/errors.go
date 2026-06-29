package client

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrNotFound is returned by Read when the server sends no entry for the id.
var ErrNotFound = errors.New("client: record not found")

// IsDuplicate reports whether err is a duplicate-key rejection.
func IsDuplicate(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

// IsNotFound reports whether err means the record does not exist.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound) || status.Code(err) == codes.NotFound
}
