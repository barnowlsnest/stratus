package storage

import (
	"errors"
)

var (
	ErrOutOfBounds        = errors.New("storage: requested range exceeds WAL bounds")
	ErrNilRecord          = errors.New("storage: nil chunk")
	ErrNilStorage         = errors.New("storage: nil storage")
	ErrNilOption          = errors.New("storage: nil config")
	ErrEmptyDedupKey      = errors.New("storage: empty dedup key")
	ErrEmptyRecord        = errors.New("storage: empty record")
	ErrTooLongRangeToRead = errors.New("storage: requested range exceeds max read size")
)
