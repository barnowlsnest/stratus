package storage

import (
	"errors"
)

var (
	ErrOutOfBounds   = errors.New("storage: requested range exceeds WAL bounds")
	ErrNilChunk      = errors.New("storage: nil chunk")
	ErrNilStorage    = errors.New("storage: nil storage")
	ErrNilOption     = errors.New("storage: nil config")
	ErrEmptyDedupKey = errors.New("storage: empty dedup key")
	ErrEmptyChunk    = errors.New("storage: empty chunk")
)
