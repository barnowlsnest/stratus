package stream

import (
	"errors"
)

var (
	ErrAlreadyStarted = errors.New("stream: already started")
	ErrAllSkipped     = errors.New("stream: all records skipped as duplicates")
	ErrNilStorage     = errors.New("stream: nil storage")
	ErrNilCache       = errors.New("stream: nil cache")
)
