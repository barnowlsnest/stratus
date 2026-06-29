package stream

import (
	"errors"
)

var (
	ErrNilIngester = errors.New("stream: nil ingester")
	ErrNilCache    = errors.New("stream: nil cache")
)
