package ingester

import (
	"errors"
)

var (
	ErrNilStorage = errors.New("ingester: nil storage")
	ErrAllSkipped = errors.New("ingester: all records skipped")
)
