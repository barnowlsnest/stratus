package dedup

import (
	"errors"
)

var ErrDuplicateChunk = errors.New("dedup: err")
