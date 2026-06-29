package preloader

import (
	"errors"
)

var (
	ErrNotStarted      = errors.New("preloader: not started")
	ErrAlreadyStarted  = errors.New("preloader: already started")
	ErrMissingAppender = errors.New("preloader: nil appender")
	ErrMissingCache    = errors.New("preloader: nil cache")
)
