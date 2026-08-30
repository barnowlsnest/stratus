package tui

import (
	"time"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

// infoMsg carries a fresh snapshot of the stream state.
type infoMsg struct {
	info stratusv1.StreamInfo
	err  error
}

// seedMsg carries the tail of the stream shown when the viewer starts, and the
// id the live tail must resume from.
type seedMsg struct {
	records []stratusv1.OutputRecord
	nextID  uint64
	err     error
}

// tailMsg carries one event of the live tail.
type tailMsg struct {
	event tailEvent
}

// tailDoneMsg is delivered when the live tail stops for good.
type tailDoneMsg struct{}

// addMsg, deleteMsg and reconcileMsg report the outcome of a menu command.
type addMsg struct {
	res stratusv1.AddResult
	err error
}

type deleteMsg struct {
	res stratusv1.DeleteResult
	err error
}

type reconcileMsg struct {
	rng stratusv1.Range
	err error
}

// tickMsg drives the info refresh and the throughput meter.
type tickMsg time.Time
