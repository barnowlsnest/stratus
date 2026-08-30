package tui

import (
	"context"
	"time"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

const (
	// tailBatch is how many records a single ReadOffset subscription may carry
	// before the tailer resubscribes. It also sizes the client-side buffer, so
	// it stays modest.
	tailBatch = 512
	// tailTimeout is the server-side lifetime of one subscription.
	tailTimeout = 5 * time.Minute
	// tailRetry is the pause before resubscribing after an empty or failed read,
	// so a stream that rejects the cursor does not spin.
	tailRetry = 500 * time.Millisecond
)

// tailEvent is one live record, or the error that interrupted the tail.
type tailEvent struct {
	record stratusv1.OutputRecord
	err    error
}

// startTail follows the stream from startID and emits every record it sees
// until ctx is done. It resubscribes when a subscription ends and rewinds the
// cursor to the first surviving record when the stream is truncated under it.
func startTail(ctx context.Context, client *stratusv1.Client, startID uint64) <-chan tailEvent {
	events := make(chan tailEvent)

	go func() {
		defer close(events)

		nextID := startID
		if nextID == 0 {
			nextID = 1
		}

		for ctx.Err() == nil {
			read, err := client.ReadOffset(ctx, nextID, tailBatch, tailTimeout)
			if err != nil {
				if !emit(ctx, events, tailEvent{err: err}) || !pause(ctx, tailRetry) {
					return
				}

				nextID = rewind(ctx, client, nextID)

				continue
			}

			var got bool
			for record := range read {
				got = true
				nextID = record.ID + 1
				if !emit(ctx, events, tailEvent{record: record}) {
					return
				}
			}

			// An immediately closed subscription means the cursor is outside the
			// stream, most often after a delete: back off and realign it.
			if !got {
				if !pause(ctx, tailRetry) {
					return
				}

				nextID = rewind(ctx, client, nextID)
			}
		}
	}()

	return events
}

func emit(ctx context.Context, events chan<- tailEvent, event tailEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case events <- event:
		return true
	}
}

func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// rewind clamps the cursor to the range the stream currently holds.
func rewind(ctx context.Context, client *stratusv1.Client, nextID uint64) uint64 {
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	info, err := client.GetStreamInfo(callCtx)
	if err != nil {
		return nextID
	}

	if info.Range.Start > nextID {
		return info.Range.Start
	}

	return nextID
}
