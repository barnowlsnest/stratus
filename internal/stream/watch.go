package stream

import (
	"slices"
	"sync"
	"sync/atomic"
)

// watchers holds one wake channel per tail. Each tail waits on a channel of its
// own: when every tail waited on one shared channel that notify closed, each
// woken tail locked that channel again on its way out of select, and with 512
// tails that lock was 19% of the server's CPU.
type watchers struct {
	// list is replaced, never changed in place, so wake reads it without a lock.
	list atomic.Pointer[[]chan struct{}]
	mu   sync.Mutex
}

func (w *watchers) add(wake chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var current []chan struct{}
	if list := w.list.Load(); list != nil {
		current = *list
	}

	// Clip so append copies instead of writing into the list wake may be reading.
	next := append(slices.Clip(current), wake)
	w.list.Store(&next)
}

func (w *watchers) remove(wake chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	list := w.list.Load()
	if list == nil {
		return
	}

	next := slices.DeleteFunc(slices.Clone(*list), func(c chan struct{}) bool { return c == wake })
	w.list.Store(&next)
}

// wake leaves one wake-up in each tail's channel without blocking. A tail that
// already has one pending needs no second: it reads everything new when it wakes.
func (w *watchers) wake() {
	list := w.list.Load()
	if list == nil {
		return
	}

	for _, wake := range *list {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// Watch returns a channel that receives a value after the stream has cached new
// records or deleted some, and a function to stop watching. Watch before the
// first read: a change after Watch is never missed, because the channel keeps
// one pending wake-up until it is read. A wake-up can be left over from a change
// already read, so a read after one may find nothing new. Call stop when done
// so the stream stops waking the channel; calling it more than once is safe.
func (s *Stream) Watch() (wake <-chan struct{}, stop func()) {
	ch := make(chan struct{}, 1)
	s.watchers.add(ch)

	var once sync.Once

	return ch, func() { once.Do(func() { s.watchers.remove(ch) }) }
}
