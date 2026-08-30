package tui

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

const (
	// infoInterval is how often the info panel and the throughput meter refresh.
	infoInterval = time.Second
	// callTimeout bounds a single command issued from the menu.
	callTimeout = 5 * time.Second
	// seedRecords is how much history the viewer shows on start.
	seedRecords = 200
)

func tickCmd() tea.Cmd {
	return tea.Tick(infoInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func fetchInfoCmd(ctx context.Context, client *stratusv1.Client) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		defer cancel()

		info, err := client.GetStreamInfo(callCtx)

		return infoMsg{info: info, err: err}
	}
}

// seedCmd loads the last records of the stream so the viewer is not empty
// before the first live append arrives.
func seedCmd(ctx context.Context, client *stratusv1.Client, rng stratusv1.Range) tea.Cmd {
	return func() tea.Msg {
		if rng.End == 0 {
			return seedMsg{nextID: 1}
		}

		startID := rng.Start
		if rng.End >= seedRecords && rng.End-seedRecords+1 > startID {
			startID = rng.End - seedRecords + 1
		}
		if startID == 0 {
			startID = 1
		}

		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		defer cancel()

		records, err := client.ReadRange(callCtx, startID, rng.End, callTimeout)
		if err != nil {
			return seedMsg{nextID: rng.End + 1, err: err}
		}

		return seedMsg{records: records, nextID: rng.End + 1}
	}
}

func waitTailCmd(events <-chan tailEvent) tea.Cmd {
	return func() tea.Msg {
		event, ok := <-events
		if !ok {
			return tailDoneMsg{}
		}

		return tailMsg{event: event}
	}
}

func addCmd(ctx context.Context, client *stratusv1.Client, key uint64, data string) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		defer cancel()

		res, err := client.Add(callCtx, []stratusv1.InputRecord{{DedupKey: key, RawData: []byte(data)}})

		return addMsg{res: res, err: err}
	}
}

func deleteCmd(ctx context.Context, client *stratusv1.Client, endID uint64) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		defer cancel()

		res, err := client.Delete(callCtx, endID)

		return deleteMsg{res: res, err: err}
	}
}

func reconcileCmd(ctx context.Context, client *stratusv1.Client) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		defer cancel()

		rng, err := client.ReconcileCache(callCtx)

		return reconcileMsg{rng: rng, err: err}
	}
}
