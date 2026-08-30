package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

// maxPayloadRunes bounds how much of a record payload the viewer prints.
const maxPayloadRunes = 512

// fit pads or truncates s to exactly width visible cells.
func fit(s string, width int) string {
	if width <= 0 {
		return ""
	}

	w := lipgloss.Width(s)
	switch {
	case w == width:
		return s
	case w < width:
		return s + strings.Repeat(" ", width-w)
	default:
		return ansi.Truncate(s, width, "…")
	}
}

// sanitize makes a raw payload safe to print on a single line.
func sanitize(data []byte) string {
	runes := []rune(string(data))
	if len(runes) > maxPayloadRunes {
		runes = append(runes[:maxPayloadRunes:maxPayloadRunes], '…')
	}

	var b strings.Builder
	for _, r := range runes {
		if r == '…' || unicode.IsPrint(r) {
			b.WriteRune(r)
			continue
		}

		b.WriteRune('·')
	}

	return b.String()
}

// recordLine renders one stream record as a viewer row.
func recordLine(s *styles, record stratusv1.OutputRecord, at time.Time) string {
	return s.recordTime.Render(at.Format("15:04:05.000")) + " " +
		s.recordID.Render(fmt.Sprintf("#%08d", record.ID)) + " " +
		s.recordMark.Render("▸ ") +
		s.recordData.Render(sanitize(record.RawData))
}

// eventLine renders a non-record row, such as a tail error.
func eventLine(s *styles, style *lipgloss.Style, at time.Time, text string) string {
	return s.recordTime.Render(at.Format("15:04:05.000")) + " " +
		s.dim.Render("········") + " " +
		s.recordMark.Render("▪ ") +
		style.Render(text)
}

// reason unwraps a gRPC error into the bare server message, so the status bar
// is not filled with transport boilerplate.
func reason(err error) string {
	if err == nil {
		return ""
	}

	if st, ok := grpcstatus.FromError(err); ok {
		return st.Message()
	}

	return err.Error()
}

// humanCount abbreviates large record counts so the info panel stays narrow.
func humanCount(n uint64) string {
	switch {
	case n < 1_000:
		return strconv.FormatUint(n, 10)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
}
