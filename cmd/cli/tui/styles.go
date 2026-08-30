package tui

import (
	"image/color"
	"strings"

	"charm.land/lipgloss/v2"
)

// Neo-cyberpunk palette: neon magenta and cyan over a near-black violet void.
const (
	colorVoid    = "#08000f"
	colorPanel   = "#150033"
	colorMagenta = "#ff2bd6"
	colorCyan    = "#00f0ff"
	colorViolet  = "#a06bff"
	colorLime    = "#8bff3f"
	colorAmber   = "#ffb000"
	colorRed     = "#ff2e63"
	colorDim     = "#6a4b9c"
	colorText    = "#d9e7ff"
)

// Border is a squared-off frame; the "neon tube" look comes from the colors,
// not from heavy box drawing.
var neonBorder = lipgloss.Border{
	Top:         "─",
	Bottom:      "─",
	Left:        "│",
	Right:       "│",
	TopLeft:     "┌",
	TopRight:    "┐",
	BottomLeft:  "└",
	BottomRight: "┘",
}

type styles struct {
	app        lipgloss.Style
	panel      lipgloss.Style
	panelOn    lipgloss.Style
	title      lipgloss.Style
	titleOn    lipgloss.Style
	rule       lipgloss.Style
	label      lipgloss.Style
	value      lipgloss.Style
	accent     lipgloss.Style
	dim        lipgloss.Style
	ok         lipgloss.Style
	warn       lipgloss.Style
	err        lipgloss.Style
	menuItem   lipgloss.Style
	menuOn     lipgloss.Style
	menuKey    lipgloss.Style
	recordID   lipgloss.Style
	recordTime lipgloss.Style
	recordData lipgloss.Style
	recordMark lipgloss.Style
	formLabel  lipgloss.Style
	formHint   lipgloss.Style
	help       lipgloss.Style
	helpKey    lipgloss.Style
	barOn      lipgloss.Style
	barOff     lipgloss.Style
}

func newStyles() styles {
	base := lipgloss.NewStyle().Background(lipgloss.Color(colorVoid))

	panel := base.
		Border(neonBorder).
		BorderForeground(lipgloss.Color(colorDim)).
		BorderBackground(lipgloss.Color(colorVoid))

	return styles{
		app:        base.Foreground(lipgloss.Color(colorText)),
		panel:      panel,
		panelOn:    panel.BorderForeground(lipgloss.Color(colorMagenta)),
		title:      base.Foreground(lipgloss.Color(colorViolet)).Bold(true),
		titleOn:    base.Foreground(lipgloss.Color(colorCyan)).Bold(true),
		rule:       base.Foreground(lipgloss.Color(colorDim)),
		label:      base.Foreground(lipgloss.Color(colorViolet)),
		value:      base.Foreground(lipgloss.Color(colorText)).Bold(true),
		accent:     base.Foreground(lipgloss.Color(colorMagenta)).Bold(true),
		dim:        base.Foreground(lipgloss.Color(colorDim)),
		ok:         base.Foreground(lipgloss.Color(colorLime)),
		warn:       base.Foreground(lipgloss.Color(colorAmber)),
		err:        base.Foreground(lipgloss.Color(colorRed)).Bold(true),
		menuItem:   base.Foreground(lipgloss.Color(colorText)),
		menuOn:     base.Foreground(lipgloss.Color(colorVoid)).Background(lipgloss.Color(colorMagenta)).Bold(true),
		menuKey:    base.Foreground(lipgloss.Color(colorCyan)),
		recordID:   base.Foreground(lipgloss.Color(colorMagenta)),
		recordTime: base.Foreground(lipgloss.Color(colorDim)),
		recordData: base.Foreground(lipgloss.Color(colorText)),
		recordMark: base.Foreground(lipgloss.Color(colorCyan)),
		formLabel:  base.Foreground(lipgloss.Color(colorCyan)).Bold(true),
		formHint:   base.Foreground(lipgloss.Color(colorDim)).Italic(true),
		help:       base.Foreground(lipgloss.Color(colorDim)),
		helpKey:    base.Foreground(lipgloss.Color(colorViolet)).Bold(true),
		barOn:      base.Foreground(lipgloss.Color(colorMagenta)),
		barOff:     base.Foreground(lipgloss.Color(colorPanel)),
	}
}

// panelBox frames body in a titled panel of the given outer size, borders
// included, clipping whatever does not fit.
func (s *styles) panelBox(title string, width, height int, focused bool, body string) string {
	frame, titleStyle := s.panel, s.title
	if focused {
		frame, titleStyle = s.panelOn, s.titleOn
	}

	inner := max(1, width-2)

	header := titleStyle.Render("▛ " + title + " ")
	if fill := inner - lipgloss.Width(header); fill > 0 {
		header += s.rule.Render(strings.Repeat("─", fill))
	}

	content := lipgloss.NewStyle().
		MaxWidth(inner).
		MaxHeight(max(1, height-2)).
		Render(lipgloss.JoinVertical(lipgloss.Left, header, body))

	return frame.Width(width).Height(height).Render(content)
}

// gradient paints text across a smooth blend of the given stops.
func gradient(text string, stops ...color.Color) string {
	runes := []rune(text)
	if len(runes) < 2 || len(stops) < 2 {
		return text
	}

	colors := lipgloss.Blend1D(len(runes), stops...)

	var b strings.Builder
	for i, r := range runes {
		b.WriteString(lipgloss.NewStyle().
			Foreground(colors[i]).
			Background(lipgloss.Color(colorVoid)).
			Bold(true).
			Render(string(r)))
	}

	return b.String()
}

// meter renders a neon progress bar of the given width.
func (s *styles) meter(width int, ratio float64) string {
	if width <= 0 {
		return ""
	}

	switch {
	case ratio < 0:
		ratio = 0
	case ratio > 1:
		ratio = 1
	}

	on := int(ratio * float64(width))

	return s.barOn.Render(strings.Repeat("█", on)) +
		s.dim.Render(strings.Repeat("╌", width-on))
}
