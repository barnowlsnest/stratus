package tui

import (
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// pulseFrames animate the link indicator so the header shows the UI is live.
var pulseFrames = []string{"◢", "◣", "◤", "◥"}

func (m *Model) View() tea.View {
	if !m.ready {
		view := tea.NewView(m.styles.dim.Render("booting stratus console…"))
		decorate(&view)

		return view
	}

	body := lipgloss.JoinHorizontal(lipgloss.Top, m.streamPanel(), m.sidePanel())

	view := tea.NewView(lipgloss.JoinVertical(lipgloss.Left,
		m.header(),
		body,
		m.statusBar(),
		m.helpBar(),
	))
	decorate(&view)

	return view
}

// decorate applies the full-window, mouse and chrome settings shared by every
// frame of the console.
func decorate(view *tea.View) {
	view.AltScreen = true
	view.MouseMode = tea.MouseModeCellMotion
	view.WindowTitle = "stratus console"
	view.BackgroundColor = lipgloss.Color(colorVoid)
	view.ForegroundColor = lipgloss.Color(colorText)
}

func (m *Model) header() string {
	title := gradient(" S T R A T U S ", lipgloss.Color(colorMagenta), lipgloss.Color(colorViolet), lipgloss.Color(colorCyan))
	brand := m.styles.accent.Render("▛▚") + title + m.styles.accent.Render("▞▜")

	link, style := "OFFLINE", m.styles.err
	if m.online {
		link, style = "ONLINE", m.styles.ok
	}

	right := style.Render(pulseFrames[m.pulse%len(pulseFrames)]+" "+link) +
		m.styles.dim.Render(" ▏ ") +
		m.styles.label.Render(m.addr())

	gap := m.width - lipgloss.Width(brand) - lipgloss.Width(right)
	if gap < 1 {
		return fit(brand, m.width)
	}

	return brand + m.styles.rule.Render(strings.Repeat("─", gap)) + right
}

func (m *Model) streamPanel() string {
	mode := "PAUSED"
	if m.follow {
		mode = "LIVE"
	}

	title := fmt.Sprintf("STREAM ▏ %s ▏ rx %s ▏ %.1f rec/s", mode, humanCount(m.received), m.flux)

	return m.styles.panelBox(title, m.streamWidthOuter(), m.bodyHeight(), m.zone == zoneStream, m.viewer.View())
}

func (m *Model) sidePanel() string {
	outer := m.sideWidthOuter()
	inner := outer - 2

	infoOuter := m.infoHeightOuter()
	menuOuter := max(minPanelHeight, m.bodyHeight()-infoOuter)

	info := m.styles.panelBox("INFO ▏ LIVE", outer, infoOuter, false, m.infoBody(inner))

	title, focused := "COMMANDS", m.zone == zoneMenu
	body := m.menuBody(inner)
	if m.form != nil {
		title, focused = "COMMANDS ▏ INPUT", true
		body = m.form.view(&m.styles, inner-2)
	}

	return lipgloss.JoinVertical(lipgloss.Left, info, m.styles.panelBox(title, outer, menuOuter, focused, body))
}

func (m *Model) infoBody(width int) string {
	s := &m.styles

	row := func(label, value string) string {
		return s.label.Render(fit(label, 8)) + s.value.Render(fit(value, width-8))
	}

	cached, disk := m.info.CachedRecords, m.info.FSRecords

	var ratio float64
	if total := cached + disk; total > 0 {
		ratio = float64(cached) / float64(total)
	}

	held := uint64(0)
	if m.info.Range.End >= m.info.Range.Start && m.info.Range.End > 0 {
		held = m.info.Range.End - m.info.Range.Start + 1
	}

	updated := "—"
	if !m.updatedAt.IsZero() {
		updated = m.updatedAt.Format("15:04:05")
	}

	// 8 columns of label plus 5 of " 100%" bracket the meter.
	meterWidth := max(1, width-13)

	return lipgloss.JoinVertical(lipgloss.Left,
		row("RANGE", fmt.Sprintf("#%d → #%d", m.info.Range.Start, m.info.Range.End)),
		row("HELD", humanCount(held)),
		row("CACHE", humanCount(cached)),
		row("DISK", humanCount(disk)),
		s.label.Render(fit("SPLIT", 8))+s.meter(meterWidth, ratio)+s.value.Render(fmt.Sprintf(" %3.0f%%", ratio*100)),
		row("FLUX", fmt.Sprintf("%.1f rec/s", m.flux)),
		row("SYNC", updated+" · "+infoInterval.String()),
	)
}

func (m *Model) menuBody(width int) string {
	s := &m.styles
	rows := make([]string, 0, len(m.menu)*2+1)

	for i, item := range m.menu {
		style, marker := s.menuItem, "  "
		if i == m.sel {
			style, marker = s.menuOn, "▶ "
		}

		rows = append(rows,
			s.accent.Render(marker)+style.Render(fit(item.label, width-5))+s.menuKey.Render("["+item.key+"]"),
			s.dim.Render(fit("    "+item.hint, width)),
		)
	}

	rows = append(rows, s.dim.Render(fit("", width)))

	return lipgloss.JoinVertical(lipgloss.Left, rows...)
}

func (m *Model) statusBar() string {
	s := &m.styles

	style, marker := s.dim, "◇"
	switch m.status.level {
	case statusOK:
		style, marker = s.ok, "◆"
	case statusBusy:
		style, marker = s.warn, pulseFrames[m.pulse%len(pulseFrames)]
	case statusFail:
		style, marker = s.err, "✖"
	case statusIdle:
	}

	left := style.Render(marker + " " + m.status.text)
	right := s.dim.Render(fmt.Sprintf("buf %d/%d · %s", len(m.lines), maxViewerLines, time.Now().Format("15:04:05")))

	gap := m.width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		return fit(left, m.width)
	}

	return left + strings.Repeat(" ", gap) + right
}

func (m *Model) helpBar() string {
	s := &m.styles

	keys := [][2]string{
		{keyTab, "focus"},
		{"↑↓", "select"},
		{keyEnter, "run"},
		{"a/d/r", "append/delete/reconcile"},
		{keyFollow, "follow"},
		{keyClear, "clear"},
		{keyQuit, "quit"},
	}

	if m.form != nil {
		keys = [][2]string{
			{keyTab, "field"},
			{keyEnter, "submit"},
			{keyEscape, "abort"},
		}
	}

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, s.helpKey.Render(k[0])+s.help.Render(" "+k[1]))
	}

	return fit(strings.Join(parts, s.dim.Render("  ·  ")), m.width)
}
