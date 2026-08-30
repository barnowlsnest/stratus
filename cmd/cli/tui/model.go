package tui

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/barnowlsnest/stratus/cmd/cli/options"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

const (
	// maxViewerLines is the depth of the scrollback kept by the viewer.
	maxViewerLines = 5000
	// sideWidth is the outer width of the right hand column.
	sideWidth = 38
	// infoHeight is the outer height of the info panel.
	infoHeight = 11
	// minPanelWidth and minPanelHeight keep the panels renderable on small
	// terminals, even when the frame then overflows the window.
	minPanelWidth  = 24
	minPanelHeight = 5
)

// Key names routed by the console.
const (
	keyQuit      = "q"
	keyInterrupt = "ctrl+c"
	keyEscape    = "esc"
	keyTab       = "tab"
	keyShiftTab  = "shift+tab"
	keyUp        = "up"
	keyDown      = "down"
	keyEnter     = "enter"
	keyEnd       = "end"
	keyBottom    = "G"
	keyFollow    = "f"
	keyClear     = "c"
)

type zone int

const (
	zoneStream zone = iota
	zoneMenu
)

type statusLevel int

const (
	statusIdle statusLevel = iota
	statusOK
	statusBusy
	statusFail
)

type status struct {
	text  string
	level statusLevel
}

// Model is the root Bubble Tea model of the stratus TUI: a live stream viewer
// on the left, live stream info and the command menu on the right.
type Model struct {
	// ctx yields the context every server call is issued under, resolved at
	// call time rather than stored on the model.
	ctx    func() context.Context
	client *stratusv1.Client
	opts   *options.Options
	styles styles

	width  int
	height int
	ready  bool

	viewer viewport.Model
	lines  []string
	follow bool

	info      stratusv1.StreamInfo
	online    bool
	updatedAt time.Time
	received  uint64
	window    uint64
	flux      float64

	menu   []menuItem
	sel    int
	zone   zone
	form   *form
	status status

	tail    <-chan tailEvent
	tailing bool
	pulse   int
}

func NewModel(ctx context.Context, client *stratusv1.Client, opts *options.Options) *Model {
	m := &Model{
		ctx:    func() context.Context { return ctx },
		client: client,
		opts:   opts,
		styles: newStyles(),
		viewer: viewport.New(),
		follow: true,
		menu:   newMenu(),
	}
	m.status = status{text: "linking to " + m.addr(), level: statusBusy}

	return m
}

// addr is the stratus endpoint the console is attached to.
func (m *Model) addr() string {
	return net.JoinHostPort(m.opts.Host, strconv.Itoa(m.opts.Port))
}

func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		fetchInfoCmd(m.ctx(), m.client),
		tickCmd(),
	)
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.resize(msg.Width, msg.Height)
		return m, nil

	case tea.KeyPressMsg:
		cmd := m.handleKey(msg)

		return m, cmd

	case tea.MouseWheelMsg:
		if msg.Button == tea.MouseWheelUp {
			m.follow = false
		}

		viewer, cmd := m.viewer.Update(msg)
		m.viewer = viewer

		return m, cmd

	case tickMsg:
		m.flux = float64(m.window) / infoInterval.Seconds()
		m.window = 0
		m.pulse++

		return m, tea.Batch(fetchInfoCmd(m.ctx(), m.client), tickCmd())

	case infoMsg:
		cmd := m.handleInfo(msg)

		return m, cmd

	case seedMsg:
		cmd := m.handleSeed(msg)

		return m, cmd

	case tailMsg:
		cmd := m.handleTail(msg)

		return m, cmd

	case tailDoneMsg:
		m.tailing = false
		m.append(eventLine(&m.styles, &m.styles.warn, time.Now(), "live tail stopped"))

		return m, nil

	case addMsg:
		cmd := m.handleAdd(msg)

		return m, cmd

	case deleteMsg:
		cmd := m.handleDelete(msg)

		return m, cmd

	case reconcileMsg:
		cmd := m.handleReconcile(msg)

		return m, cmd
	}

	return m, nil
}

func (m *Model) handleAdd(msg addMsg) tea.Cmd {
	if msg.err != nil {
		m.status = status{text: "append failed: " + reason(msg.err), level: statusFail}

		return nil
	}

	m.status = status{
		text:  fmt.Sprintf("appended #%d (%d duplicate)", msg.res.AddedRecords.End, msg.res.DuplicateRecords),
		level: statusOK,
	}

	return fetchInfoCmd(m.ctx(), m.client)
}

func (m *Model) handleDelete(msg deleteMsg) tea.Cmd {
	if msg.err != nil {
		m.status = status{text: "delete failed: " + reason(msg.err), level: statusFail}

		return nil
	}

	m.status = status{
		text:  fmt.Sprintf("deleted #%d → #%d", msg.res.DeletedRecords.Start, msg.res.DeletedRecords.End),
		level: statusOK,
	}

	// The deleted records are gone from the stream, so snap back to the live end
	// and keep showing what the stream actually holds.
	m.append(eventLine(&m.styles, &m.styles.warn, time.Now(),
		fmt.Sprintf("deleted #%d → #%d ▏ stream now #%d → #%d",
			msg.res.DeletedRecords.Start, msg.res.DeletedRecords.End,
			msg.res.StreamRecords.Start, msg.res.StreamRecords.End)))
	m.refollow()

	return fetchInfoCmd(m.ctx(), m.client)
}

func (m *Model) handleReconcile(msg reconcileMsg) tea.Cmd {
	if msg.err != nil {
		m.status = status{text: "reconcile failed: " + reason(msg.err), level: statusFail}

		return nil
	}

	m.status = status{
		text:  fmt.Sprintf("cache reconciled #%d → #%d", msg.rng.Start, msg.rng.End),
		level: statusOK,
	}

	return fetchInfoCmd(m.ctx(), m.client)
}

func (m *Model) handleInfo(msg infoMsg) tea.Cmd {
	if msg.err != nil {
		if m.online {
			m.append(eventLine(&m.styles, &m.styles.err, time.Now(), "link lost: "+reason(msg.err)))
		}

		m.online = false

		return nil
	}

	first := !m.online
	m.online = true
	m.info = msg.info
	m.updatedAt = time.Now()

	if first && !m.tailing {
		m.tailing = true
		m.status = status{text: "linked to " + m.addr(), level: statusOK}

		return seedCmd(m.ctx(), m.client, msg.info.Range)
	}

	return nil
}

func (m *Model) handleSeed(msg seedMsg) tea.Cmd {
	if msg.err != nil {
		m.append(eventLine(&m.styles, &m.styles.warn, time.Now(), "history unavailable: "+reason(msg.err)))
	}

	now := time.Now()
	for _, record := range msg.records {
		m.append(recordLine(&m.styles, record, now))
	}

	m.append(eventLine(&m.styles, &m.styles.ok, now, fmt.Sprintf("live tail from #%d", msg.nextID)))

	m.tail = startTail(m.ctx(), m.client, msg.nextID)

	return waitTailCmd(m.tail)
}

func (m *Model) handleTail(msg tailMsg) tea.Cmd {
	if msg.event.err != nil {
		m.append(eventLine(&m.styles, &m.styles.err, time.Now(), "tail: "+reason(msg.event.err)))

		return waitTailCmd(m.tail)
	}

	m.received++
	m.window++
	m.append(recordLine(&m.styles, msg.event.record, time.Now()))

	return waitTailCmd(m.tail)
}

func (m *Model) handleKey(msg tea.KeyPressMsg) tea.Cmd {
	key := msg.String()

	if m.form != nil {
		return m.handleFormKey(msg, key)
	}

	switch key {
	case keyInterrupt, keyQuit, keyEscape:
		return tea.Quit
	case keyTab, keyShiftTab:
		m.zone = 1 - m.zone
		return nil
	case "a":
		return m.launch(cmdAppend)
	case "d":
		return m.launch(cmdDelete)
	case "r":
		return m.launch(cmdReconcile)
	case keyFollow:
		m.follow = !m.follow
		if m.follow {
			m.viewer.GotoBottom()
		}

		return nil
	case keyClear:
		m.lines = nil
		m.viewer.SetContentLines(nil)

		return nil
	}

	if m.zone == zoneMenu {
		switch key {
		case keyUp, "k":
			m.sel = (m.sel - 1 + len(m.menu)) % len(m.menu)
			return nil
		case keyDown, "j":
			m.sel = (m.sel + 1) % len(m.menu)
			return nil
		case keyEnter:
			return m.launch(m.menu[m.sel].kind)
		}

		return nil
	}

	if key == keyEnd || key == keyBottom {
		m.refollow()

		return nil
	}

	m.follow = false

	viewer, cmd := m.viewer.Update(msg)
	m.viewer = viewer

	return cmd
}

func (m *Model) handleFormKey(msg tea.KeyPressMsg, key string) tea.Cmd {
	switch key {
	case keyEscape, keyInterrupt:
		m.form = nil
		m.status = status{text: "aborted", level: statusIdle}

		return nil
	case keyTab, keyDown:
		m.form.next()
		return nil
	case keyShiftTab, keyUp:
		m.form.prev()
		return nil
	case keyEnter:
		return m.submit()
	}

	return m.form.update(msg)
}

// launch runs a command outright, or opens the form collecting its arguments.
func (m *Model) launch(kind commandKind) tea.Cmd {
	m.zone = zoneMenu

	for i, item := range m.menu {
		if item.kind == kind {
			m.sel = i
		}
	}

	if kind == cmdReconcile {
		m.status = status{text: "reconciling cache…", level: statusBusy}

		return reconcileCmd(m.ctx(), m.client)
	}

	m.form = newForm(kind, &m.styles, m.formWidth())
	m.status = status{text: "awaiting input", level: statusIdle}

	return nil
}

func (m *Model) submit() tea.Cmd {
	values, err := m.form.values()
	if err != nil {
		m.status = status{text: err.Error(), level: statusFail}

		return nil
	}

	kind := m.form.kind
	m.form = nil

	switch kind {
	case cmdAppend:
		key, _ := strconv.ParseUint(values[0], 10, 64)
		m.status = status{text: "appending…", level: statusBusy}

		return addCmd(m.ctx(), m.client, key, values[1])
	case cmdDelete:
		endID, _ := strconv.ParseUint(values[0], 10, 64)
		m.status = status{text: "deleting…", level: statusBusy}

		return deleteCmd(m.ctx(), m.client, endID)
	case cmdReconcile:
	}

	return nil
}

// append adds a rendered row to the scrollback, trimming the oldest rows.
func (m *Model) append(line string) {
	m.lines = append(m.lines, line)
	if len(m.lines) > maxViewerLines {
		m.lines = m.lines[len(m.lines)-maxViewerLines:]
	}

	m.viewer.SetContentLines(m.lines)

	if m.follow {
		m.viewer.GotoBottom()
	}
}

// refollow resumes live follow and jumps to the newest row.
func (m *Model) refollow() {
	m.follow = true
	m.viewer.GotoBottom()
}

func (m *Model) resize(width, height int) {
	m.width, m.height, m.ready = width, height, true

	m.viewer.SetWidth(m.streamWidthOuter() - 2)
	m.viewer.SetHeight(max(1, m.bodyHeight()-3))
	m.viewer.SetContentLines(m.lines)

	if m.form != nil {
		for i := range m.form.fields {
			m.form.fields[i].input.SetWidth(m.formWidth())
		}
	}

	if m.follow {
		m.viewer.GotoBottom()
	}
}

// bodyHeight is the height left for the panels once the header, the status bar
// and the help bar are taken out.
func (m *Model) bodyHeight() int {
	return max(minPanelHeight, m.height-3)
}

// sideWidthOuter is the outer width of the right hand column, borders included.
func (m *Model) sideWidthOuter() int {
	if m.width < sideWidth+minPanelWidth {
		return max(minPanelWidth, m.width/2)
	}

	return sideWidth
}

// streamWidthOuter is the outer width of the stream viewer panel.
func (m *Model) streamWidthOuter() int {
	return max(minPanelWidth, m.width-m.sideWidthOuter())
}

// infoHeightOuter is the outer height of the info panel; the command panel
// takes whatever is left of the column.
func (m *Model) infoHeightOuter() int {
	return min(infoHeight, max(minPanelHeight, m.bodyHeight()-minPanelHeight))
}

func (m *Model) formWidth() int {
	return max(8, m.sideWidthOuter()-6)
}
