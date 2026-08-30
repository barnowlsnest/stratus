package tui

import (
	"fmt"
	"strconv"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type commandKind int

const (
	cmdAppend commandKind = iota
	cmdDelete
	cmdReconcile
)

// Command labels, shared by the menu and the form header.
const (
	labelAppend    = "APPEND"
	labelDelete    = "DELETE"
	labelReconcile = "RECONCILE"
)

type menuItem struct {
	kind  commandKind
	key   string
	label string
	hint  string
}

func newMenu() []menuItem {
	return []menuItem{
		{kind: cmdAppend, key: "a", label: labelAppend, hint: "push a record onto the stream"},
		{kind: cmdDelete, key: "d", label: labelDelete, hint: "truncate the stream up to an id"},
		{kind: cmdReconcile, key: "r", label: labelReconcile, hint: "rebuild the in-memory cache"},
	}
}

type field struct {
	label       string
	placeholder string
	numeric     bool
	input       textinput.Model
}

// form collects the arguments of a command that needs them.
type form struct {
	kind    commandKind
	title   string
	fields  []field
	focused int
}

func newForm(kind commandKind, s *styles, width int) *form {
	var f form

	switch kind {
	case cmdAppend:
		f = form{
			kind:  kind,
			title: labelAppend,
			fields: []field{
				{label: "DEDUP KEY", placeholder: "uint64", numeric: true},
				{label: "PAYLOAD", placeholder: "raw data"},
			},
		}
	case cmdDelete:
		f = form{
			kind:  kind,
			title: labelDelete,
			fields: []field{
				{label: "END ID", placeholder: "truncate up to id", numeric: true},
			},
		}
	case cmdReconcile:
		return nil
	}

	inputStyles := textinput.DefaultDarkStyles()
	inputStyles.Focused.Prompt = s.accent
	inputStyles.Focused.Text = s.value
	inputStyles.Focused.Placeholder = s.dim
	inputStyles.Blurred.Prompt = s.dim
	inputStyles.Blurred.Text = s.menuItem
	inputStyles.Blurred.Placeholder = s.dim
	inputStyles.Cursor.Color = lipgloss.Color(colorCyan)

	for i := range f.fields {
		input := textinput.New()
		input.Prompt = "▓ "
		input.Placeholder = f.fields[i].placeholder
		input.SetStyles(inputStyles)
		input.SetWidth(width)
		f.fields[i].input = input
	}

	f.fields[0].input.Focus()

	return &f
}

func (f *form) next() {
	f.fields[f.focused].input.Blur()
	f.focused = (f.focused + 1) % len(f.fields)
	f.fields[f.focused].input.Focus()
}

func (f *form) prev() {
	f.fields[f.focused].input.Blur()
	f.focused = (f.focused - 1 + len(f.fields)) % len(f.fields)
	f.fields[f.focused].input.Focus()
}

func (f *form) update(msg tea.Msg) tea.Cmd {
	input, cmd := f.fields[f.focused].input.Update(msg)
	f.fields[f.focused].input = input

	return cmd
}

// values validates the form and returns the raw field values in order.
func (f *form) values() (values []string, err error) {
	values = make([]string, 0, len(f.fields))

	for i := range f.fields {
		value := strings.TrimSpace(f.fields[i].input.Value())
		if value == "" {
			return nil, fmt.Errorf("%s is required", strings.ToLower(f.fields[i].label))
		}

		if f.fields[i].numeric {
			if _, err = strconv.ParseUint(value, 10, 64); err != nil {
				return nil, fmt.Errorf("%s must be a uint64", strings.ToLower(f.fields[i].label))
			}
		}

		values = append(values, value)
	}

	return values, nil
}

func (f *form) view(s *styles, width int) string {
	rows := make([]string, 0, len(f.fields)*3+2)
	rows = append(rows, s.formLabel.Render("┤ "+f.title+" ├"), "")

	for i := range f.fields {
		marker := s.dim.Render("  ")
		if i == f.focused {
			marker = s.accent.Render("▶ ")
		}

		rows = append(rows,
			marker+s.label.Render(f.fields[i].label),
			"  "+f.fields[i].input.View(),
			"",
		)
	}

	rows = append(rows, s.formHint.Render(fit("enter run · tab field · esc abort", width)))

	return lipgloss.JoinVertical(lipgloss.Left, rows...)
}
