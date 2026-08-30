package tui

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/stratus/cmd/cli/options"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

type ViewSuite struct {
	suite.Suite
}

func TestViewSuite(t *testing.T) {
	suite.Run(t, new(ViewSuite))
}

func (s *ViewSuite) newModel(width, height int) *Model {
	m := NewModel(s.T().Context(), nil, &options.Options{Host: "127.0.0.1", Port: 8000})
	m.info = stratusv1.StreamInfo{
		Range:         stratusv1.Range{Start: 12, End: 4096},
		CachedRecords: 3200,
		FSRecords:     885,
	}
	m.online = true
	m.updatedAt = time.Unix(0, 0)

	_, _ = m.Update(tea.WindowSizeMsg{Width: width, Height: height})

	for i := range 5 {
		m.append(recordLine(&m.styles, stratusv1.OutputRecord{
			ID:      uint64(4092 + i),
			RawData: []byte("payload"),
		}, time.Unix(0, 0)))
	}

	return m
}

// The frame must fill the terminal exactly: a taller or wider frame scrolls the
// alt screen and smears the panels.
func (s *ViewSuite) TestFrameFitsTerminal() {
	cases := []struct {
		name   string
		width  int
		height int
	}{
		{name: "wide", width: 160, height: 48},
		{name: "typical", width: 100, height: 30},
		{name: "short", width: 100, height: 14},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			m := s.newModel(tc.width, tc.height)

			actual := m.View().Content

			s.Equal(tc.height, lipgloss.Height(actual))
			s.LessOrEqual(lipgloss.Width(actual), tc.width)
		})
	}
}

func (s *ViewSuite) TestPanelsAndMenuRendered() {
	m := s.newModel(120, 32)

	actual := m.View().Content

	for _, expected := range []string{"STREAM", "INFO", "COMMANDS", "APPEND", "DELETE", "RECONCILE", "#00004092"} {
		s.Contains(actual, expected)
	}
}

func (s *ViewSuite) TestFormOpensAndValidates() {
	m := s.newModel(120, 32)

	m.Update(tea.KeyPressMsg{Code: 'a', Text: "a"})
	s.Require().NotNil(m.form)
	s.Contains(m.View().Content, "DEDUP KEY")

	// Empty fields must be refused instead of issuing a call.
	s.Nil(m.submit())
	s.Equal(statusFail, m.status.level)
}

func (s *ViewSuite) TestSanitize() {
	cases := []struct {
		name     string
		data     string
		expected string
	}{
		{name: "printable", data: "hello", expected: "hello"},
		{name: "control chars", data: "a\nb\tc", expected: "a·b·c"},
		{name: "truncated", data: strings.Repeat("x", maxPayloadRunes+10), expected: strings.Repeat("x", maxPayloadRunes) + "…"},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.Equal(tc.expected, sanitize([]byte(tc.data)))
		})
	}
}

func (s *ViewSuite) TestFit() {
	cases := []struct {
		name     string
		in       string
		width    int
		expected string
	}{
		{name: "pads", in: "ab", width: 4, expected: "ab  "},
		{name: "exact", in: "abcd", width: 4, expected: "abcd"},
		{name: "truncates", in: "abcdef", width: 4, expected: "abc…"},
		{name: "zero", in: "abc", width: 0, expected: ""},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.Equal(tc.expected, fit(tc.in, tc.width))
		})
	}
}
