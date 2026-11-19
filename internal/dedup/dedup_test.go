package dedup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type DedupSuite struct {
	suite.Suite
}

func TestDedupSuite(t *testing.T) {
	suite.Run(t, new(DedupSuite))
}

func (s *DedupSuite) TestTry() {
	d := New(1 * time.Second)
	s.Require().NoError(d.Try(1))
	s.Require().ErrorIs(d.Try(1), ErrDuplicate)
	<-time.After(500 * time.Millisecond)
	s.Require().ErrorIs(d.Try(1), ErrDuplicate)
	<-time.After(600 * time.Millisecond)
	s.Require().NoError(d.Try(1))
}

// Keys expire in the order they were added, each ttl after its own first Try,
// and an expired key is accepted again.
func (s *DedupSuite) TestTryExpiresInOrder() {
	const ttl = 200 * time.Millisecond

	d := New(ttl)
	s.Require().NoError(d.Try(1))
	s.Require().NoError(d.Try(2))
	<-time.After(ttl / 2)
	s.Require().NoError(d.Try(3))
	<-time.After(ttl/2 + 20*time.Millisecond)

	tests := []struct {
		expected error
		name     string
		key      uint64
	}{
		{name: "first key expired", key: 1, expected: nil},
		{name: "second key expired", key: 2, expected: nil},
		{name: "newer key still remembered", key: 3, expected: ErrDuplicate},
		{name: "re-added key remembered", key: 1, expected: ErrDuplicate},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			actual := d.Try(tc.key)
			if tc.expected == nil {
				s.Require().NoError(actual)

				return
			}

			s.Require().ErrorIs(actual, tc.expected)
		})
	}
}
