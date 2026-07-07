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
