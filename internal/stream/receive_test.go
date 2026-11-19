package stream

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/stratus/internal/storage"
)

type ReceiveSuite struct {
	suite.Suite
}

func TestReceiveSuite(t *testing.T) {
	suite.Run(t, new(ReceiveSuite))
}

// receiveWaiting takes what is waiting up to limit and leaves the rest in the
// channel, so a steady flow of records cannot keep one batch open.
func (s *ReceiveSuite) TestReceiveWaitingStopsAtLimit() {
	tests := []struct {
		name         string
		waiting      int
		limit        int
		closed       bool
		expectedLen  int
		expectedLeft int
	}{
		{name: "fewer waiting than limit", waiting: 3, limit: 5, expectedLen: 3, expectedLeft: 0},
		{name: "as many waiting as limit", waiting: 5, limit: 5, expectedLen: 5, expectedLeft: 0},
		{name: "more waiting than limit", waiting: 8, limit: 5, expectedLen: 5, expectedLeft: 3},
		{name: "closed channel", waiting: 2, limit: 5, closed: true, expectedLen: 2, expectedLeft: 0},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			records := make(chan *storage.Record, tc.waiting)
			for id := range tc.waiting {
				records <- &storage.Record{ID: uint64(id + 1)}
			}
			if tc.closed {
				close(records)
			}

			actual := receiveWaiting(records, nil, tc.limit)

			s.Len(actual, tc.expectedLen)
			s.Len(records, tc.expectedLeft)
			for i, r := range actual {
				s.Equal(uint64(i+1), r.ID)
			}
		})
	}
}
