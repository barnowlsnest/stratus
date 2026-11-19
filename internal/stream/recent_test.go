package stream

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/stratus/internal/storage"
)

type RecentSuite struct {
	suite.Suite
}

func TestRecentSuite(t *testing.T) {
	suite.Run(t, new(RecentSuite))
}

// After IDs 1..10 go into 4 slots, only 7..10 are held, and a range stops at
// the first ID that is not.
func (s *RecentSuite) TestAppendRange() {
	r := newRecent(4)
	for id := uint64(1); id <= 10; id++ {
		r.put(&storage.Record{ID: id})
	}

	tests := []struct {
		name        string
		fromID      uint64
		toID        uint64
		expectedIDs []uint64
	}{
		{name: "all held", fromID: 7, toID: 10, expectedIDs: []uint64{7, 8, 9, 10}},
		{name: "stops past the newest", fromID: 8, toID: 12, expectedIDs: []uint64{8, 9, 10}},
		{name: "overwritten first ID", fromID: 5, toID: 10, expectedIDs: []uint64{}},
		{name: "never put", fromID: 11, toID: 12, expectedIDs: []uint64{}},
		{name: "from above to", fromID: 9, toID: 8, expectedIDs: []uint64{}},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			actual := make([]uint64, 0, len(tc.expectedIDs))
			for _, record := range r.appendRange(tc.fromID, tc.toID, nil) {
				actual = append(actual, record.ID)
			}
			s.Equal(tc.expectedIDs, actual)
		})
	}
}
