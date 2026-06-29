package server

import (
	"context"
	"io"
	"testing"

	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		input    error
		expected codes.Code
	}{
		{"nil", nil, codes.OK},
		{"duplicate", dedup.ErrDuplicateChunk, codes.AlreadyExists},
		{"out of bounds", storage.ErrOutOfBounds, codes.OutOfRange},
		{"eof", io.EOF, codes.NotFound},
		{"too long", storage.ErrTooLongRangeToRead, codes.InvalidArgument},
		{"empty record", storage.ErrEmptyRecord, codes.InvalidArgument},
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"unknown", io.ErrUnexpectedEOF, codes.Internal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := status.Code(toStatus(tc.input))
			require.Equal(t, tc.expected, actual)
		})
	}
}
