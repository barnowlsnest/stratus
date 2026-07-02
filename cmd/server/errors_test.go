package server

import (
	"context"
	"io"
	"testing"

	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/storage"
	stratusv1 "github.com/barnowlsnest/stratus/pkg/client/gen/stratus/v1"
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

func TestToErrorMapping(t *testing.T) {
	const (
		firstLSN = uint64(3)
		lastLSN  = uint64(41)
	)

	tests := []struct {
		name           string
		input          error
		expected       stratusv1.Err
		expectedFatal  bool
		expectedBounds bool
	}{
		{name: "nil", input: nil, expectedFatal: true},
		{name: "duplicate", input: dedup.ErrDuplicateChunk, expected: stratusv1.Err_ERR_ALREADY_EXISTS},
		{name: "all skipped", input: ingester.ErrAllSkipped, expected: stratusv1.Err_ERR_ALREADY_EXISTS},
		{name: "out of bounds", input: storage.ErrOutOfBounds, expected: stratusv1.Err_ERR_OUT_OF_RANGE, expectedBounds: true},
		{name: "read miss", input: io.EOF, expected: stratusv1.Err_ERR_NOT_FOUND, expectedBounds: true},
		{name: "too long range", input: storage.ErrTooLongRangeToRead, expected: stratusv1.Err_ERR_INVALID_ARGUMENT},
		{name: "empty record", input: storage.ErrEmptyRecord, expected: stratusv1.Err_ERR_INVALID_ARGUMENT},
		{name: "empty dedup key", input: storage.ErrEmptyDedupKey, expected: stratusv1.Err_ERR_INVALID_ARGUMENT},
		{name: "nil record", input: storage.ErrNilRecord, expected: stratusv1.Err_ERR_INVALID_ARGUMENT},
		{name: "empty write request", input: ErrEmptyWriteRequest, expected: stratusv1.Err_ERR_INVALID_ARGUMENT},
		{name: "empty read request", input: ErrEmptyReadRequest, expected: stratusv1.Err_ERR_INVALID_ARGUMENT},
		{name: "canceled is fatal", input: context.Canceled, expectedFatal: true},
		{name: "deadline is fatal", input: context.DeadlineExceeded, expectedFatal: true},
		{name: "unknown is fatal", input: io.ErrUnexpectedEOF, expectedFatal: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := toError(tc.input, firstLSN, lastLSN)

			if tc.expectedFatal {
				require.Nil(t, actual)

				return
			}

			require.NotNil(t, actual)
			require.Equal(t, tc.expected, actual.GetCode())
			require.Equal(t, tc.input.Error(), actual.GetMessage())

			if tc.expectedBounds {
				require.Equal(t, firstLSN, actual.GetFirstLsn())
				require.Equal(t, lastLSN, actual.GetLastLsn())
			} else {
				require.Zero(t, actual.GetFirstLsn())
				require.Zero(t, actual.GetLastLsn())
			}
		})
	}
}
