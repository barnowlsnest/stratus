package server

import (
	"context"
	"errors"
	
	stratusv1 "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
)

var ErrNotImplemented = errors.New("not implemented")

type (
	Stream interface {
		Add(ctx context.Context, records []*storage.Record) (stream.AddResult, error)
		Get(ctx context.Context, fromID, toID uint64) ([]*storage.Record, error)
		Del(ctx context.Context, upTo uint64) error
	}
	
	Server struct {
		stratusv1.UnsafeStreamServiceServer
		stream Stream
	}
)

func New(stream Stream) *Server {
	return &Server{
		stream: stream,
	}
}
