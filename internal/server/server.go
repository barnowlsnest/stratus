package server

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/barnowlsnest/stratus/internal/stream"
	stratusv1 "github.com/barnowlsnest/stratus/pkg/client/gen/stratus/v1"
	"google.golang.org/grpc"
)

type (
	// Server exposes a single stream.Stream over the gRPC StreamService.
	Server struct {
		stratusv1.UnimplementedStreamServiceServer
		stream *stream.Stream
		addr   string
	}

	// Option configures a Server in New.
	Option func(*Server)
)

// WithStream sets the stream the server delegates to.
func WithStream(s *stream.Stream) Option {
	return func(srv *Server) {
		srv.stream = s
	}
}

// WithAddr sets the TCP listen address used by Run.
func WithAddr(addr string) Option {
	return func(srv *Server) {
		srv.addr = addr
	}
}

// New builds a Server from the given options. It requires a stream.
func New(opts ...Option) (*Server, error) {
	s := &Server{}
	for _, opt := range opts {
		opt(s)
	}

	if s.stream == nil {
		return nil, ErrNilStream
	}

	return s, nil
}

// Register adds the service to an existing gRPC server.
func (s *Server) Register(g *grpc.Server) {
	stratusv1.RegisterStreamServiceServer(g, s)
}

// Run listens on the configured address and serves until ctx is cancelled,
// then stops gracefully.
func (s *Server) Run(ctx context.Context) error {
	if s.addr == "" {
		return ErrNoAddr
	}

	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	g := grpc.NewServer()
	s.Register(g)

	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()

	return g.Serve(lis)
}

// Write handles the bidirectional write stream: one request → one response.
func (s *Server) Write(srv stratusv1.StreamService_WriteServer) error {
	ctx := srv.Context()
	for {
		req, err := srv.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		start, end, err := s.handleWrite(ctx, req)
		if err != nil {
			return toStatus(err)
		}

		if err := srv.Send(&stratusv1.WriteResponse{Start: start, End: end}); err != nil {
			return err
		}
	}
}

func (s *Server) handleWrite(ctx context.Context, req *stratusv1.WriteRequest) (start, end uint64, err error) {
	switch payload := req.GetPayload().(type) {
	case *stratusv1.WriteRequest_Record:
		return s.stream.Add(ctx, stream.NewItem(payload.Record.GetDedupKey(), payload.Record.GetPayload()))
	case *stratusv1.WriteRequest_Batch:
		records := payload.Batch.GetRecords()
		items := make([]*stream.Item, len(records))
		for i, r := range records {
			items[i] = stream.NewItem(r.GetDedupKey(), r.GetPayload())
		}

		return s.stream.AddN(ctx, items...)
	default:
		return 0, 0, ErrEmptyWriteRequest
	}
}

// Read handles the bidirectional read stream: one query → one response.
func (s *Server) Read(srv stratusv1.StreamService_ReadServer) error {
	ctx := srv.Context()
	for {
		req, err := srv.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		entries, err := s.handleRead(ctx, req)
		if err != nil {
			return toStatus(err)
		}

		if err := srv.Send(&stratusv1.ReadResponse{Entries: entries}); err != nil {
			return err
		}
	}
}

func (s *Server) handleRead(ctx context.Context, req *stratusv1.ReadRequest) ([]*stratusv1.Entry, error) {
	switch query := req.GetQuery().(type) {
	case *stratusv1.ReadRequest_Id:
		item, err := s.stream.Read(ctx, query.Id)
		if err != nil {
			return nil, err
		}

		return []*stratusv1.Entry{{Id: item.ID, Payload: item.RawBytes}}, nil
	case *stratusv1.ReadRequest_Range:
		items, err := s.stream.Range(ctx, query.Range.GetFirst(), query.Range.GetLast())
		if err != nil {
			return nil, err
		}

		entries := make([]*stratusv1.Entry, len(items))
		for i, item := range items {
			entries[i] = &stratusv1.Entry{Id: item.ID, Payload: item.RawBytes}
		}

		return entries, nil
	default:
		return nil, ErrEmptyReadRequest
	}
}
