package server

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/barnowlsnest/stratus/internal/stream"
	stratusv1 "github.com/barnowlsnest/stratus/pkg/client/gen/stratus/v1"
	"google.golang.org/grpc"
)

// defaultShutdownGrace bounds how long Run waits for in-flight RPCs to drain
// gracefully before forcing connections closed.
const defaultShutdownGrace = 5 * time.Second

type (
	// Server exposes a single stream.Stream over the gRPC StreamService.
	Server struct {
		stratusv1.UnimplementedStreamServiceServer
		stream        *stream.Stream
		addr          string
		shutdownGrace time.Duration
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

// WithShutdownGrace bounds how long Run waits for in-flight RPCs to drain on
// shutdown before forcing connections closed. Defaults to defaultShutdownGrace.
func WithShutdownGrace(d time.Duration) Option {
	return func(srv *Server) {
		srv.shutdownGrace = d
	}
}

// New builds a Server from the given options. It requires a stream.
func New(opts ...Option) (*Server, error) {
	s := &Server{shutdownGrace: defaultShutdownGrace}
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
		s.stop(g)
	}()

	return g.Serve(lis)
}

// stop drains in-flight RPCs gracefully, but only for shutdownGrace. Long-lived
// idle streams never finish on their own, so GracefulStop would block forever;
// once the grace elapses we force connections closed with Stop.
func (s *Server) stop(g *grpc.Server) {
	stopped := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(stopped)
	}()

	timer := time.NewTimer(s.shutdownGrace)
	defer timer.Stop()

	select {
	case <-stopped:
	case <-timer.C:
		g.Stop()
	}
}

// inStreamError maps err to an in-stream Error with current WAL bounds
// attached, or nil when the error is fatal and must terminate the RPC.
func (s *Server) inStreamError(err error) *stratusv1.Error {
	metadata := s.stream.Metadata()

	return toError(err, metadata.Preloader.FirstID, metadata.Preloader.LastID)
}

// Write handles the bidirectional write stream: one request → one response.
// Recoverable failures are sent as in-stream errors; only fatal errors
// terminate the stream.
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

		response := &stratusv1.WriteResponse{}
		start, end, err := s.handleWrite(ctx, req)
		switch streamErr := s.inStreamError(err); {
		case err == nil:
			response.Start, response.End = start, end
		case streamErr == nil:
			return toStatus(err)
		default:
			response.Error = streamErr
		}

		if err := srv.Send(response); err != nil {
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
// Recoverable failures are sent as in-stream errors; only fatal errors
// terminate the stream.
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

		response := &stratusv1.ReadResponse{}
		entries, err := s.handleRead(ctx, req)
		switch streamErr := s.inStreamError(err); {
		case err == nil:
			response.Entries = entries
		case streamErr == nil:
			return toStatus(err)
		default:
			response.Error = streamErr
		}

		if err := srv.Send(response); err != nil {
			return err
		}
	}
}

// GetMetadata returns a flat snapshot of the stream's preloader and ingester
// metadata.
func (s *Server) GetMetadata(ctx context.Context, _ *stratusv1.GetMetadataRequest) (*stratusv1.GetMetadataResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m := s.stream.Metadata()

	var lastTruncateClaimAtUnix int64
	if t := m.Ingester.LastTruncateClaimAt; !t.IsZero() {
		lastTruncateClaimAtUnix = t.Unix()
	}

	return &stratusv1.GetMetadataResponse{
		StorageSize:             m.Preloader.StorageSize,
		CacheSize:               m.Preloader.CacheSize,
		FirstId:                 m.Preloader.FirstID,
		LastId:                  m.Preloader.LastID,
		BytesWritten:            m.Ingester.BytesWritten,
		DuplicatesCount:         m.Ingester.DuplicatesCount,
		WritesCount:             m.Ingester.WritesCount,
		WritesPerSecond:         m.Ingester.WritesPerSecond,
		DurationSeconds:         m.Ingester.Duration.Seconds(),
		TruncateClaimsCount:     m.Ingester.TruncateClaimsCount,
		LastTruncateClaimAtUnix: lastTruncateClaimAtUnix,
	}, nil
}

// CutOffset drops all records up to and including up_to, replying with the
// dropped LSN range.
func (s *Server) CutOffset(ctx context.Context, req *stratusv1.CutOffsetRequest) (*stratusv1.CutOffsetResponse, error) {
	upTo := req.GetUpTo()
	first := s.stream.Metadata().Preloader.FirstID

	if err := s.stream.Cut(ctx, upTo); err != nil {
		return nil, toStatus(err)
	}

	return &stratusv1.CutOffsetResponse{First: first, Last: upTo}, nil
}

func (s *Server) UpdateCache(ctx context.Context, req *stratusv1.UpdateCacheRequest) (*stratusv1.UpdateCacheResponse, error) {
	if err := s.stream.UpdateCache(ctx, req.First, req.Last); err != nil {
		return nil, toStatus(err)
	}

	return &stratusv1.UpdateCacheResponse{}, nil
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
