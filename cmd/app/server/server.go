package server

import (
	"context"
	"errors"
	"time"

	stratusv1 "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	readOffsetMinTimeout = 61 * time.Millisecond
	readOffsetMaxTimeout = 5 * time.Hour
)

type (
	Stream interface {
		Add(ctx context.Context, records []*storage.Record) (stream.AddResult, error)
		Get(ctx context.Context, fromID, toID uint64) ([]*storage.Record, error)
		Del(ctx context.Context, upTo uint64) (stream.DelResult, error)
		Info() stream.Info
		DataReady() <-chan struct{}
		ReCache(ctx context.Context) error
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

func (s *Server) Add(ctx context.Context, req *stratusv1.AddRequest) (*stratusv1.AddResponse, error) {
	if len(req.GetRecords()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no records to add")
	}

	res, err := s.stream.Add(ctx, toStorageRecords(req.GetRecords()))
	if err != nil {
		return nil, toStatus(err)
	}

	return &stratusv1.AddResponse{
		AddedRecords:     toProtoRange(res.AddedRange),
		StreamRecords:    toProtoRange(res.StreamRange),
		DuplicateRecords: res.DedupCount,
	}, nil
}

func (s *Server) ReadRange(ctx context.Context, req *stratusv1.ReadRangeRequest) (*stratusv1.ReadResponse, error) {
	ctx, cancel := withTimeout(ctx, req.GetTimeout())
	defer cancel()

	records, err := s.stream.Get(ctx, req.GetStartId(), req.GetEndId())
	if err != nil {
		return nil, toStatus(err)
	}

	return &stratusv1.ReadResponse{Records: toOutputRecords(records)}, nil
}

func (s *Server) ReadOffset(req *stratusv1.ReadOffsetRequest, str grpc.ServerStreamingServer[stratusv1.ReadResponse]) error {
	if req.GetMaxRecords() == 0 {
		return status.Error(codes.InvalidArgument, "max_records must be greater than zero")
	}

	requested := req.GetTimeout().AsDuration()
	if requested > readOffsetMaxTimeout {
		return status.Error(codes.InvalidArgument, "timeout must not exceed "+readOffsetMaxTimeout.String())
	}

	ctx, cancel := context.WithTimeout(str.Context(), max(readOffsetMinTimeout, requested))
	defer cancel()

	startID := req.GetStartId()
	endID := startID + req.GetMaxRecords() - 1
	if endID < startID {
		endID = ^uint64(0)
	}

	nextID := startID
	for {
		if nextID >= endID {
			return nil
		}

		i := s.stream.Info()
		if i.FirstID > 0 && nextID < i.FirstID {
			return toStatus(storage.ErrOutOfBounds)
		}

		ready := s.stream.DataReady()
		recs, err := s.stream.Get(ctx, nextID, endID)
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return nil
		case errors.Is(err, storage.ErrOutOfBounds):
			if i.FirstID > 0 && nextID < i.FirstID {
				return toStatus(err)
			}
		case err != nil:
			return toStatus(err)
		}

		if len(recs) > 0 {
			if err := str.Send(&stratusv1.ReadResponse{Records: toOutputRecords(recs)}); err != nil {
				return err
			}
			nextID += uint64(len(recs))
			continue
		}

		select {
		case <-ready:
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Server) Delete(ctx context.Context, req *stratusv1.DeleteRequest) (*stratusv1.DeleteResponse, error) {
	res, err := s.stream.Del(ctx, req.GetEndId())
	if err != nil {
		return nil, toStatus(err)
	}

	return &stratusv1.DeleteResponse{
		DeletedRecords: toProtoRange(res.DeletedRange),
		StreamRecords:  toProtoRange(res.StreamRange),
	}, nil
}

func (s *Server) ReCache(ctx context.Context, _ *emptypb.Empty) (*stratusv1.ReCacheResponse, error) {
	if err := s.stream.ReCache(ctx); err != nil {
		return nil, toStatus(err)
	}

	i := s.stream.Info()
	streamRange := &stratusv1.Range{
		Start: i.FirstID,
		End:   i.LastID,
	}

	return &stratusv1.ReCacheResponse{
		Range: streamRange,
	}, nil
}

func (s *Server) GetStreamInfo(context.Context, *emptypb.Empty) (*stratusv1.GetStreamInfoResponse, error) {
	i := s.stream.Info()
	streamRange := &stratusv1.Range{
		Start: i.FirstID,
		End:   i.LastID,
	}

	streamInfo := &stratusv1.StreamInfo{
		Range:         streamRange,
		CachedRecords: i.CachedRecords,
		FsRecords:     i.FSRecords,
	}

	return &stratusv1.GetStreamInfoResponse{
		Info: streamInfo,
	}, nil
}

func toStorageRecords(in []*stratusv1.InputRecord) []*storage.Record {
	records := make([]*storage.Record, 0, len(in))
	for _, r := range in {
		records = append(records, &storage.Record{
			DedupKey: r.GetDedupKey(),
			Bytes:    r.GetRawData(),
		})
	}

	return records
}

func toOutputRecords(in []*storage.Record) []*stratusv1.OutputRecord {
	out := make([]*stratusv1.OutputRecord, 0, len(in))
	for _, r := range in {
		out = append(out, &stratusv1.OutputRecord{
			Id:      r.ID,
			RawData: r.Bytes,
		})
	}

	return out
}

func toProtoRange(r stream.Range) *stratusv1.Range {
	return &stratusv1.Range{
		Start: r.First(),
		End:   r.Last(),
	}
}

func withTimeout(ctx context.Context, d *durationpb.Duration) (context.Context, context.CancelFunc) {
	if d == nil || d.AsDuration() <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, d.AsDuration())
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, storage.ErrOutOfBounds):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, storage.ErrTooLongRangeToRead),
		errors.Is(err, storage.ErrEmptyRecord),
		errors.Is(err, storage.ErrNilRecord),
		errors.Is(err, storage.ErrEmptyDedupKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, stream.ErrAllSkipped):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
