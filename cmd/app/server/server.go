package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	stratusv1 "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
)

var (
	readOffsetMinTimeout = 61 * time.Millisecond
	readOffsetMaxTimeout = 24 * time.Hour

	// outputRecordsPool reuses the block+pointer pair built for each ReadOffset
	// Send. Fanout heap profiles showed toOutputRecords at ~37% of alloc_space.
	outputRecordsPool = sync.Pool{New: func() any { return &outputRecordsBuf{} }}
)

// maxReusedRecords is the largest read slice a ReadOffset tail keeps for its
// next read. A tail catching up on a long backlog drops its slice instead, so
// it does not hold that much memory for the rest of the stream.
const maxReusedRecords = 4096

// maxPooledOutputRecords drops oversized pooled buffers so a catch-up burst
// does not leave multi-megabyte slabs in the pool forever.
const maxPooledOutputRecords = 4096

type outputRecordsBuf struct {
	block []stratusv1.OutputRecord
	out   []*stratusv1.OutputRecord
}

type (
	Stream interface {
		Add(ctx context.Context, records []*storage.Record) (stream.AddResult, error)
		Get(ctx context.Context, fromID, toID uint64, dst []*storage.Record) ([]*storage.Record, error)
		GetBatch(ctx context.Context, fromID, toID uint64, dst []*storage.Record) ([]*storage.Record, error)
		Del(ctx context.Context, upTo uint64) (stream.DelResult, error)
		Info() stream.Info
		FirstID() uint64
		Watch() (wake <-chan struct{}, stop func())
		ReconcileCache(ctx context.Context) error
	}

	Server struct {
		stratusv1.UnsafeStreamServiceServer
		stream Stream
	}
)

func New(s Stream) *Server {
	return &Server{
		stream: s,
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

	records, err := s.stream.Get(ctx, req.GetStartId(), req.GetEndId(), nil)
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

	// Watch before the first read so no new record between a read and the wait
	// is missed.
	wake, stopWatching := s.stream.Watch()
	defer stopWatching()

	nextID := startID
	// recs is reused across reads; Send has marshaled the response before it
	// returns, and the response holds payloads, not this slice.
	var recs []*storage.Record
	for {
		// Checked on every pass, not only while waiting: a tail that always has
		// records to read never reaches the wait below, and reads from the cache
		// do not look at ctx.
		if nextID >= endID || ctx.Err() != nil {
			return nil
		}

		firstID := s.stream.FirstID()
		if firstID > 0 && nextID < firstID {
			return toStatus(storage.ErrOutOfBounds)
		}

		// GetBatch bounds each message by record count and payload bytes, so a
		// tail far behind catches up in steps instead of in one oversized message.
		var err error
		recs, err = s.stream.GetBatch(ctx, nextID, endID, recs)
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return nil
		case errors.Is(err, storage.ErrOutOfBounds):
			if firstID > 0 && nextID < firstID {
				return toStatus(err)
			}
		case err != nil:
			return toStatus(err)
		}

		if len(recs) > 0 {
			out, release := borrowOutputRecords(recs)
			err := str.Send(&stratusv1.ReadResponse{Records: out})
			release()
			if err != nil {
				return err
			}
			nextID += uint64(len(recs))
			recs = reuseRecords(recs)
			continue
		}

		select {
		case <-wake:
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

func (s *Server) ReconcileCache(ctx context.Context, _ *emptypb.Empty) (*stratusv1.ReconcileCacheResponse, error) {
	if err := s.stream.ReconcileCache(ctx); err != nil {
		return nil, toStatus(err)
	}

	i := s.stream.Info()
	streamRange := &stratusv1.Range{
		Start: i.FirstID,
		End:   i.LastID,
	}

	return &stratusv1.ReconcileCacheResponse{
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

// toOutputRecords converts records into one block of OutputRecords and a list
// pointing into it: two allocations per response instead of one per record
// plus the list. Used by unary ReadRange, where gRPC marshals after the handler
// returns so a pooled buffer cannot be released in the handler.
func toOutputRecords(in []*storage.Record) []*stratusv1.OutputRecord {
	block := make([]stratusv1.OutputRecord, len(in))
	out := make([]*stratusv1.OutputRecord, len(in))
	fillOutputRecords(in, block, out)

	return out
}

// borrowOutputRecords is the pooled form for ReadOffset. release must run after
// Send returns: Send marshals before it returns, so the buffer is then free.
func borrowOutputRecords(in []*storage.Record) (out []*stratusv1.OutputRecord, release func()) {
	buf := outputRecordsPool.Get().(*outputRecordsBuf)
	n := len(in)
	if cap(buf.block) < n {
		buf.block = make([]stratusv1.OutputRecord, n)
		buf.out = make([]*stratusv1.OutputRecord, n)
	} else {
		buf.block = buf.block[:n]
		buf.out = buf.out[:n]
	}
	fillOutputRecords(in, buf.block, buf.out)

	return buf.out, func() {
		for i := range buf.block {
			buf.block[i].RawData = nil
		}
		clear(buf.out)
		buf.block = buf.block[:0]
		buf.out = buf.out[:0]
		if cap(buf.block) > maxPooledOutputRecords {
			return
		}
		outputRecordsPool.Put(buf)
	}
}

func fillOutputRecords(in []*storage.Record, block []stratusv1.OutputRecord, out []*stratusv1.OutputRecord) {
	for i, r := range in {
		block[i].Id = r.ID
		block[i].RawData = r.Bytes
		out[i] = &block[i]
	}
}

// reuseRecords empties recs for the next read, clearing it so it does not keep
// sent records alive, or drops it when it is larger than maxReusedRecords.
func reuseRecords(recs []*storage.Record) []*storage.Record {
	if cap(recs) > maxReusedRecords {
		return nil
	}

	clear(recs)

	return recs[:0]
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
