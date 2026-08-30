package stratusv1

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
)

// InputRecord is a record submitted to the stream via Add.
type InputRecord struct {
	// DedupKey identifies the record for deduplication. Records sharing a
	// DedupKey within the server's dedup window are collapsed to one.
	DedupKey uint64
	// RawData is the opaque payload stored for the record.
	RawData []byte
}

// OutputRecord is a record returned from the stream on the read path.
type OutputRecord struct {
	// ID is the LSN assigned by the server when the record was appended.
	ID uint64
	// RawData is the opaque payload of the record.
	RawData []byte
}

// Range is an inclusive [Start, End] span of record IDs.
type Range struct {
	Start uint64
	End   uint64
}

// AddResult reports the outcome of an Add call.
type AddResult struct {
	// AddedRecords is the range of IDs assigned to newly appended records.
	AddedRecords Range
	// StreamRecords is the range of IDs currently held by the stream.
	StreamRecords Range
	// DuplicateRecords is the number of submitted records dropped as duplicates.
	DuplicateRecords uint64
}

// StreamInfo describes the current state of the stream.
type StreamInfo struct {
	// Range is the range of IDs currently held by the stream.
	Range Range
	// CachedRecords is the number of records held in the in-memory cache.
	CachedRecords uint64
	// FSRecords is the number of records persisted on the filesystem.
	FSRecords uint64
}

// DeleteResult reports the outcome of a Delete call.
type DeleteResult struct {
	// DeletedRecords is the range of IDs removed from the stream.
	DeletedRecords Range
	// StreamRecords is the range of IDs remaining in the stream.
	StreamRecords Range
}

func toProtoInputRecords(in []InputRecord) []*pb.InputRecord {
	out := make([]*pb.InputRecord, 0, len(in))
	for _, r := range in {
		out = append(out, &pb.InputRecord{
			DedupKey: r.DedupKey,
			RawData:  r.RawData,
		})
	}

	return out
}

func fromProtoOutputRecords(in []*pb.OutputRecord) []OutputRecord {
	out := make([]OutputRecord, 0, len(in))
	for _, r := range in {
		out = append(out, OutputRecord{
			ID:      r.GetId(),
			RawData: r.GetRawData(),
		})
	}

	return out
}

func fromProtoStreamInfo(i *pb.StreamInfo) StreamInfo {
	return StreamInfo{
		Range:         fromProtoRange(i.GetRange()),
		CachedRecords: i.GetCachedRecords(),
		FSRecords:     i.GetFsRecords(),
	}
}

func fromProtoRange(r *pb.Range) Range {
	return Range{
		Start: r.GetStart(),
		End:   r.GetEnd(),
	}
}

func toProtoDuration(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}

	return durationpb.New(d)
}
