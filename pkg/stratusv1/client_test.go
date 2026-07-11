package stratusv1

import (
	"context"
	"testing"
	"time"

	pb "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

// stubService records the last request seen and returns canned responses.
type stubService struct {
	pb.StreamServiceClient

	addReq        *pb.AddRequest
	readRangeReq  *pb.ReadRangeRequest
	readOffsetReq *pb.ReadOffsetRequest
	deleteReq     *pb.DeleteRequest

	addResp    *pb.AddResponse
	readResp   *pb.ReadResponse
	deleteResp *pb.DeleteResponse
}

func (s *stubService) Add(_ context.Context, in *pb.AddRequest, _ ...grpc.CallOption) (*pb.AddResponse, error) {
	s.addReq = in
	return s.addResp, nil
}

func (s *stubService) ReadRange(_ context.Context, in *pb.ReadRangeRequest, _ ...grpc.CallOption) (*pb.ReadResponse, error) {
	s.readRangeReq = in
	return s.readResp, nil
}

func (s *stubService) ReadOffset(_ context.Context, in *pb.ReadOffsetRequest, _ ...grpc.CallOption) (*pb.ReadResponse, error) {
	s.readOffsetReq = in
	return s.readResp, nil
}

func (s *stubService) Delete(_ context.Context, in *pb.DeleteRequest, _ ...grpc.CallOption) (*pb.DeleteResponse, error) {
	s.deleteReq = in
	return s.deleteResp, nil
}

type ClientSuite struct {
	suite.Suite
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}

func (s *ClientSuite) client(svc pb.StreamServiceClient) *Client {
	return &Client{svc: svc}
}

func (s *ClientSuite) TestAdd() {
	stub := &stubService{addResp: &pb.AddResponse{
		AddedRecords:     &pb.Range{Start: 10, End: 12},
		StreamRecords:    &pb.Range{Start: 1, End: 12},
		DuplicateRecords: 2,
	}}

	actual, err := s.client(stub).Add(context.Background(), []InputRecord{
		{DedupKey: 1, RawData: []byte("a")},
		{DedupKey: 2, RawData: []byte("b")},
	})
	s.Require().NoError(err)

	expected := AddResult{
		AddedRecords:     Range{Start: 10, End: 12},
		StreamRecords:    Range{Start: 1, End: 12},
		DuplicateRecords: 2,
	}
	s.Equal(expected, actual)

	s.Require().Len(stub.addReq.GetRecords(), 2)
	s.Equal(uint64(1), stub.addReq.GetRecords()[0].GetDedupKey())
	s.Equal([]byte("a"), stub.addReq.GetRecords()[0].GetRawData())
}

func (s *ClientSuite) TestReadRange() {
	stub := &stubService{readResp: &pb.ReadResponse{Records: []*pb.OutputRecord{
		{Id: 5, RawData: []byte("x")},
		{Id: 6, RawData: []byte("y")},
	}}}

	actual, err := s.client(stub).ReadRange(context.Background(), 5, 6, 2*time.Second)
	s.Require().NoError(err)

	expected := []OutputRecord{
		{ID: 5, RawData: []byte("x")},
		{ID: 6, RawData: []byte("y")},
	}
	s.Equal(expected, actual)

	s.Equal(uint64(5), stub.readRangeReq.GetStartId())
	s.Equal(uint64(6), stub.readRangeReq.GetEndId())
	s.Equal(2*time.Second, stub.readRangeReq.GetTimeout().AsDuration())
}

func (s *ClientSuite) TestReadOffset() {
	stub := &stubService{readResp: &pb.ReadResponse{Records: []*pb.OutputRecord{
		{Id: 7, RawData: []byte("z")},
	}}}

	actual, err := s.client(stub).ReadOffset(context.Background(), 7, 10, 0)
	s.Require().NoError(err)

	s.Equal([]OutputRecord{{ID: 7, RawData: []byte("z")}}, actual)
	s.Equal(uint64(7), stub.readOffsetReq.GetStartId())
	s.Equal(uint64(10), stub.readOffsetReq.GetMaxRecords())
	// Non-positive timeout is omitted from the request.
	s.Nil(stub.readOffsetReq.GetTimeout())
}

func (s *ClientSuite) TestDelete() {
	stub := &stubService{deleteResp: &pb.DeleteResponse{
		DeletedRecords: &pb.Range{Start: 1, End: 3},
		StreamRecords:  &pb.Range{Start: 4, End: 12},
	}}

	actual, err := s.client(stub).Delete(context.Background(), 3)
	s.Require().NoError(err)

	expected := DeleteResult{
		DeletedRecords: Range{Start: 1, End: 3},
		StreamRecords:  Range{Start: 4, End: 12},
	}
	s.Equal(expected, actual)
	s.Equal(uint64(3), stub.deleteReq.GetEndId())
}

func (s *ClientSuite) TestFromProtoRangeNil() {
	s.Equal(Range{}, fromProtoRange(nil))
}
