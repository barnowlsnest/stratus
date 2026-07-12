// Package stratusv1 provides a Go client for the Stratus StreamService gRPC
// API. It exposes its own request and response types so callers do not depend
// on the generated protobuf package.
package stratusv1

import (
	"context"
	"time"

	pb "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a Stratus StreamService client.
type Client struct {
	svc  pb.StreamServiceClient
	conn *grpc.ClientConn // non-nil only when created via Dial; owned by the client.
}

// New wraps an existing gRPC connection. The caller retains ownership of cc and
// is responsible for closing it; Close on the returned client is a no-op.
func New(cc grpc.ClientConnInterface) *Client {
	return &Client{svc: pb.NewStreamServiceClient(cc)}
}

// Dial creates a client connected to the target. The returned client owns the
// connection and must be closed with Close.
func Dial(target string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{svc: pb.NewStreamServiceClient(conn), conn: conn}, nil
}

func WithInsecure() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// Close closes the underlying connection when the client owns it (created via
// Dial). It is a no-op for clients created with New.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}

	return c.conn.Close()
}

// Add appends records to the stream and reports the assigned range along with
// the number of records dropped as duplicates.
func (c *Client) Add(ctx context.Context, records []InputRecord) (AddResult, error) {
	resp, err := c.svc.Add(ctx, &pb.AddRequest{Records: toProtoInputRecords(records)})
	if err != nil {
		return AddResult{}, err
	}

	return AddResult{
		AddedRecords:     fromProtoRange(resp.GetAddedRecords()),
		StreamRecords:    fromProtoRange(resp.GetStreamRecords()),
		DuplicateRecords: resp.GetDuplicateRecords(),
	}, nil
}

// ReadRange returns records with IDs in the inclusive range [startID, endID].
// A non-positive timeout leaves the read bounded only by ctx.
func (c *Client) ReadRange(ctx context.Context, startID, endID uint64, timeout time.Duration) ([]OutputRecord, error) {
	resp, err := c.svc.ReadRange(ctx, &pb.ReadRangeRequest{
		StartId: startID,
		EndId:   endID,
		Timeout: toProtoDuration(timeout),
	})
	if err != nil {
		return nil, err
	}

	return fromProtoOutputRecords(resp.GetRecords()), nil
}

// ReadOffset returns up to maxRecords records starting at startID. A
// non-positive timeout leaves the read bounded only by ctx.
func (c *Client) ReadOffset(ctx context.Context, startID, maxRecords uint64, timeout time.Duration) (<-chan OutputRecord, error) {
	resp, err := c.svc.ReadOffset(ctx, &pb.ReadOffsetRequest{
		StartId:    startID,
		MaxRecords: maxRecords,
		Timeout:    toProtoDuration(timeout),
	})
	if err != nil {
		return nil, err
	}

	out := make(chan OutputRecord, maxRecords)
	go func() {
		defer close(out)
		for {
			select {
			case <-resp.Context().Done():
				return
			default:
				msg, err := resp.Recv()
				if err != nil {
					return
				}
				for _, r := range msg.GetRecords() {
					select {
					case <-resp.Context().Done():
						return
					case out <- OutputRecord{
						ID:      r.GetId(),
						RawData: r.GetRawData(),
					}:
					}
				}
			}
		}
	}()

	// return fromProtoOutputRecords(resp.GetRecords()), nil
	return out, nil
}

// Delete removes records with IDs up to and including endID, reporting the
// deleted range and the range remaining in the stream.
func (c *Client) Delete(ctx context.Context, endID uint64) (DeleteResult, error) {
	resp, err := c.svc.Delete(ctx, &pb.DeleteRequest{EndId: endID})
	if err != nil {
		return DeleteResult{}, err
	}

	return DeleteResult{
		DeletedRecords: fromProtoRange(resp.GetDeletedRecords()),
		StreamRecords:  fromProtoRange(resp.GetStreamRecords()),
	}, nil
}
