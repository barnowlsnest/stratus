package client

import (
	"context"
	"sync"

	stratusv1 "github.com/barnowlsnest/stratus/pkg/client/gen/stratus/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	// Record is a write input.
	Record struct {
		DedupKey uint64
		Payload  []byte
	}

	// Entry is a record read back from the stream.
	Entry struct {
		ID      uint64
		Payload []byte
	}

	// Client owns one connection and two long-lived streams. Each stream is
	// serialized by its own mutex (Approach A: one in-flight call per stream).
	Client struct {
		conn        *grpc.ClientConn
		writeMu     sync.Mutex
		writeStream stratusv1.StreamService_WriteClient
		readMu      sync.Mutex
		readStream  stratusv1.StreamService_ReadClient
	}
)

// New dials addr and opens the write and read streams. If the caller passes no
// dial options, an insecure transport is used for plaintext local use.
func New(ctx context.Context, addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	rpc := stratusv1.NewStreamServiceClient(conn)

	writeStream, err := rpc.Write(ctx)
	if err != nil {
		_ = conn.Close()

		return nil, err
	}

	readStream, err := rpc.Read(ctx)
	if err != nil {
		_ = conn.Close()

		return nil, err
	}

	return &Client{
		conn:        conn,
		writeStream: writeStream,
		readStream:  readStream,
	}, nil
}

// Write appends a single record and returns its LSN.
func (c *Client) Write(ctx context.Context, dedupKey uint64, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	req := &stratusv1.WriteRequest{Payload: &stratusv1.WriteRequest_Record{
		Record: &stratusv1.Record{DedupKey: dedupKey, Payload: payload},
	}}
	if err := c.writeStream.Send(req); err != nil {
		return 0, err
	}

	resp, err := c.writeStream.Recv()
	if err != nil {
		return 0, err
	}

	return resp.GetStart(), nil
}

// WriteBatch appends a batch of records and returns the assigned LSN range.
func (c *Client) WriteBatch(ctx context.Context, records []Record) (first, last uint64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	pbRecords := make([]*stratusv1.Record, len(records))
	for i, record := range records {
		pbRecords[i] = &stratusv1.Record{DedupKey: record.DedupKey, Payload: record.Payload}
	}

	req := &stratusv1.WriteRequest{Payload: &stratusv1.WriteRequest_Batch{
		Batch: &stratusv1.RecordBatch{Records: pbRecords},
	}}
	if err := c.writeStream.Send(req); err != nil {
		return 0, 0, err
	}

	resp, err := c.writeStream.Recv()
	if err != nil {
		return 0, 0, err
	}

	return resp.GetStart(), resp.GetEnd(), nil
}

// Read returns the entry at id, or ErrNotFound if the server sends no entry.
func (c *Client) Read(ctx context.Context, id uint64) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}

	c.readMu.Lock()
	defer c.readMu.Unlock()

	req := &stratusv1.ReadRequest{Query: &stratusv1.ReadRequest_Id{Id: id}}
	if err := c.readStream.Send(req); err != nil {
		return Entry{}, err
	}

	resp, err := c.readStream.Recv()
	if err != nil {
		return Entry{}, err
	}

	if len(resp.GetEntries()) == 0 {
		return Entry{}, ErrNotFound
	}

	entry := resp.GetEntries()[0]

	return Entry{ID: entry.GetId(), Payload: entry.GetPayload()}, nil
}

// Range returns the entries in the inclusive LSN range [first, last].
func (c *Client) Range(ctx context.Context, first, last uint64) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.readMu.Lock()
	defer c.readMu.Unlock()

	req := &stratusv1.ReadRequest{Query: &stratusv1.ReadRequest_Range{
		Range: &stratusv1.Range{First: first, Last: last},
	}}
	if err := c.readStream.Send(req); err != nil {
		return nil, err
	}

	resp, err := c.readStream.Recv()
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, len(resp.GetEntries()))
	for i, entry := range resp.GetEntries() {
		entries[i] = Entry{ID: entry.GetId(), Payload: entry.GetPayload()}
	}

	return entries, nil
}

// Close closes the underlying connection (which closes both streams).
func (c *Client) Close() error {
	return c.conn.Close()
}
