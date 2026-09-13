package server

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	stratusv1api "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

const testToken = "s3cret"

type AuthSuite struct {
	suite.Suite
}

func TestAuthSuite(t *testing.T) {
	suite.Run(t, new(AuthSuite))
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func (s *AuthSuite) TestInterceptors() {
	cases := []struct {
		name       string
		configured string
		md         metadata.MD
		expected   codes.Code
	}{
		{name: "valid token", configured: testToken, md: metadata.Pairs(authMetadataKey, bearerPrefix+testToken), expected: codes.OK},
		{name: "no metadata", configured: testToken, md: nil, expected: codes.Unauthenticated},
		{name: "wrong token", configured: testToken, md: metadata.Pairs(authMetadataKey, bearerPrefix+"nope"), expected: codes.Unauthenticated},
		{name: "missing bearer prefix", configured: testToken, md: metadata.Pairs(authMetadataKey, testToken), expected: codes.Unauthenticated},
		{name: "empty bearer value", configured: testToken, md: metadata.Pairs(authMetadataKey, bearerPrefix), expected: codes.Unauthenticated},
		{name: "empty configured token", configured: "", md: metadata.Pairs(authMetadataKey, bearerPrefix), expected: codes.Unauthenticated},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx := context.Background()
			if tc.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tc.md)
			}

			unaryCalled := false
			_, err := UnaryAuthInterceptor(tc.configured)(ctx, nil, &grpc.UnaryServerInfo{},
				func(context.Context, any) (any, error) {
					unaryCalled = true
					return nil, nil
				})
			s.Equal(tc.expected, status.Code(err))
			s.Equal(tc.expected == codes.OK, unaryCalled)

			streamCalled := false
			err = StreamAuthInterceptor(tc.configured)(nil, &fakeServerStream{ctx: ctx}, &grpc.StreamServerInfo{},
				func(any, grpc.ServerStream) error {
					streamCalled = true
					return nil
				})
			s.Equal(tc.expected, status.Code(err))
			s.Equal(tc.expected == codes.OK, streamCalled)
		})
	}
}

// fakeStream satisfies the Stream interface; only Info is exercised.
type fakeStream struct{}

func (fakeStream) Add(context.Context, []*storage.Record) (stream.AddResult, error) {
	return stream.AddResult{}, nil
}

func (fakeStream) Get(context.Context, uint64, uint64) ([]*storage.Record, error) { return nil, nil }

func (fakeStream) Del(context.Context, uint64) (stream.DelResult, error) {
	return stream.DelResult{}, nil
}

func (fakeStream) Info() stream.Info { return stream.Info{FirstID: 1, LastID: 3} }

func (fakeStream) DataReady() <-chan struct{} { return make(chan struct{}) }

func (fakeStream) ReconcileCache(context.Context) error { return nil }

func (s *AuthSuite) TestClientTokenEndToEnd() {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryAuthInterceptor(testToken)),
		grpc.StreamInterceptor(StreamAuthInterceptor(testToken)),
	)
	stratusv1api.RegisterStreamServiceServer(srv, New(fakeStream{}))
	go func() { _ = srv.Serve(lis) }()
	s.T().Cleanup(srv.Stop)

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})

	cases := []struct {
		name     string
		opts     []grpc.DialOption
		expected codes.Code
	}{
		{name: "with token", opts: []grpc.DialOption{stratusv1.WithToken(testToken)}, expected: codes.OK},
		{name: "wrong token", opts: []grpc.DialOption{stratusv1.WithToken("nope")}, expected: codes.Unauthenticated},
		{name: "without token", opts: nil, expected: codes.Unauthenticated},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			opts := append([]grpc.DialOption{dialer, stratusv1.WithInsecure()}, tc.opts...)
			client, err := stratusv1.Dial("passthrough:///bufnet", opts...)
			s.Require().NoError(err)
			defer func() { _ = client.Close() }()

			_, err = client.GetStreamInfo(context.Background())
			s.Equal(tc.expected, status.Code(err))

			// Streaming RPCs are gated too: an unauthenticated stream ends without records.
			ch, err := client.ReadOffset(context.Background(), 1, 1, 0)
			s.Require().NoError(err)
			for range ch {
				s.Fail("unexpected record")
			}
		})
	}
}
