package server

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	authMetadataKey = "authorization"
	bearerPrefix    = "Bearer "
)

func UnaryAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorize(ctx, token); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func StreamAuthInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorize(ss.Context(), token); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func authorize(ctx context.Context, token string) error {
	if token == "" {
		return status.Error(codes.Unauthenticated, "authentication is not configured")
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}

	for _, value := range md.Get(authMetadataKey) {
		got, found := strings.CutPrefix(value, bearerPrefix)
		if found && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
			return nil
		}
	}

	return status.Error(codes.Unauthenticated, "invalid or missing bearer token")
}
