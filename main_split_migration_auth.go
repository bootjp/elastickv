package main

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Keep migration credentials on runner clients, not on the shared peer cache.
type splitMigrationAuthConn struct {
	grpc.ClientConnInterface
	token string
}

func (c splitMigrationAuthConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("authorization", "Bearer "+c.token)
	return errors.WithStack(c.ClientConnInterface.Invoke(metadata.NewOutgoingContext(ctx, md), method, args, reply, opts...))
}

func newSplitMigrationInternalClient(conn grpc.ClientConnInterface, token string) pb.InternalClient {
	if token != "" {
		conn = splitMigrationAuthConn{ClientConnInterface: conn, token: token}
	}
	return pb.NewInternalClient(conn)
}
