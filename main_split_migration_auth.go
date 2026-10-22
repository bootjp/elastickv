package main

import (
	"context"

	internalutil "github.com/bootjp/elastickv/internal"
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
	ctx = migrationCredentialContext(ctx)
	opts = append(opts, grpc.PerRPCCredentials(internalutil.GRPCBearerToken(c.token)))
	return errors.WithStack(c.ClientConnInterface.Invoke(ctx, method, args, reply, opts...))
}

func (c splitMigrationAuthConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx = migrationCredentialContext(ctx)
	opts = append(opts, grpc.PerRPCCredentials(internalutil.GRPCBearerToken(c.token)))
	stream, err := c.ClientConnInterface.NewStream(ctx, desc, method, opts...)
	return stream, errors.WithStack(err)
}

func migrationCredentialContext(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Delete("authorization")
	return metadata.NewOutgoingContext(ctx, md)
}

func newSplitMigrationInternalClient(conn grpc.ClientConnInterface, token string) pb.InternalClient {
	if token != "" {
		conn = splitMigrationAuthConn{ClientConnInterface: conn, token: token}
	}
	return pb.NewInternalClient(conn)
}
