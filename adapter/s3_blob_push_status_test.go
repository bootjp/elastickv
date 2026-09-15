package adapter

import (
	"context"
	"crypto/sha256"
	"net"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rejectingS3BlobFetchServer refuses the push before reading a single frame,
// which is what an auth interceptor or a fail-closed capability check does.
type rejectingS3BlobFetchServer struct {
	pb.UnimplementedS3BlobFetchServer
}

func (rejectingS3BlobFetchServer) PushChunkBlob(pb.S3BlobFetch_PushChunkBlobServer) error {
	return status.Error(codes.Unauthenticated, "peer token required")
}

// TestPushChunkBlobSurfacesTheServersStatusWhenItRejectsEarly pins the gRPC
// contract that a client-stream Send reports a server-terminated stream as a
// bare io.EOF: the real status is only available from the receive side.
//
// A payload larger than the flow-control window guarantees the sender blocks
// long enough to observe the reset, which is the case the caller must not see
// as an opaque EOF -- the replicator decides whether to retry from this code.
func TestPushChunkBlobSurfacesTheServersStatusWhenItRejectsEarly(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb.RegisterS3BlobFetchServer(server, rejectingS3BlobFetchServer{})
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		<-serveDone
	})

	cluster := NewGRPCS3BlobCluster("n1", staticS3BlobMembership{}, "read-only-admin", "peer-secret")
	t.Cleanup(func() { require.NoError(t, cluster.Close()) })

	payload := make([]byte, 4<<20)
	digest := sha256.Sum256(payload)
	replica := S3BlobReplica{NodeID: "n2", Address: listener.Addr().String(), Suffrage: "voter"}

	err = cluster.PushChunkBlob(context.Background(), replica, digest, payload, 7)
	require.Equal(t, codes.Unauthenticated, status.Code(err),
		"a stream the server reset must report the server's status, not io.EOF")
}
