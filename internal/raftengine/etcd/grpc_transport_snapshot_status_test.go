package etcd

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rejectingSnapshotServer refuses the snapshot before reading a single chunk,
// the way a receiver out of spool space or holding a conflicting group ID does.
type rejectingSnapshotServer struct {
	pb.UnimplementedEtcdRaftServer
}

func (rejectingSnapshotServer) SendSnapshot(pb.EtcdRaft_SendSnapshotServer) error {
	return status.Error(codes.FailedPrecondition, "receiver is not accepting snapshots")
}

// TestSnapshotSendersSurfaceTheReceiversStatus covers all three snapshot send
// paths. gRPC reports a stream the receiver has already terminated to the
// sender as a bare io.EOF, so a sender that returns the Send error loses the
// receiver's actual reason -- and a snapshot that fails for a diagnosable
// cause (no headroom, wrong group, unsupported format) shows up in the
// operator's log as "EOF".
//
// The payload is deliberately larger than the flow-control window but smaller
// than gRPC's 4 MiB send cap. Larger than the window is what guarantees the
// sender is still writing when the reset arrives -- the case where the two
// error paths differ. Under the cap matters because an oversized message makes
// the client reject it locally with a status of its own, which would satisfy
// the assertion without the stream ever being reset.
func TestSnapshotSendersSurfaceTheReceiversStatus(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb.RegisterEtcdRaftServer(server, rejectingSnapshotServer{})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()

	payload := make([]byte, 1<<20)
	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Data:     payload,
			Metadata: testSnapshotMetadata(9, 3, nil),
		},
	}

	newTransport := func(t *testing.T) *GRPCTransport {
		t.Helper()
		transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: listener.Addr().String()}})
		t.Cleanup(func() { require.NoError(t, transport.Close()) })
		// The default 16 MiB chunk turns this payload into a single write, and
		// a single write can win the race against the reset. Chunking small
		// keeps the sender writing across the reset in every run, so the test
		// measures the error path rather than the scheduler.
		transport.snapshotChunkSize = 32 << 10
		return transport
	}

	t.Run("in-memory payload", func(t *testing.T) {
		t.Parallel()
		err := newTransport(t).sendSnapshot(context.Background(), msg)
		require.Equal(t, codes.FailedPrecondition, grpcStatusCode(err))
	})

	t.Run("spooled payload", func(t *testing.T) {
		t.Parallel()
		spool, err := newSnapshotSpool(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, spool.Close()) })
		_, err = spool.Write(payload)
		require.NoError(t, err)

		err = newTransport(t).sendSnapshotSpool(context.Background(), msg, spool)
		require.Equal(t, codes.FailedPrecondition, grpcStatusCode(err))
	})

	t.Run("streamed FSM snapshot", func(t *testing.T) {
		t.Parallel()
		open := func(uint64) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(payload)), nil
		}
		err := newTransport(t).streamFSMSnapshot(context.Background(), msg, 9, open)
		require.Equal(t, codes.FailedPrecondition, grpcStatusCode(err))
	})
}
