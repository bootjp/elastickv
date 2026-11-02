package etcd

import (
	"context"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

func TestProtectedPeerDialDoesNotFallBackWithoutIdentity(t *testing.T) {
	peer := Peer{NodeID: 2, ID: "n2", Address: "127.0.0.1:1"}
	transport := NewGRPCTransport([]Peer{peer})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	transport.peerSession = &peerSessionTransport{nodes: map[uint64]peersession.Node{}}
	client, err := transport.clientForPeer(peer)
	require.ErrorIs(t, err, peersession.ErrDenied)
	require.Nil(t, client)
	require.Empty(t, transport.conns)
	require.Empty(t, transport.clients)
}

func TestProtectedStreamSkipsEmptyProbeAndKeepsUnaryClient(t *testing.T) {
	t.Setenv(sendStreamEnabledEnvVar, "true")
	for _, unimplemented := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "fallback"}[unimplemented], func(t *testing.T) {
			transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: "peer:9000"}})
			t.Cleanup(func() { require.NoError(t, transport.Close()) })
			stream := &testRaftSendStreamClient{}
			if unimplemented {
				stream.sendErr = status.Error(codes.Unimplemented, "legacy stream")
			}
			underlying := &testEtcdRaftClient{raftStream: stream}
			client := &sessionRaftClient{EtcdRaftClient: underlying}
			// This tests transport routing, not TLS/session proof validation.
			require.NoError(t, transport.probeSendStream(t.Context(), "peer:9000", client))
			require.Zero(t, underlying.sendStreamCalls.Load())
			req := &pb.EtcdRaftMessage{Message: []byte("actual message")}
			err := transport.dispatchRegularStream(t.Context(), "peer:9000", client, req)
			if unimplemented {
				require.Equal(t, codes.Unimplemented, grpcStatusCode(err))
				require.NoError(t, transport.dispatchRegularUnary(t.Context(), client, req))
				require.Equal(t, int32(1), underlying.sendCalls.Load())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int32(1), underlying.sendStreamCalls.Load())
			require.Len(t, stream.messages, 1)
		})
	}
}

func TestProtectedDialWaitHonorsCancellation(t *testing.T) {
	peer := Peer{NodeID: 2, ID: "n2", Address: "peer:9000"}
	transport := NewGRPCTransport([]Peer{peer})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	transport.peerSession = &peerSessionTransport{}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	transport.peerDialGates = map[string]chan struct{}{peer.Address: gate}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := transport.clientForPeerContext(ctx, peer)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, gate, 1)
	require.Empty(t, transport.conns)
}

func TestProtectedPublicationRejectsPeerChanges(t *testing.T) {
	for _, change := range []string{"remove", "address", "remove-readd", "shutdown", "cancel"} {
		t.Run(change, func(t *testing.T) {
			peer := Peer{NodeID: 2, ID: "n2", Address: "127.0.0.1:1"}
			transport := NewGRPCTransport([]Peer{peer})
			t.Cleanup(func() { require.NoError(t, transport.Close()) })
			generation := transport.peerGeneration
			conn, err := grpc.NewClient(peer.Address, internalutil.GRPCDialOptions()...)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch change {
			case "remove":
				transport.RemovePeer(2)
			case "address":
				updated := peer
				updated.Address = "127.0.0.1:2"
				transport.UpsertPeer(updated)
			case "remove-readd":
				transport.RemovePeer(2)
				transport.UpsertPeer(peer)
			case "shutdown":
				require.NoError(t, transport.Close())
			case "cancel":
				cancel()
			}
			client, err := transport.publishProtectedPeer(ctx, peer, generation, conn, pb.NewEtcdRaftClient(conn))
			require.Error(t, err)
			require.Nil(t, client)
			require.Equal(t, connectivity.Shutdown, conn.GetState())
			require.Empty(t, transport.clients)
		})
	}
}

func TestProtectedPeerWithoutNodeIDUsesDerivedIdentity(t *testing.T) {
	peer := Peer{ID: "n2", Address: "peer:9000"}
	transport := NewGRPCTransport([]Peer{peer})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	transport.peerSession = &peerSessionTransport{nodes: map[uint64]peersession.Node{}}
	_, err := transport.clientForPeerContext(t.Context(), peer)
	// Reaching dial's missing-provision check proves normalized roster matching.
	require.ErrorIs(t, err, peersession.ErrDenied)
	_, ok := transport.peers[DeriveNodeID("n2")]
	require.True(t, ok)
	require.Equal(t, transport.peers[DeriveNodeID("n2")], normalizeSessionPeer(peer))
}

func TestProtectedStreamWaitHonorsCancellation(t *testing.T) {
	transport := NewGRPCTransport(nil)
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	transport.peerDialGates = map[string]chan struct{}{"stream:peer:9000": gate}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := transport.streamFor(ctx, "peer:9000", &sessionRaftClient{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, gate, 1)
}

func TestPeerClientCacheDiscardsExpiredSession(t *testing.T) {
	transport := NewGRPCTransport(nil)
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	transport.clients["peer:9000"] = &sessionRaftClient{handle: &peersession.Handle{}}
	client, found := transport.cachedPeerClient("peer:9000")
	require.False(t, found)
	require.Nil(t, client)
	require.Empty(t, transport.clients)
	require.True(t, transport.peerSessionConfigFrozen)
}

func TestPeerSessionConfigurationRejectsMissingOwner(t *testing.T) {
	transport := NewGRPCTransport(nil)
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	require.ErrorIs(t, transport.ConfigurePeerSessions(nil, 1, nil, nil), peersession.ErrDenied)
}

func TestClosedTransportCannotDialOrPublishPeerClient(t *testing.T) {
	transport := NewGRPCTransport(nil)
	transport.peerSession = &peerSessionTransport{}
	require.NoError(t, transport.Close())
	require.ErrorIs(t, transport.lifecycleCtx.Err(), context.Canceled)
	client, err := transport.clientForPeer(Peer{NodeID: 2, Address: "127.0.0.1:1"})
	require.Error(t, err)
	require.Nil(t, client)
	require.Empty(t, transport.conns)
}

func TestLegacyTransportCanCreateFreshClientAfterClose(t *testing.T) {
	peer := Peer{NodeID: 2, Address: "127.0.0.1:1"}
	transport := NewGRPCTransport([]Peer{peer})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	old, err := transport.clientForPeer(peer)
	require.NoError(t, err)
	oldConn := transport.conns[peer.Address]
	require.NoError(t, transport.Close())
	require.Equal(t, connectivity.Shutdown, oldConn.GetState())
	require.ErrorIs(t, transport.lifecycleCtx.Err(), context.Canceled)
	client, err := transport.clientForPeerContext(t.Context(), peer)
	require.NoError(t, err)
	require.NotSame(t, old, client)
	require.NotSame(t, oldConn, transport.conns[peer.Address])
	require.NotEqual(t, connectivity.Shutdown, transport.conns[peer.Address].GetState())
	cached, err := transport.clientForPeerContext(t.Context(), peer)
	require.NoError(t, err)
	require.Same(t, client, cached)
}
