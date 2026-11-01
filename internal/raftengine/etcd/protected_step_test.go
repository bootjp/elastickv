package etcd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func queueCertificate(t *testing.T, serial int64) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{"peer.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, leaf
}

func protectedQueueFixture(t *testing.T) (*Engine, *peersession.Owner, pb.EtcdRaftClient) {
	t.Helper()
	serverCert, serverLeaf := queueCertificate(t, 1)
	clientCert, clientLeaf := queueCertificate(t, 2)
	roots := x509.NewCertPool()
	roots.AddCert(serverLeaf)
	roots.AddCert(clientLeaf)
	local := peersession.Node{Cluster: raftbootstrap.UUID{1}, Member: 1, Address: "local:9000", Key: sha256.Sum256(serverLeaf.RawSubjectPublicKeyInfo)}
	remote := peersession.Node{Cluster: local.Cluster, Member: 2, Address: "remote:9000", Key: sha256.Sum256(clientLeaf.RawSubjectPublicKeyInfo)}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	provisions := []peersession.Provision{{Node: local, Groups: []uint64{1}}, {Node: remote, Groups: []uint64{1}}}
	makeOwner := func(node peersession.Node) *peersession.Owner {
		o, err := peersession.NewOwner(peersession.Config{Local: node, Versions: versions, Provisioned: provisions,
			MethodFactory: func(o *peersession.Owner) ([]peersession.Method, error) { return PeerSessionMethodsForOwner(1, o), nil }})
		require.NoError(t, err)
		t.Cleanup(o.Close)
		require.NoError(t, o.UpdateGroup(peersession.Group{ID: 1, Generation: 1, Minimum: versions,
			Stage: peersession.IdentityReady, State: peersession.Intact, Pending: peersession.PendingClear, MarkerReady: true,
			Floor: raftbootstrap.Floor{ActivationIndex: 1, Generation: 1},
			Configuration: raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{
				{ID: 1, Address: local.Address, PublicKeyDigest: local.Key}, {ID: 2, Address: remote.Address, PublicKeyDigest: remote.Key},
			}}}))
		return o
	}
	owner, clientOwner := makeOwner(local), makeOwner(remote)
	e := identityElectionEngine(t, 1)
	e.fsmSnapDir = t.TempDir()
	e.protectedStepCh = make(chan protectedStep, 8)
	e.protectedPriorityStepCh = make(chan protectedStep, 8)
	e.startedCh = make(chan struct{})
	close(e.startedCh)
	e.transport.SetHandler(e.handleTransportMessage)
	e.transport.peerSession = &peerSessionTransport{owner: owner}
	e.transport.fsmSnapDir = e.fsmSnapDir
	e.transport.SetFSMSnapshotProtection(e.protectReceivedFSMSnapshot, e.unprotectReceivedFSMSnapshot)
	options, err := owner.ServerOptions(&tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: roots,
		ClientAuth: tls.VerifyClientCertIfGiven, MinVersion: tls.VersionTLS13})
	require.NoError(t, err)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(options...)
	owner.Register(server)
	e.transport.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	credentials, err := clientOwner.TransportCredentials(&tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: roots,
		ServerName: "peer.test", MinVersion: tls.VersionTLS13}, false)
	require.NoError(t, err)
	cc, err := grpc.NewClient("passthrough:///peer.test", grpc.WithTransportCredentials(credentials),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	h, err := clientOwner.Handshake(ctx, cc, 1, local)
	require.NoError(t, err)
	return e, owner, pb.NewEtcdRaftClient(&sessionClientConn{conn: cc, handle: h})
}

func TestProtectedQueueRevocation(t *testing.T) {
	for _, mode := range []string{"unary", "stream", "snapshot"} {
		for _, revoke := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/live", true: "/revoked"}[revoke], func(t *testing.T) {
				e, owner, client := protectedQueueFixture(t)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				msg := &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(1)}
				if mode == "snapshot" {
					msg.Type = raftpb.MsgSnap.Enum()
					msg.Snapshot = &raftpb.Snapshot{Metadata: testSnapshotMetadata(10, 1, []uint64{1, 2, 3})}
				}
				raw, err := proto.Marshal(msg)
				require.NoError(t, err)
				switch mode {
				case "unary":
					_, err = client.Send(ctx, &pb.EtcdRaftMessage{Message: raw})
				case "stream":
					var stream pb.EtcdRaft_SendStreamClient
					stream, err = client.SendStream(ctx)
					require.NoError(t, err)
					require.NoError(t, stream.Send(&pb.EtcdRaftMessage{Message: raw}))
					_, err = stream.CloseAndRecv()
				case "snapshot":
					var stream pb.EtcdRaft_SendSnapshotClient
					stream, err = client.SendSnapshot(ctx)
					require.NoError(t, err)
					require.NoError(t, stream.Send(&pb.EtcdRaftSnapshotChunk{Metadata: raw}))
					require.NoError(t, stream.Send(&pb.EtcdRaftSnapshotChunk{Chunk: []byte("snapshot bytes"), Final: true}))
					_, err = stream.CloseAndRecv()
				}
				require.NoError(t, err)
				queue := protectedTestQueue(e, msg.GetType())
				require.Len(t, queue, 1)
				if revoke {
					owner.SuspendGroup(1)
				}
				consumeProtectedTestStep(t, e, msg.GetType())
				if revoke {
					require.Zero(t, e.lastLeaderContactFrom)
					require.Equal(t, uint64(1), e.rawNode.BasicStatus().GetCommit())
					if mode == "snapshot" {
						_, err := os.Stat(fsmSnapPath(e.fsmSnapDir, 10))
						require.ErrorIs(t, err, os.ErrNotExist)
						require.Empty(t, e.protectedReceivedFSMSnaps)
					}
				} else {
					require.Equal(t, uint64(2), e.lastLeaderContactFrom)
				}
			})
		}
	}
}

func protectedTestQueue(e *Engine, kind raftpb.MessageType) chan protectedStep {
	if isInboundPriorityMsg(kind) {
		return e.protectedPriorityStepCh
	}
	return e.protectedStepCh
}

func consumeProtectedTestStep(t *testing.T, e *Engine, kind raftpb.MessageType) {
	t.Helper()
	if isInboundPriorityMsg(kind) {
		require.True(t, e.tryHandlePriorityStep())
		return
	}
	e.handleProtectedStep(<-e.protectedStepCh)
}

func TestProtectedQueueAdmissionScope(t *testing.T) {
	e, owner, client := protectedQueueFixture(t)
	raw := peerDecoderWire(t, raftpb.MsgHeartbeat, 2, 1)
	_, err := client.Send(t.Context(), &pb.EtcdRaftMessage{Message: raw})
	require.NoError(t, err)
	request := <-e.protectedPriorityStepCh
	entered, release := make(chan struct{}), make(chan struct{})
	scoped := make(chan error, 1)
	go func() {
		scoped <- request.receipt.WithAdmission(t.Context(), func() error {
			close(entered)
			<-release // Test-only barrier at the terminal-action boundary.
			return nil
		})
	}()
	<-entered
	revoked := make(chan struct{})
	go func() { owner.SuspendGroup(1); close(revoked) }()
	select {
	case <-revoked:
		t.Error("revocation crossed terminal admission scope")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-scoped)
	<-revoked
	called := false
	require.Error(t, request.receipt.WithAdmission(t.Context(), func() error { called = true; return nil }))
	require.False(t, called)
	// The receipt also survives RPC completion but not a later owner suspension.
	e.handleProtectedStep(request)
	require.Zero(t, e.lastLeaderContactFrom)
}
