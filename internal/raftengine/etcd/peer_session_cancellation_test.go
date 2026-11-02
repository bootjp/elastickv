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
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestProtectedDispatchCancelsTLSHandshake(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	local := peersession.Node{Cluster: raftbootstrap.UUID{1}, Member: 1, Address: "127.0.0.1:9999", Key: sha256.Sum256(leaf.RawSubjectPublicKeyInfo)}
	remote := peersession.Node{Cluster: local.Cluster, Member: 2, Address: listener.Addr().String(), Key: raftbootstrap.Digest{2}}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	owner, err := peersession.NewOwner(peersession.Config{Local: local, Versions: versions,
		Provisioned: []peersession.Provision{{Node: local, Groups: []uint64{1}}, {Node: remote, Groups: []uint64{1}}}, Methods: PeerSessionMethods(1, nil)})
	require.NoError(t, err)
	defer owner.Close()
	require.NoError(t, owner.UpdateGroup(peersession.Group{ID: 1, Generation: 1, Minimum: versions, State: peersession.Intact,
		Configuration: raftbootstrap.Configuration{State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{
			{ID: 1, Address: local.Address, PublicKeyDigest: local.Key}, {ID: 2, Address: remote.Address, PublicKeyDigest: remote.Key},
		}}}))
	transport := NewGRPCTransport([]Peer{{NodeID: 2, ID: "n2", Address: remote.Address}})
	defer func() { require.NoError(t, transport.Close()) }()
	require.NoError(t, transport.ConfigurePeerSessions(owner, 1, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, map[uint64]peersession.Node{2: remote}))
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- transport.Dispatch(ctx, raftpb.Message{Type: raftpb.MsgApp.Enum(), From: uint64Ptr(1), To: uint64Ptr(2)})
	}()
	select {
	case conn := <-accepted:
		defer func() { _ = conn.Close() }()
	case <-time.After(2 * time.Second):
		t.Fatal("TLS connection was not attempted")
	}
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("dispatch ignored caller cancellation during TLS handshake")
	}
	require.Empty(t, transport.conns)
	require.Empty(t, transport.clients)
}
