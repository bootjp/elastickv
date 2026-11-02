package peersession

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
)

func TestNativeBootRevocationSpansGroups(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	provisions := []Provision{{Node: f.clientNode, Groups: []uint64{testGroup, testGroup + 1}}, {Node: f.serverNode, Groups: []uint64{testGroup, testGroup + 1}}}
	require.NoError(t, f.client.ReplaceProvisioning(provisions))
	require.NoError(t, f.server.ReplaceProvisioning(provisions))
	g := f.group
	g.ID++
	require.NoError(t, f.client.UpdateGroup(g))
	require.NoError(t, f.server.UpdateGroup(g))
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	oldClient := f.client
	cc := f.dial(t)
	old, err := oldClient.Handshake(ctx, cc, g.ID, f.serverNode)
	require.NoError(t, err)
	var reply wire.Frame
	req := frameMessage(Message{Group: g.ID, From: 2, To: 1, Traffic: Vote})
	require.NoError(t, old.Invoke(ctx, cc, sendMethod, req, &reply))
	var methods []Method
	for _, m := range oldClient.methods {
		methods = append(methods, m)
	}
	newClient, err := NewOwner(Config{Local: f.clientNode, Provisioned: provisions, Versions: oldClient.versions, Methods: methods})
	require.NoError(t, err)
	t.Cleanup(func() { newClient.Close(); require.Equal(t, Counts{}, newClient.Counts()) })
	require.NoError(t, newClient.UpdateGroup(f.group))
	f.client = newClient
	freshCC := f.dial(t)
	f.handshake(t, freshCC)
	// The replacement boot was admitted in group 17; old group 18 must also
	// lose its session even though its own group state never changed.
	require.Eventually(t, func() bool {
		select {
		case <-old.Done():
			return true
		default:
			return false
		}
	}, testTimeout, time.Millisecond)
	require.Error(t, old.Invoke(ctx, cc, sendMethod, req, &reply))
	require.Equal(t, 1, f.server.Counts().Active)
}

func TestNativeTLS12PublicCompatibilityAndEMSPeer(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	public := f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}))
	_, err := healthpb.NewHealthClient(public).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	f.clientTLS = f.clientTLS.Clone()
	f.clientTLS.MinVersion = tls.VersionTLS12
	f.clientTLS.MaxVersion = tls.VersionTLS12
	cc := f.dial(t)
	h := f.handshake(t, cc)
	var reply wire.Frame
	require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
	var remote peer.Peer
	_, err = healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Peer(&remote))
	require.NoError(t, err)
	auth, ok := remote.AuthInfo.(*ClientAuthInfo)
	require.True(t, ok)
	require.Equal(t, uint16(tls.VersionTLS12), auth.State.Version)
	unsafe := f.clientTLS.Clone()
	unsafe.Renegotiation = tls.RenegotiateOnceAsClient
	_, err = f.client.TransportCredentials(unsafe, false)
	require.Error(t, err)
}

func TestNativeServerHostnameVerificationAndTLSConfiguration(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	for _, mutate := range []func(*tls.Config){
		func(c *tls.Config) { c.InsecureSkipVerify = true },
		func(c *tls.Config) { c.Certificates = nil },
		func(c *tls.Config) { c.MaxVersion = tls.VersionTLS11 },
		func(c *tls.Config) {
			c.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return c, nil }
		},
	} {
		cfg := f.clientTLS.Clone()
		mutate(cfg)
		_, err := f.client.TransportCredentials(cfg, false)
		require.Error(t, err)
	}
	serverCfg := f.serverTLS.Clone()
	serverCfg.ClientAuth = tls.RequestClientCert
	_, err := f.server.TransportCredentials(serverCfg, true)
	require.Error(t, err, "unverified certificate mode is not peer identity")
	tc, err := f.client.TransportCredentials(f.clientTLS, false)
	require.NoError(t, err)
	cc := f.dialCredentials(t, tc, grpc.WithAuthority("wrong.test"))
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	_, err = f.client.Handshake(ctx, cc, testGroup, f.serverNode)
	require.Error(t, err)
	require.Zero(t, f.server.Counts().Active)
}

func TestBoundContextPropagatesParentAndSessionCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(t.Context())
	life, cancelLife := context.WithCancel(t.Context())
	defer cancelLife()
	child, cleanup := boundContext(parent, life)
	defer cleanup()
	cancelParent()
	select {
	case <-child.Done():
	case <-time.After(testTimeout):
		t.Fatal("parent cancellation was lost")
	}
	other, done := boundContext(t.Context(), life)
	defer done()
	cancelLife()
	select {
	case <-other.Done():
	case <-time.After(testTimeout):
		t.Fatal("session revocation was lost")
	}
}
