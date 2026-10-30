package peersession

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type staticCredentials map[string]string

func (s staticCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return s, nil
}
func (staticCredentials) RequireTransportSecurity() bool { return true }

func beginOnly(t *testing.T, f *nativeFixture, cc *grpc.ClientConn) (*handshakePin, *session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	h, err := f.client.clientHello(testGroup, f.serverNode)
	require.NoError(t, err)
	pin := &handshakePin{owner: f.client, remote: f.serverNode, group: testGroup, method: wire.PeerSession_Begin_FullMethodName}
	a, id, err := exchangeBegin(ctx, cc, pin, h)
	require.NoError(t, err)
	s, err := f.client.clientPending(pin.connection, h, a, id)
	require.NoError(t, err)
	return pin, s
}

func TestNativePendingAndCrossPhysicalConnection(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	first := f.dial(t)
	second := f.dial(t)
	pin, s := beginOnly(t, f, first)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	pendingHandle := &Handle{owner: f.client, c: pin.connection, s: s}
	var reply wire.Frame
	require.Error(t, pendingHandle.Invoke(ctx, first, sendMethod, request(Vote), &reply))
	finish, err := f.client.clientFinishRequest(pin.connection, s)
	require.NoError(t, err)
	// An otherwise valid client proof delivered on another real TLS connection
	// has no pending server session. Session IDs are not bearer capabilities.
	err = second.Invoke(ctx, wire.PeerSession_Finish_FullMethodName, finish, &reply, grpc.ForceCodec(StrictCodec{}))
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	h, err := f.client.finishClient(ctx, first, pin, s)
	require.NoError(t, err)
	require.NoError(t, h.Invoke(ctx, first, sendMethod, request(Vote), &reply))
	require.Error(t, h.Invoke(ctx, second, sendMethod, request(Vote), &reply))
	require.Equal(t, int64(1), f.service.delivered.Load())
}

func TestNativeUnauthenticatedSpoofAndVersions(t *testing.T) {
	for _, name := range []string{"no-cert", "token-alone", "wrong-ca", "unprovisioned-key", "spoof-id", "spoof-address", "spoof-server", "wrong-group", "version-zero", "old-no-session"} {
		t.Run(name, func(t *testing.T) {
			f := setupNative(t, fixtureOptions{})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			h, err := f.client.clientHello(testGroup, f.serverNode)
			require.NoError(t, err)
			cc := negativeDial(t, f, name)
			body := spoofHello(t, h, name)
			if name == "token-alone" {
				ctx = metadata.AppendToOutgoingContext(ctx, "x-startup-control", "not-peer-identity", "authorization", "not-peer-identity")
			}
			var reply wire.Frame
			if name == "old-no-session" {
				err = cc.Invoke(ctx, sendMethod, request(Vote), &reply)
			} else {
				err = cc.Invoke(ctx, wire.PeerSession_Begin_FullMethodName, &wire.Frame{Body: body}, &reply)
			}
			require.Error(t, err)
			require.Zero(t, f.service.delivered.Load())
			require.Zero(t, f.server.Counts().Active)
		})
	}
}

func negativeDial(t *testing.T, f *nativeFixture, name string) *grpc.ClientConn {
	t.Helper()
	switch name {
	case "no-cert", "token-alone":
		return f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, MinVersion: tls.VersionTLS13}))
	case "wrong-ca":
		p := newPKI(t)
		cert, _ := p.certificate(t, 12)
		return f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}))
	case "unprovisioned-key":
		cert, _ := f.pki.certificate(t, 13)
		return f.dialCredentials(t, credentials.NewTLS(&tls.Config{RootCAs: f.pki.roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}))
	default:
		return f.dial(t)
	}
}
func spoofHello(t *testing.T, h hello, name string) []byte {
	t.Helper()
	switch name {
	case "spoof-id":
		h.Client.Member++
	case "spoof-address":
		h.Client.Address = "wrong.test:9001"
	case "spoof-server":
		h.Server.Member++
	case "wrong-group":
		h.Group++
	}
	b, err := encodeHello(h)
	require.NoError(t, err)
	if name == "version-zero" {
		offset := len(b) - binary.Size(view{})
		binary.BigEndian.PutUint32(b[offset:], 0)
	}
	return b
}

func TestNativePayloadMethodAndSessionBinding(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	for _, m := range []Message{
		{Group: testGroup + 1, From: 2, To: 1, Traffic: Vote},
		{Group: testGroup, From: 3, To: 1, Traffic: Vote},
		{Group: testGroup, From: 2, To: 3, Traffic: Vote},
		{Group: testGroup, From: 2, To: 1, Traffic: Traffic(99)},
	} {
		// Bypass client preflight on purpose: the server still fully decodes and
		// rejects the payload even with native owner-minted RPC credentials.
		err := cc.Invoke(ctx, sendMethod, frameMessage(m), &reply, grpc.PerRPCCredentials(h))
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	f.client.mu.Lock()
	mac := rpcProof(h.s, sendMethod)
	sid := h.s.id
	f.client.mu.Unlock()
	copied := staticCredentials{sessionHeader: hex.EncodeToString(sid[:]), proofHeader: hex.EncodeToString(mac[:])}
	err := cc.Invoke(ctx, otherMethod, request(Vote), &reply, grpc.PerRPCCredentials(copied))
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	copied[sessionHeader] = hex.EncodeToString(make([]byte, 32))
	err = cc.Invoke(ctx, sendMethod, request(Vote), &reply, grpc.PerRPCCredentials(copied))
	require.Error(t, err)
	duplicate := metadata.AppendToOutgoingContext(ctx, sessionHeader, hex.EncodeToString(sid[:]))
	err = cc.Invoke(duplicate, sendMethod, request(Vote), &reply, grpc.PerRPCCredentials(h))
	require.Error(t, err)
	require.Zero(t, f.service.delivered.Load())
	require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
}

func TestNativeReconnectAndTransparentRetryRequireNewHandshake(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	tc, err := f.client.TransportCredentials(f.clientTLS, false)
	require.NoError(t, err)
	cc := f.dialCredentials(t, tc, grpc.WithDefaultServiceConfig(`{"methodConfig":[{"name":[{"service":"test.Peer","method":"Send"}],"retryPolicy":{"MaxAttempts":3,"InitialBackoff":"0.001s","MaxBackoff":"0.001s","BackoffMultiplier":1,"RetryableStatusCodes":["UNAVAILABLE","UNKNOWN"]}}]}`))
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	f.service.retryClose.Store(true)
	// Deliberately use native Invoke, not Handle.Invoke's cancel-on-revoke:
	// gRPC is allowed to retry, but the old credential MUST reject its new TLS.
	err = cc.Invoke(ctx, sendMethod, request(Replication), &reply, grpc.PerRPCCredentials(h))
	require.Error(t, err)
	require.Equal(t, int64(1), f.service.delivered.Load())
	require.Eventually(t, func() bool {
		f.client.mu.Lock()
		defer f.client.mu.Unlock()
		for c := range f.client.connections {
			if c != h.c {
				return true
			}
		}
		return false
	}, testTimeout, time.Millisecond)
	newHandle := f.handshake(t, cc)
	require.NotEqual(t, h.SessionID(), newHandle.SessionID())
	require.NotSame(t, h.c, newHandle.c)
	require.NoError(t, newHandle.Invoke(ctx, cc, sendMethod, request(Replication), &reply))
	require.Equal(t, int64(2), f.service.delivered.Load())
}

type badAckServer struct {
	wire.UnimplementedPeerSessionServer
	owner *Owner
}

func (s *badAckServer) Begin(ctx context.Context, f *wire.Frame) (*wire.Frame, error) {
	return s.owner.Begin(ctx, f)
}
func (s *badAckServer) Finish(ctx context.Context, f *wire.Frame) (*wire.Frame, error) {
	a, err := s.owner.Finish(ctx, f)
	if err == nil {
		a.Body[len(a.Body)-1] ^= 1
	}
	return a, err
}
func TestNativeOppositeAckProofRequired(t *testing.T) {
	f := setupNative(t, fixtureOptions{wrap: func(o *Owner) wire.PeerSessionServer { return &badAckServer{owner: o} }})
	cc := f.dial(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	_, err := f.client.Handshake(ctx, cc, testGroup, f.serverNode)
	require.Error(t, err)
	require.Eventually(t, func() bool { return f.server.Counts().Active == 0 && f.client.Counts().Active == 0 }, testTimeout, time.Millisecond)
}

func TestNativeWrongFinishProofRetiresPending(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	pin, s := beginOnly(t, f, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	wrong := proofFrame(finishKind, s.id, raftbootstrap.Digest{})
	err := cc.Invoke(ctx, wire.PeerSession_Finish_FullMethodName, wrong, &reply)
	require.Error(t, err)
	_, err = f.client.finishClient(ctx, cc, pin, s)
	require.Error(t, err)
	require.Zero(t, f.server.Counts().Active)
}

func TestNativeStagePendingRoleAndPolicyRevocation(t *testing.T) {
	for _, kind := range []string{"pending", "stage", "role", "policy", "key-mapping"} {
		t.Run(kind, func(t *testing.T) {
			f := setupNative(t, fixtureOptions{})
			cc := f.dial(t)
			h := f.handshake(t, cc)
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			var reply wire.Frame
			require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
			revokeNative(t, f, kind)
			require.Eventually(t, func() bool {
				select {
				case <-h.Done():
					return true
				default:
					return false
				}
			}, testTimeout, time.Millisecond)
			require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
			require.Equal(t, int64(1), f.service.delivered.Load())
			if kind == "pending" || kind == "stage" || kind == "role" {
				next := f.handshake(t, cc)
				require.NoError(t, next.Invoke(ctx, cc, sendMethod, request(Probe), &reply))
				require.Error(t, next.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
			}
		})
	}
}

func revokeNative(t *testing.T, f *nativeFixture, kind string) {
	t.Helper()
	g := f.group
	g.Generation++
	switch kind {
	case "pending":
		g.Pending = PendingTransition
	case "stage":
		g.Stage = Reconciling
	case "role":
		g.Configuration.State.Voters = []uint64{1}
		g.Configuration.State.Learners = []uint64{2}
	case "policy":
		g.Policy[0]++
	case "key-mapping":
		require.NoError(t, f.server.ReplaceProvisioning([]Provision{{Node: f.serverNode, Groups: []uint64{testGroup}}}))
		return
	}
	require.NoError(t, f.server.UpdateGroup(g))
}

type testCatchupAuthorizer struct {
	mu             sync.Mutex
	pending        Pending
	phase          GrantPhase
	invalidBinding bool
}

func (a *testCatchupAuthorizer) Recheck(_ context.Context, b Binding) (CatchupStatus, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	candidate, leader := b.Remote, b.Local
	if candidate.Member == 1 {
		candidate, leader = leader, candidate
	}
	c := CatchupStatus{Binding: b, Pending: a.pending, Phase: a.phase, Candidate: candidate, Leader: leader, LeaderTerm: 5, Barrier: 10, AppliedAdd: raftbootstrap.Digest{1}, Grant: raftbootstrap.Digest{2}}
	if a.invalidBinding {
		c.Binding.Session[0]++
	}
	return c, nil
}
func TestNativePrejoinLostAndExactCatchupGate(t *testing.T) {
	for _, state := range []State{Prejoin, Lost} {
		f := setupNative(t, fixtureOptions{clientState: state, learner: true})
		cc := f.dial(t)
		h := f.handshake(t, cc)
		ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
		var reply wire.Frame
		require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(Probe), &reply))
		for _, traffic := range []Traffic{Vote, Replication, Catchup, Registration, Control, Data, PolicyControl} {
			require.Error(t, h.Invoke(ctx, cc, sendMethod, request(traffic), &reply))
		}
		cancel()
	}
	a := &testCatchupAuthorizer{}
	f := setupNative(t, fixtureOptions{clientState: Prejoin, learner: true, authorizer: a})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Catchup), &reply), "omitted pending gate/ack is not authority")
	a.mu.Lock()
	a.pending = PendingClear
	a.phase = GrantReciprocallyAcknowledged
	a.invalidBinding = true
	a.mu.Unlock()
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Catchup), &reply))
	a.mu.Lock()
	a.invalidBinding = false
	a.mu.Unlock()
	require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(Catchup), &reply))
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Control), &reply))
	// A trusted authorizer revocation is checked again on the very next chunk.
	a.mu.Lock()
	a.pending = PendingTransition
	a.mu.Unlock()
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Catchup), &reply))
}

func TestNativePreactivationIntactElectionLane(t *testing.T) {
	f := setupNative(t, fixtureOptions{preactivation: true})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	for _, traffic := range []Traffic{Probe, Vote, Replication, PolicyControl} {
		require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(traffic), &reply))
	}
	for _, traffic := range []Traffic{Control, Data} {
		require.Error(t, h.Invoke(ctx, cc, sendMethod, request(traffic), &reply))
	}
	g := f.group
	g.Generation++
	g.Floor = raftbootstrap.Floor{ActivationIndex: 8, Generation: 1}
	require.NoError(t, f.server.UpdateGroup(g))
	g.Generation++
	g.Floor = raftbootstrap.Floor{}
	require.Error(t, f.server.UpdateGroup(g))
}

type secureControlCredentials struct {
	checked *atomic.Bool
}

func (c secureControlCredentials) RequireTransportSecurity() bool { return true }
func (c secureControlCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	ri, ok := credentials.RequestInfoFromContext(ctx)
	if !ok {
		return nil, ErrDenied
	}
	if err := credentials.CheckSecurityLevel(ri.AuthInfo, credentials.PrivacyAndIntegrity); err != nil {
		return nil, err
	}
	c.checked.Store(true)
	return map[string]string{"x-test-startup-control": "test-only-control"}, nil
}
func TestNativeControlCredentialSecurityLevel(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	checked := &atomic.Bool{}
	var reply wire.Frame
	f.service.requireControlToken.Store(true)
	require.Error(t, cc.Invoke(ctx, sendMethod, request(Control), &reply, grpc.PerRPCCredentials(h)), "existing method token auth is still conjunctive")
	require.NoError(t, h.Invoke(ctx, cc, sendMethod, request(Control), &reply, grpc.PerRPCCredentials(secureControlCredentials{checked: checked})))
	require.True(t, checked.Load())
}

func TestNativeCredentialCompositionRejectsReservedHeaderCollision(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	var reply wire.Frame
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply, grpc.PerRPCCredentials(staticCredentials{"X-Elastickv-Peer-Session": "not-a-session"})))
	require.Zero(t, f.service.delivered.Load())
	require.Error(t, cc.Invoke(ctx, sendMethod, request(Control), &reply, grpc.PerRPCCredentials(staticCredentials{"x-test-startup-control": "test-only-control"})), "a control token alone never establishes a peer session")
}

func TestNativeClientConnCloseDestroysBothSides(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	require.NoError(t, cc.Close())
	require.Eventually(t, func() bool {
		select {
		case <-h.Done():
			return f.client.Counts() == (Counts{}) && f.server.Counts() == (Counts{})
		default:
			return false
		}
	}, testTimeout, time.Millisecond)
}

func TestNativeNewBootRetiresOldNonceAndSessions(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	oldClient := f.client
	oldCC := f.dial(t)
	old := f.handshake(t, oldCC)
	oldClient.mu.Lock()
	var provisions []Provision
	for _, p := range oldClient.provisioned {
		provisions = append(provisions, p)
	}
	var methods []Method
	for _, m := range oldClient.methods {
		methods = append(methods, m)
	}
	oldBoot := oldClient.boot
	oldClient.mu.Unlock()
	newClient, err := NewOwner(Config{Local: f.clientNode, Provisioned: provisions, Versions: oldClient.versions, Methods: methods})
	require.NoError(t, err)
	t.Cleanup(newClient.Close)
	require.NoError(t, newClient.UpdateGroup(f.group))
	f.client = newClient
	newCC := f.dial(t)
	fresh := f.handshake(t, newCC)
	require.NotEqual(t, old.SessionID(), fresh.SessionID())
	require.Eventually(t, func() bool {
		select {
		case <-old.Done():
			return true
		default:
			return false
		}
	}, testTimeout, time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	h, err := newClient.clientHello(testGroup, f.serverNode)
	require.NoError(t, err)
	h.Boot = oldBoot
	b, err := encodeHello(h)
	require.NoError(t, err)
	third := f.dial(t)
	var reply wire.Frame
	err = third.Invoke(ctx, wire.PeerSession_Begin_FullMethodName, &wire.Frame{Body: b}, &reply)
	require.Error(t, err, "a retired boot cannot be readmitted by delayed Begin")
}

func TestNativeStreamValidatesEveryChunkBeforeSpooling(t *testing.T) {
	for _, firstValid := range []bool{false, true} {
		f := setupNative(t, fixtureOptions{})
		cc := f.dial(t)
		h := f.handshake(t, cc)
		ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
		s, err := cc.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, snapshotMethod, grpc.PerRPCCredentials(h))
		require.NoError(t, err)
		var reply wire.Frame
		if firstValid {
			require.NoError(t, s.SendMsg(request(Replication)))
			require.NoError(t, s.RecvMsg(&reply))
		}
		bad := frameMessage(Message{Group: testGroup, From: 99, To: 1, Traffic: Replication})
		_ = s.SendMsg(bad)
		require.Error(t, s.RecvMsg(&reply))
		select {
		case <-f.service.ended:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if firstValid {
			path := <-f.service.spools
			require.NoFileExists(t, path)
			require.Equal(t, int64(1), f.service.delivered.Load())
		} else {
			require.Empty(t, f.service.spools)
			require.Zero(t, f.service.delivered.Load())
		}
		cancel()
	}
}

func TestNativeServerOutboundPayloadValidation(t *testing.T) {
	f := setupNative(t, fixtureOptions{})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	f.service.spoofReply.Store(true)
	var reply wire.Frame
	require.Error(t, h.Invoke(ctx, cc, sendMethod, request(Vote), &reply))
}
