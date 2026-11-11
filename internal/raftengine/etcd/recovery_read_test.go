package etcd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// The loopback test transport predates peer sessions. Only its peer-message
// transport lane is trusted here; operation handles/checks use the real arbiter.
// Production receipt/peer-proposal rejection is not bypassed by native code.
type recoveryReadTestAdmission struct {
	*raftadmission.Arbiter
	mu           sync.Mutex
	beforeResult func()
}

type recoveryReadTestScope struct{ raftengine.NativeScope }

func (s recoveryReadTestScope) Check(ctx context.Context, request raftengine.NativeRequest) error {
	if request.Operation == raftengine.NativePeerStep {
		return nil
	}
	return s.NativeScope.Check(ctx, request)
}

func (a *recoveryReadTestAdmission) WithNative(ctx context.Context, group uint64, action func(raftengine.NativeScope) error) error {
	return a.Arbiter.WithNative(ctx, group, func(scope raftengine.NativeScope) error { return action(recoveryReadTestScope{scope}) })
}

func (a *recoveryReadTestAdmission) ValidateResult(ctx context.Context) error {
	a.mu.Lock()
	before := a.beforeResult
	a.mu.Unlock()
	if before != nil {
		before()
	}
	return a.Arbiter.ValidateResult(ctx)
}

func newRecoveryReadCluster(t *testing.T) ([]*transportTestNode, []*recoveryReadTestAdmission) {
	t.Helper()
	nodes, peers := newTransportTestNodes(t, 3)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })
	guards := make([]*recoveryReadTestAdmission, len(nodes))
	for i, node := range nodes {
		a, err := raftadmission.New(raftadmission.Config{Cluster: raftbootstrap.UUID{1}, DefaultGroup: 1, Groups: []uint64{1}, ValidatePayload: func(raftadmission.Kind, uint64, []byte) error { return nil }})
		require.NoError(t, err)
		t.Cleanup(a.Close)
		guards[i] = &recoveryReadTestAdmission{Arbiter: a}
		node.engine, err = Open(t.Context(), OpenConfig{NodeID: node.peer.NodeID, LocalID: node.peer.ID, LocalAddress: node.peer.Address,
			DataDir: node.dir, Peers: peers, Bootstrap: true, Transport: node.transport, StateMachine: node.fsm, NativeAdmission: guards[i], GroupID: 1,
			TickInterval: 20 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 1})
		require.NoError(t, err)
	}
	return nodes, guards
}

func publishRecoveryReadProjection(t *testing.T, node *transportTestNode, guard *recoveryReadTestAdmission, session uint64, ready bool) {
	t.Helper()
	c := raftbootstrap.Configuration{Index: node.engine.Status().ConfigurationIndex}
	node.engine.mu.RLock()
	for id, peer := range node.engine.peers {
		c.State.Voters = append(c.State.Voters, id)
		c.Members = append(c.Members, raftbootstrap.Member{ID: id, Address: peer.Address, PublicKeyDigest: raftbootstrap.Digest{byte(id)}})
	}
	node.engine.mu.RUnlock()
	digest, err := raftbootstrap.ConfigurationDigest(c)
	require.NoError(t, err)
	ticket, err := guard.Ticket(1)
	require.NoError(t, err)
	registration := raftadmission.RegistrationUnknown
	if ready {
		registration = raftadmission.RegistrationNotConfigured
	}
	require.NoError(t, guard.PublishRecovery(ticket, raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1},
		BaselineDigest: digest, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 1, Generation: 1,
		LastConfigurationIndex: c.Index, Configuration: c}, session, registration))
	if ready {
		ticket, err = guard.Ticket(1)
		require.NoError(t, err)
		require.NoError(t, guard.MarkCatalogLoaded(ticket))
	}
}

func TestRecoveryReadIndexActualFollowerUsesOriginalRecoveryHandle(t *testing.T) {
	t.Parallel()
	nodes, guards := newRecoveryReadCluster(t)
	leader := waitForLeaderNode(t, nodes)
	var follower *transportTestNode
	var guard *recoveryReadTestAdmission
	for i, node := range nodes {
		require.Eventually(t, func() bool { return node.engine.Leader().ID != "" }, 5*time.Second, time.Millisecond)
		publishRecoveryReadProjection(t, node, guards[i], 1, false)
		if node != leader {
			follower, guard = node, guards[i]
		}
	}
	require.NotNil(t, follower)
	for _, ready := range []bool{false, true} {
		if ready {
			publishRecoveryReadProjection(t, follower, guard, 2, true)
		}
		recoveryReadFollowerBarrier(t, follower, guard, ready)
	}
	ctx, op, err := guard.Begin(t.Context(), raftadmission.Recovery)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	guard.mu.Lock()
	guard.beforeResult = func() { guard.Suspend(1) }
	guard.mu.Unlock()
	_, err = follower.engine.RecoveryLinearizableRead(ctx)
	require.ErrorIs(t, err, raftengine.ErrLeadershipLost, "post-barrier revocation must withhold the result as unknown")
	_, err = follower.engine.RecoveryLinearizableRead(ctx)
	require.Error(t, err, "a stale handle cannot refresh itself")
	follower.engine.mu.RLock()
	require.Empty(t, follower.engine.pendingReads)
	follower.engine.mu.RUnlock()
}

func recoveryReadFollowerBarrier(t *testing.T, follower *transportTestNode, guard *recoveryReadTestAdmission, ready bool) {
	t.Helper()
	stage := peersession.IdentityReady
	if ready {
		stage = peersession.DataReady
	}
	require.Equal(t, stage, guard.Stage(1))
	ctx, op, err := guard.Begin(t.Context(), raftadmission.Recovery)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	index, err := follower.engine.RecoveryLinearizableRead(ctx)
	require.NoError(t, err)
	require.NotZero(t, index)
	require.GreaterOrEqual(t, follower.engine.AppliedIndex(), index)
	_, err = follower.engine.LinearizableRead(ctx)
	require.ErrorIs(t, err, errNotLeader)
	_, err = follower.engine.RecoveryLinearizableRead(t.Context())
	require.ErrorIs(t, err, raftengine.ErrNativeAdmissionDenied)
	require.ErrorIs(t, follower.engine.handleTransportMessage(ctx, raftpb.Message{Type: raftpb.MsgProp.Enum()}), errPeerProposalForbidden)
	if ready {
		data, dataOp, err := guard.Begin(t.Context(), raftadmission.Data)
		require.NoError(t, err)
		defer dataOp.CancelAndJoin()
		_, err = follower.engine.RecoveryLinearizableRead(data)
		require.ErrorIs(t, err, raftengine.ErrNativeAdmissionDenied)
	}
}

func TestRecoveryReadIndexRequiresNativeGuard(t *testing.T) {
	t.Parallel()
	_, err := (&Engine{}).RecoveryLinearizableRead(t.Context())
	require.ErrorIs(t, err, raftengine.ErrNativeAdmissionDenied)
}

func TestRecoveryReadIndexCancellationRemovesPendingRequest(t *testing.T) {
	t.Parallel()
	e := identityElectionEngine(t, 1)
	e.nativeAdmission = fixtureNativeAdmission{}
	e.readCh = make(chan readRequest)
	e.doneCh = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() { defer close(done); _, err := e.RecoveryLinearizableRead(ctx); result <- err }()
	t.Cleanup(func() { cancel(); <-done })
	request := <-e.readCh
	require.True(t, request.recovery)
	// This real RawNode has no known leader, so its ReadIndex cannot complete.
	e.handleRead(request)
	e.mu.RLock()
	require.Len(t, e.pendingReads, 1)
	e.mu.RUnlock()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled recovery read did not return")
	}
	e.mu.RLock()
	require.Empty(t, e.pendingReads)
	e.mu.RUnlock()
}
