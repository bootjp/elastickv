package etcd

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Test-only permissive guard for fixtures that exercise WAL recovery, not H.
type fixtureNativeAdmission struct{}
type fixtureNativeScope struct{}

func (fixtureNativeAdmission) BindProposal(ctx context.Context, _ uint64, _ []byte) (context.Context, error) {
	return ctx, nil
}

func (fixtureNativeAdmission) WithNative(_ context.Context, _ uint64, action func(raftengine.NativeScope) error) error {
	return action(fixtureNativeScope{})
}
func (fixtureNativeAdmission) ValidateResult(context.Context) error              { return nil }
func (fixtureNativeScope) Observe(raftengine.NativeState)                        {}
func (fixtureNativeScope) Check(context.Context, raftengine.NativeRequest) error { return nil }
func (fixtureNativeScope) Invalidate()                                           {}

type recordingAdmission struct {
	mu          sync.Mutex
	states      []raftengine.NativeState
	suspended   bool
	checks      []raftengine.NativeRequest
	check       func(context.Context, raftengine.NativeRequest) error
	resultError error
	bind        func(context.Context, uint64, []byte) (context.Context, error)
}

func (a *recordingAdmission) BindProposal(ctx context.Context, group uint64, payload []byte) (context.Context, error) {
	if a.bind != nil {
		return a.bind(ctx, group, payload)
	}
	return ctx, nil
}

func (a *recordingAdmission) WithNative(_ context.Context, _ uint64, action func(raftengine.NativeScope) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return action(a)
}
func (a *recordingAdmission) ValidateResult(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.resultError
}
func (a *recordingAdmission) Observe(state raftengine.NativeState) {
	if len(a.states) != 0 && a.states[len(a.states)-1] != state {
		a.suspended = true
	}
	a.states = append(a.states, state)
}
func (a *recordingAdmission) Check(ctx context.Context, request raftengine.NativeRequest) error {
	a.checks = append(a.checks, request)
	if a.suspended {
		return errors.New("suspended")
	}
	if a.check != nil {
		return a.check(ctx, request)
	}
	return nil
}
func (a *recordingAdmission) Invalidate() { a.suspended = true }

func TestNativeAdmissionObservesActualTermBeforeOwnerCallback(t *testing.T) {
	t.Parallel()
	e := identityElectionEngine(t, 1)
	a := &recordingAdmission{}
	e.nativeAdmission = a
	e.nativeConfigurationIndex = 1
	e.admissionChanged = func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		require.True(t, a.suspended)
		require.Equal(t, uint64(10), a.states[len(a.states)-1].Term)
	}
	err := e.withNative(t.Context(), nil, func() error {
		return e.rawNode.Step(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(10)})
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), a.states[0].Term)
	require.Equal(t, uint64(10), a.states[1].Term)
	require.Equal(t, uint64(2), a.states[1].LeaderID)
	called := false
	err = e.withNative(t.Context(), &raftengine.NativeRequest{Operation: raftengine.NativeProposal}, func() error { called = true; return nil })
	require.ErrorIs(t, err, raftengine.ErrNativeAdmissionDenied)
	require.False(t, called)
}

func TestNativeAdmissionProposalKeepsContextAndPayload(t *testing.T) {
	t.Parallel()
	e := identityElectionEngine(t, 1)
	require.NoError(t, e.rawNode.Campaign())
	for _, kind := range []raftpb.MessageType{raftpb.MsgPreVoteResp, raftpb.MsgVoteResp} {
		for _, from := range []uint64{2, 3} {
			require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: kind.Enum(), From: proto.Uint64(from), To: proto.Uint64(1), Term: proto.Uint64(2)}))
		}
	}
	e.refreshStatus()
	require.Equal(t, raftengine.StateLeader, e.State())
	ctx := context.WithValue(t.Context(), nativeTestHandleKey{}, "H")
	a := &recordingAdmission{check: func(got context.Context, request raftengine.NativeRequest) error {
		require.Equal(t, ctx, got)
		require.Equal(t, []byte("data"), request.Payload)
		require.Equal(t, sha256.Sum256([]byte("data")), request.PayloadDigest)
		return errors.New("deny terminal H")
	}}
	e.nativeAdmission = a
	req := proposalRequest{ctx: ctx, id: 19, payload: []byte("data"), payloadDigest: sha256.Sum256([]byte("data")), done: make(chan proposalResult, 1)}
	e.handleProposal(req)
	require.ErrorIs(t, (<-req.done).err, raftengine.ErrNativeAdmissionDenied)
	require.Empty(t, e.pendingProposals)
}

type nativeTestHandleKey struct{}

func TestNativeAdmissionInitialObservePrecedesRecoveryCallback(t *testing.T) {
	t.Parallel()
	e := identityElectionEngine(t, 1)
	a := &recordingAdmission{}
	e.nativeAdmission = a
	e.nativeConfigurationIndex = 1
	called := false
	e.recoveredAdmission = func(state RecoveredState) error {
		// Acquiring A here also proves the publication callback is outside A.
		a.mu.Lock()
		defer a.mu.Unlock()
		require.NotEmpty(t, a.states)
		observed := a.states[len(a.states)-1]
		require.Equal(t, state.HardState().GetTerm(), observed.Term)
		require.Equal(t, state.ConfigurationIndex(), observed.ConfigurationIndex)
		require.Equal(t, raftengine.StateFollower, observed.Role)
		called = true
		return nil
	}
	require.NoError(t, e.publishRecoveredAdmission())
	require.True(t, called)
}

func TestNativeAdmissionInvalidatedResultIsUnknown(t *testing.T) {
	t.Parallel()
	e := &Engine{nativeAdmission: &recordingAdmission{resultError: errors.New("expired H")}}
	require.ErrorIs(t, e.validateNativeResult(t.Context()), raftengine.ErrLeadershipLost)
}

func TestProtectedOpenRejectsMissingNativeGuard(t *testing.T) {
	t.Parallel()
	cfg := OpenConfig{NodeID: 7, GroupID: identityFixture().Identity.Group, NativeIdentity: identityFixture(),
		DataDir: t.TempDir(), StateMachine: &testStateMachine{}, ElectionTick: 10, HeartbeatTick: 1}
	require.ErrorContains(t, validateConfig(cfg), "requires admission")
}

func TestNativeProposalBindingOwnedCopy(t *testing.T) {
	t.Parallel()
	for _, admin := range []bool{false, true} {
		ctx := context.WithValue(t.Context(), nativeTestHandleKey{}, "existing H")
		a := &recordingAdmission{bind: func(got context.Context, group uint64, payload []byte) (context.Context, error) {
			if got.Value(nativeTestHandleKey{}) == nil {
				return nil, errors.New("missing operation H")
			}
			require.Equal(t, uint64(9), group)
			return context.WithValue(got, nativeTestDigestKey{}, sha256.Sum256(payload)), nil
		}}
		e := &Engine{nativeAdmission: a, groupID: 9, proposeCh: make(chan proposalRequest), doneCh: make(chan struct{})}
		propose := e.Propose
		if admin {
			propose = e.ProposeAdmin
		}
		_, err := propose(t.Context(), []byte("no H"))
		require.ErrorContains(t, err, "missing operation H")
		data := []byte("owned data")
		result := make(chan error, 1)
		go func() { _, err := propose(ctx, data); result <- err }()
		req := <-e.proposeCh
		data[0] = 'X'
		require.Equal(t, []byte("owned data"), req.payload)
		require.Equal(t, sha256.Sum256(req.payload), req.payloadDigest)
		require.Equal(t, req.payloadDigest, req.ctx.Value(nativeTestDigestKey{}))
		req.done <- proposalResult{result: &raftengine.ProposalResult{CommitIndex: 10}}
		require.NoError(t, <-result)
	}
}

type nativeTestDigestKey struct{}

func TestNativePeerAdmissionGenerationAndLockOrder(t *testing.T) {
	t.Parallel()
	e, owner, client := protectedQueueFixture(t)
	a := &recordingAdmission{check: func(_ context.Context, request raftengine.NativeRequest) error {
		require.Equal(t, raftengine.NativePeerStep, request.Operation)
		require.Equal(t, uint64(1), request.SessionGeneration)
		return nil
	}}
	e.nativeAdmission = a
	e.groupID, e.nativeConfigurationIndex = 1, 1
	e.admissionChanged = func() { owner.SuspendGroup(1) }
	_, err := client.Send(t.Context(), &pb.EtcdRaftMessage{Message: peerDecoderWire(t, raftpb.MsgHeartbeat, 2, 1)})
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { e.tryHandlePriorityStep(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("A/owner lock cycle")
	}
	require.Equal(t, uint64(2), e.lastLeaderContactFrom)
	projection, ok := owner.GroupProjection(1)
	require.True(t, ok)
	require.Equal(t, peersession.Reconciling, projection.Stage)
}
