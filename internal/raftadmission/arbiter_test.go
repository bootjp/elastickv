package raftadmission

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func admissionPolicy(t *testing.T, id uint64) raftbootstrap.PolicyRecord {
	t.Helper()
	c := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1}},
		Members: []raftbootstrap.Member{{ID: 1, Address: "local:9000", PublicKeyDigest: sha256.Sum256([]byte("local key"))}}}
	d, err := raftbootstrap.ConfigurationDigest(c)
	require.NoError(t, err)
	return raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion,
		Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: id}, BaselineDigest: d,
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 1,
		Generation: 1, LastConfigurationIndex: 1, Configuration: c}
}

func newAdmission(t *testing.T, rotate bool) *Arbiter {
	t.Helper()
	a, err := New(Config{Cluster: raftbootstrap.UUID{1}, DefaultGroup: 1, Groups: []uint64{1, 2},
		RotationRequested: rotate, ValidatePayload: func(kind Kind, _ uint64, payload []byte) error {
			if len(payload) != 1 || payload[0] != byte(kind) {
				return ErrClosed
			}
			return nil
		}})
	require.NoError(t, err)
	t.Cleanup(a.Close)
	for _, id := range []uint64{1, 2} {
		observeAdmission(t, a, id, 1)
		publishAdmission(t, a, id, 1)
	}
	return a
}

func observeAdmission(t *testing.T, a *Arbiter, id, term uint64) {
	t.Helper()
	require.NoError(t, a.WithNative(t.Context(), id, func(s raftengine.NativeScope) error {
		s.Observe(raftengine.NativeState{Term: term, Role: raftengine.StateLeader, LeaderID: 1, ConfigurationIndex: 1})
		return nil
	}))
}

func publishAdmission(t *testing.T, a *Arbiter, id, session uint64) {
	t.Helper()
	ticket, err := a.Ticket(id)
	require.NoError(t, err)
	require.NoError(t, a.PublishRecovery(ticket, admissionPolicy(t, id), session, RegistrationNotConfigured))
}

func loadAdmissionCatalog(t *testing.T, a *Arbiter) {
	t.Helper()
	ticket, err := a.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, a.MarkCatalogLoaded(ticket))
}

func TestAdmissionStagesAndExactPayload(t *testing.T) {
	a := newAdmission(t, false)
	require.Equal(t, peersession.ControlReady, a.Stage(1))
	_, _, err := a.Begin(t.Context(), Data)
	require.ErrorIs(t, err, ErrClosed)
	ctx, control, err := a.Begin(t.Context(), HLC)
	require.NoError(t, err)
	defer control.CancelAndJoin()
	_, err = a.BindProposal(ctx, 1, []byte{byte(Data)})
	require.ErrorIs(t, err, ErrClosed)
	bound, err := a.BindProposal(ctx, 1, []byte{byte(HLC)})
	require.NoError(t, err)
	request := raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: sha256.Sum256([]byte{byte(HLC)})}
	require.NoError(t, a.WithNative(t.Context(), 1, func(s raftengine.NativeScope) error { return s.Check(bound, request) }))
	request.PayloadDigest[0]++
	require.Error(t, a.WithNative(t.Context(), 1, func(s raftengine.NativeScope) error { return s.Check(bound, request) }))
	_, err = a.BindProposal(context.Background(), 1, []byte{byte(HLC)})
	require.ErrorIs(t, err, ErrStale)
	loadAdmissionCatalog(t, a)
	require.Equal(t, peersession.DataReady, a.Stage(2))
	require.NoError(t, control.Finish())
}

func TestAdmissionTermChangeRetiresQueuedOperationAndPeer(t *testing.T) {
	a := newAdmission(t, false)
	loadAdmissionCatalog(t, a)
	ctx, op, err := a.Begin(t.Context(), Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	bound, err := a.BindProposal(ctx, 2, []byte{byte(Data)})
	require.NoError(t, err)
	observeAdmission(t, a, 2, 2)
	require.Error(t, a.WithNative(t.Context(), 2, func(s raftengine.NativeScope) error {
		return s.Check(context.Background(), raftengine.NativeRequest{Operation: raftengine.NativePeerStep, SessionGeneration: 1})
	}))
	publishAdmission(t, a, 2, 2)
	require.Equal(t, peersession.IdentityReady, a.Stage(2), "retired owner must join before reopening")
	require.Error(t, a.WithNative(t.Context(), 2, func(s raftengine.NativeScope) error {
		return s.Check(bound, raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: sha256.Sum256([]byte{byte(Data)})})
	}))
	require.Error(t, a.ValidateResult(ctx))
	op.CancelAndJoin()
	require.Equal(t, peersession.DataReady, a.Stage(2))
}

func TestAdmissionDirectReadIsRevokedBeforeTargetRouting(t *testing.T) {
	a := newAdmission(t, false)
	loadAdmissionCatalog(t, a)
	ctx, op, err := a.Begin(t.Context(), Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	observeAdmission(t, a, 2, 2)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("direct read retained authority across a native change before routing")
	}
	publishAdmission(t, a, 2, 2)
	require.ErrorIs(t, a.ValidateResult(ctx), ErrStale)
	_, _, err = a.Begin(t.Context(), Data)
	require.ErrorIs(t, err, ErrClosed, "replacement data requests wait for old request retirement")
	op.CancelAndJoin()
	_, replacement, err := a.Begin(t.Context(), Data)
	require.NoError(t, err)
	replacement.CancelAndJoin()
}

func TestAdmissionDefaultTermRevokesPermissionAndCatalogReceipt(t *testing.T) {
	a := newAdmission(t, false)
	loadAdmissionCatalog(t, a)
	old, err := a.Ticket(1)
	require.NoError(t, err)
	ctx, op, err := a.Begin(t.Context(), Data)
	require.NoError(t, err)
	observeAdmission(t, a, 1, 2)
	publishAdmission(t, a, 1, 2)
	require.ErrorIs(t, a.MarkCatalogLoaded(old), ErrStale)
	require.Error(t, a.ValidateResult(ctx))
	op.CancelAndJoin()
	require.Equal(t, peersession.ControlReady, a.Stage(2))
	loadAdmissionCatalog(t, a)
	require.Equal(t, peersession.DataReady, a.Stage(2))
}

func TestAdmissionRetiredChildrenJoinBeforeReadiness(t *testing.T) {
	a := newAdmission(t, false)
	loadAdmissionCatalog(t, a)
	_, op, err := a.Begin(t.Context(), Maintenance)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	require.NoError(t, op.Go(func(ctx context.Context) { <-ctx.Done(); close(entered); <-release }))
	observeAdmission(t, a, 1, 2)
	<-entered
	publishAdmission(t, a, 1, 2)
	loadAdmissionCatalog(t, a)
	require.Equal(t, peersession.IdentityReady, a.Stage(1))
	joined := make(chan struct{})
	go func() { op.CancelAndJoin(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("retired operation returned before child joined")
	default:
	}
	close(release)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("retired child did not join")
	}
	require.Equal(t, peersession.DataReady, a.Stage(1))
}

func TestAdmissionCancellationAndFatalRemainClosed(t *testing.T) {
	a := newAdmission(t, false)
	loadAdmissionCatalog(t, a)
	parent, cancel := context.WithCancel(t.Context())
	ctx, op, err := a.Begin(parent, Data)
	require.NoError(t, err)
	cancel()
	require.Error(t, a.ValidateResult(ctx))
	op.CancelAndJoin()
	a.LatchFatal(ErrClosed)
	observeAdmission(t, a, 1, 2)
	require.Equal(t, peersession.Reconciling, a.Stage(1))
	_, _, err = a.Begin(t.Context(), Registration)
	require.Error(t, err)
}

func TestAdmissionRequestedRotationIsNotNotRequested(t *testing.T) {
	a := newAdmission(t, true)
	loadAdmissionCatalog(t, a)
	require.Equal(t, peersession.IdentityReady, a.Stage(1))
	for _, kind := range []Kind{Data, Control, Catalog, Allocator, HLC, Rotation, Maintenance} {
		_, _, err := a.Begin(t.Context(), kind)
		require.Error(t, err)
	}
}
