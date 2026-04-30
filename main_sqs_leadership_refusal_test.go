package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// fakeLeadershipController is a sqsLeadershipController test
// double. It records TransferLeadership invocations and exposes
// the registered leader-acquired callback so tests can fire it
// manually (the real engine fires it from refreshStatus on a
// state transition; tests don't need a real raft loop).
type fakeLeadershipController struct {
	state              raftengine.State
	transferCalls      atomic.Int32
	transferErr        error
	registeredCb       func()
	deregisterCalls    atomic.Int32
	registerCalls      atomic.Int32
	transferRecvCancel chan struct{}
}

func (f *fakeLeadershipController) State() raftengine.State {
	return f.state
}

func (f *fakeLeadershipController) TransferLeadership(_ context.Context) error {
	f.transferCalls.Add(1)
	if f.transferRecvCancel != nil {
		close(f.transferRecvCancel)
	}
	return f.transferErr
}

func (f *fakeLeadershipController) RegisterLeaderAcquiredCallback(fn func()) func() {
	f.registerCalls.Add(1)
	f.registeredCb = fn
	return func() { f.deregisterCalls.Add(1) }
}

// awaitTransferCalls waits up to 1s for at least n TransferLeadership
// calls to land. Needed because refuse() offloads to a goroutine —
// a synchronous assertion would race the goroutine.
func (f *fakeLeadershipController) awaitTransferCalls(t *testing.T, n int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if f.transferCalls.Load() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.GreaterOrEqual(t, f.transferCalls.Load(), n,
		"expected at least %d TransferLeadership call(s)", n)
}

// TestInstallSQSLeadershipRefusal_HTFIFOCapableNoOp pins the
// happy-path early return: a binary that ADVERTISES htfifo never
// needs to refuse leadership. The hook must NOT register a
// callback, and the returned deregister must be a safe no-op.
func TestInstallSQSLeadershipRefusal_HTFIFOCapableNoOp(t *testing.T) {
	t.Parallel()
	admin := &fakeLeadershipController{state: raftengine.StateLeader}
	dereg := installSQSLeadershipRefusal(
		context.Background(), admin, 7,
		map[uint64]bool{7: true}, // partitioned queue on this group
		true,                     // binary HAS htfifo
		slog.Default(),
	)
	require.Zero(t, admin.transferCalls.Load(),
		"htfifo-capable binary must not refuse leadership at startup")
	require.Zero(t, admin.registerCalls.Load(),
		"htfifo-capable binary must not register a leader-acquired callback")
	require.NotPanics(t, dereg)
}

// TestInstallSQSLeadershipRefusal_NoPartitionedQueueNoOp pins the
// other early-return: a group with NO partitioned queues mapped
// to it doesn't need the policy hook either, even when the binary
// lacks htfifo.
func TestInstallSQSLeadershipRefusal_NoPartitionedQueueNoOp(t *testing.T) {
	t.Parallel()
	admin := &fakeLeadershipController{state: raftengine.StateLeader}
	dereg := installSQSLeadershipRefusal(
		context.Background(), admin, 99,
		map[uint64]bool{7: true, 8: true}, // group 99 NOT in set
		false,                             // binary lacks htfifo
		slog.Default(),
	)
	require.Zero(t, admin.transferCalls.Load(),
		"group with no partitioned queue mapping must not be refused")
	require.Zero(t, admin.registerCalls.Load())
	require.NotPanics(t, dereg)
}

// TestInstallSQSLeadershipRefusal_StartupAlreadyLeaderRefuses pins
// the startup branch: install at a moment when the engine is
// already StateLeader — refuse() must fire immediately so the
// cluster steps the unsafe leader down without waiting for a
// future re-election.
func TestInstallSQSLeadershipRefusal_StartupAlreadyLeaderRefuses(t *testing.T) {
	t.Parallel()
	admin := &fakeLeadershipController{state: raftengine.StateLeader}
	_ = installSQSLeadershipRefusal(
		context.Background(), admin, 7,
		map[uint64]bool{7: true},
		false, // binary lacks htfifo
		slog.Default(),
	)
	admin.awaitTransferCalls(t, 1)
	require.Equal(t, int32(1), admin.registerCalls.Load(),
		"the per-acquisition observer must also be registered "+
			"so future re-elections trigger the same refusal")
}

// TestInstallSQSLeadershipRefusal_StartupFollowerWaits pins the
// startup-follower branch: install at a moment when the engine is
// NOT leader — refuse() must NOT fire yet. The callback must be
// registered so a future leader-acquisition triggers refusal.
func TestInstallSQSLeadershipRefusal_StartupFollowerWaits(t *testing.T) {
	t.Parallel()
	admin := &fakeLeadershipController{state: raftengine.StateFollower}
	_ = installSQSLeadershipRefusal(
		context.Background(), admin, 7,
		map[uint64]bool{7: true},
		false,
		slog.Default(),
	)
	require.Zero(t, admin.transferCalls.Load(),
		"follower must not be refused at install time")
	require.Equal(t, int32(1), admin.registerCalls.Load(),
		"per-acquisition observer must still be registered")
}

// TestInstallSQSLeadershipRefusal_AcquisitionTriggersRefuse pins
// the per-acquisition path: a node that becomes leader AFTER
// install must be refused via the leader-acquired callback.
func TestInstallSQSLeadershipRefusal_AcquisitionTriggersRefuse(t *testing.T) {
	t.Parallel()
	admin := &fakeLeadershipController{state: raftengine.StateFollower}
	_ = installSQSLeadershipRefusal(
		context.Background(), admin, 7,
		map[uint64]bool{7: true},
		false,
		slog.Default(),
	)
	require.NotNil(t, admin.registeredCb,
		"callback must be registered for the per-acquisition path")

	// Simulate refreshStatus firing the observer after the node
	// became leader.
	admin.registeredCb()
	admin.awaitTransferCalls(t, 1)
}

// TestInstallSQSLeadershipRefusal_DeregisterPropagates pins that
// the returned deregister flows through to the engine's
// deregister hook. Coordinators with shorter lifetimes than the
// engine MUST call this to avoid accumulating dead callbacks.
func TestInstallSQSLeadershipRefusal_DeregisterPropagates(t *testing.T) {
	t.Parallel()
	admin := &fakeLeadershipController{state: raftengine.StateFollower}
	dereg := installSQSLeadershipRefusal(
		context.Background(), admin, 7,
		map[uint64]bool{7: true},
		false,
		slog.Default(),
	)
	dereg()
	require.Equal(t, int32(1), admin.deregisterCalls.Load())
}

// TestInstallSQSLeadershipRefusal_TransferErrorLogged pins the
// error path: TransferLeadership returning an error must NOT
// crash anything; refuse() logs and moves on. The callback can
// fire again at the next leader-acquired event.
func TestInstallSQSLeadershipRefusal_TransferErrorLogged(t *testing.T) {
	t.Parallel()
	admin := &fakeLeadershipController{
		state:       raftengine.StateLeader,
		transferErr: errors.New("simulated transfer failure"),
	}
	require.NotPanics(t, func() {
		_ = installSQSLeadershipRefusal(
			context.Background(), admin, 7,
			map[uint64]bool{7: true},
			false,
			slog.Default(),
		)
	})
	admin.awaitTransferCalls(t, 1)
}

// TestInstallSQSLeadershipRefusal_NilAdminIsSafe pins the typed-
// nil guard: a missing controller must not crash. Returns a
// no-op deregister so callers can defer uniformly.
func TestInstallSQSLeadershipRefusal_NilAdminIsSafe(t *testing.T) {
	t.Parallel()
	dereg := installSQSLeadershipRefusal(
		context.Background(), nil, 7,
		map[uint64]bool{7: true},
		false,
		slog.Default(),
	)
	require.NotPanics(t, dereg)
}

// TestPartitionedGroupSet_FlattensRouting pins that
// partitionedGroupSet collapses --sqsFifoPartitionMap into the
// {gid → bool} set the leadership-refusal hook consumes.
func TestPartitionedGroupSet_FlattensRouting(t *testing.T) {
	t.Parallel()
	in := map[string]sqsFifoQueueRouting{
		"orders.fifo": {partitionCount: 4, groups: []string{"10", "11", "12", "13"}},
		"events.fifo": {partitionCount: 2, groups: []string{"20", "21"}},
	}
	got := partitionedGroupSet(in, slog.Default())
	require.Equal(t, map[uint64]bool{
		10: true, 11: true, 12: true, 13: true,
		20: true, 21: true,
	}, got)
}

// TestPartitionedGroupSet_EmptyReturnsNil pins the empty-input
// fast path. An operator running a non-partitioned cluster should
// not pay for an empty map allocation.
func TestPartitionedGroupSet_EmptyReturnsNil(t *testing.T) {
	t.Parallel()
	require.Nil(t, partitionedGroupSet(nil, slog.Default()))
	require.Nil(t, partitionedGroupSet(map[string]sqsFifoQueueRouting{}, slog.Default()))
}

// TestPartitionedGroupSet_SkipsMalformedGroupRef pins the
// defensive log-and-skip branch: a group reference that escaped
// canonicalisation (test seeding the map directly) is logged but
// does not panic. The valid groups still end up in the set.
func TestPartitionedGroupSet_SkipsMalformedGroupRef(t *testing.T) {
	t.Parallel()
	in := map[string]sqsFifoQueueRouting{
		"q.fifo": {partitionCount: 2, groups: []string{"42", "not-a-uint64"}},
	}
	got := partitionedGroupSet(in, slog.Default())
	require.Equal(t, map[uint64]bool{42: true}, got,
		"malformed group ref is skipped; valid one survives")
}
