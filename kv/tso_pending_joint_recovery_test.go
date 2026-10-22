package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type jointRecoveryReleaseEngine struct {
	*prepareSafetyEngine
	floor    uint64
	failure  error
	attempts int
}

func (e *jointRecoveryReleaseEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if bytes.Equal(data, EncodePrepareEvidenceRelease(e.floor)) {
		e.attempts++
		if e.failure != nil {
			return nil, errors.WithStack(e.failure)
		}
	}
	return e.prepareSafetyEngine.ProposeAdmin(ctx, data)
}

type jointRecoveryImage struct {
	pending []byte
	active  []byte
	data    [2][]byte
	replay  [][]byte
	floor   uint64
}

func jointRecoverySnapshot(t *testing.T, fsm raftengine.StateMachine) []byte {
	t.Helper()
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snapshot.Close()) }()
	var buf bytes.Buffer
	_, err = snapshot.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

func jointRecoveryTimestampGroup(state *TSOStateMachine, engine *recordingTSOEngine) *ShardGroup {
	engine.leader.ID = "self"
	return &ShardGroup{TSOState: state, Engine: &prepareSafetyEngine{
		recordingTSOEngine: engine,
		members:            []raftengine.Server{{ID: "self", Address: "self", Suffrage: "voter"}},
	}}
}

func captureJointRecoveryImage(t *testing.T) jointRecoveryImage {
	t.Helper()
	shards, _, first := newPrepareSafetyGate(t)
	other, secondEngine, second := newPrepareSafetyGate(t)
	shards.groups[2] = other.groups[1]
	t.Cleanup(func() { delete(shards.groups, 2) })
	allocator, state, engine, provider := pendingTestAllocator(t, shards)
	allocator.group = jointRecoveryTimestampGroup(state, engine)
	shards.groups[0] = allocator.group
	provider.setFloor(100)
	release := &jointRecoveryReleaseEngine{
		prepareSafetyEngine: secondEngine,
		failure:             errors.New("second data group release temporarily unavailable"),
	}
	shards.groups[2].Engine = release
	var image jointRecoveryImage
	engine.afterPropose = func(data []byte) {
		if isTSOPendingPhaseDEntry(data) {
			image.floor = state.PendingPhaseDFloor()
			release.floor = image.floor
			image.pending = jointRecoverySnapshot(t, state)
		}
		if bytes.HasPrefix(data, []byte(tsoPhaseDEnvelope)) {
			image.replay = append(image.replay, bytes.Clone(data))
		}
	}
	reservation, err := allocator.ReserveBatchAfter(t.Context(), 2, 0, true, true)
	require.ErrorIs(t, err, release.failure)
	require.Equal(t, TSOReservation{}, reservation)
	require.NotEmpty(t, image.pending)
	require.Greater(t, image.floor, uint64(100))
	require.Equal(t, [][]byte{marshalTSOPhaseD(image.floor)}, image.replay)
	require.True(t, state.PhaseDActive())
	require.Zero(t, state.AllocationFloor(), "partial release must not reserve a timestamp window")
	require.Equal(t, 1, release.attempts)
	assertJointRecoveryDataGate(t, first, image.floor, true)
	assertJointRecoveryDataGate(t, second, image.floor, false)
	image.active = jointRecoverySnapshot(t, state)
	image.data[0] = jointRecoverySnapshot(t, first)
	image.data[1] = jointRecoverySnapshot(t, second)
	return image
}

func assertJointRecoveryDataGate(t *testing.T, fsm *kvFSM, floor uint64, released bool) {
	t.Helper()
	sealed, releaseFloor, err := readPrepareEvidenceCreationGate(t.Context(), fsm.store)
	require.NoError(t, err)
	require.Equal(t, !released, sealed)
	if released {
		require.Equal(t, floor, releaseFloor)
	} else {
		require.Zero(t, releaseFloor)
	}
	enabled, unproven, actualFloor, err := InspectPrepareEvidence(t.Context(), fsm.store, floor)
	require.NoError(t, err)
	require.True(t, enabled)
	require.False(t, unproven)
	require.Equal(t, floor, actualFloor)
}

// The pending snapshot predates the active log record. Recovery must replay
// that record before resuming a release which already reached one data group.
func TestTSOPendingJointRecoveryPreservesBarrierUntilAllDataGroupsRelease(t *testing.T) {
	image := captureJointRecoveryImage(t)
	for _, scenario := range []struct {
		name     string
		snapshot []byte
		replay   [][]byte
	}{
		{name: "pending_snapshot_with_active_log_replay", snapshot: image.pending, replay: image.replay},
		{name: "active_snapshot_with_partial_release", snapshot: image.active},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			state := NewTSOStateMachine(NewHLC())
			require.NoError(t, state.Restore(bytes.NewReader(scenario.snapshot)))
			require.Equal(t, image.floor, state.PrepareEvidenceProtocolFloor())
			require.Zero(t, state.AllocationFloor())
			shards, firstEngine, first := newPrepareSafetyGate(t)
			other, secondEngine, second := newPrepareSafetyGate(t)
			require.NoError(t, first.Restore(bytes.NewReader(image.data[0])))
			require.NoError(t, second.Restore(bytes.NewReader(image.data[1])))
			assertJointRecoveryDataGate(t, first, image.floor, true)
			assertJointRecoveryDataGate(t, second, image.floor, false)
			shards.groups[2] = other.groups[1]
			t.Cleanup(func() { delete(shards.groups, 2) })
			engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 2,
				leader: raftengine.LeaderInfo{Address: "self"}, apply: applyTSOTestFSM(state)}
			shards.groups[0] = jointRecoveryTimestampGroup(state, engine)
			release := &jointRecoveryReleaseEngine{prepareSafetyEngine: secondEngine,
				floor: image.floor, failure: errors.New("restored data group release temporarily unavailable")}
			shards.groups[2].Engine = release
			provider := &recordingTSOFloorProvider{floor: image.floor + 1000}
			allocator, err := NewRaftTSOAllocator(shards.groups[0], state.hlc,
				WithTSOCutoverFloorProvider(provider), WithTSOPhaseDActivationGate(shards))
			require.NoError(t, err)
			if len(scenario.replay) != 0 {
				require.False(t, state.PhaseDActive())
				require.Equal(t, image.floor, state.PendingPhaseDFloor())
				timestamp, issueErr := allocator.Next(t.Context())
				require.ErrorIs(t, issueErr, ErrTSOPhaseDActivationPending)
				require.Zero(t, timestamp)
				require.Empty(t, engine.proposedPayloads())
				for _, entry := range scenario.replay {
					require.Nil(t, state.Apply(entry))
				}
			}
			require.True(t, state.PhaseDActive())
			require.Equal(t, image.floor, state.PhaseDFloor())
			timestamp, err := allocator.Next(t.Context())
			require.ErrorIs(t, err, release.failure)
			require.Zero(t, timestamp)
			require.Equal(t, 1, release.attempts, "the retry must reach the actual release fan-out")
			require.Zero(t, provider.callCount(), "an unfinished release must not sample a replacement boundary")
			require.Zero(t, state.AllocationFloor())
			require.Empty(t, engine.proposedPayloads(), "no allocation may be proposed before every data group releases")
			assertJointRecoveryDataGate(t, first, image.floor, true)
			assertJointRecoveryDataGate(t, second, image.floor, false)
			require.ErrorIs(t, applyFSMRequest(t, second,
				prepareSafetyAdmittedCreation("raw", image.floor+1, image.floor)), ErrPrepareCreationSealed)
			release.failure = nil
			base, err := allocator.NextBatch(t.Context(), 2)
			require.NoError(t, err)
			require.Greater(t, base, image.floor)
			require.Equal(t, base+1, state.AllocationFloor())
			require.Equal(t, image.floor, state.PhaseDFloor())
			require.Equal(t, image.floor, state.PrepareEvidenceProtocolFloor())
			require.Zero(t, state.PendingPhaseDFloor())
			require.Equal(t, 2, release.attempts)
			require.Equal(t, 2, jointRecoveryReleaseCount(firstEngine, image.floor),
				"an already released group must safely accept both release retries")
			for _, fsm := range []*kvFSM{first, second} {
				assertJointRecoveryDataGate(t, fsm, image.floor, true)
				require.NoError(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation("raw", base, image.floor)))
			}
		})
	}
}

func jointRecoveryReleaseCount(engine *prepareSafetyEngine, floor uint64) int {
	count := 0
	for _, payload := range engine.proposedPayloads() {
		if bytes.Equal(payload, EncodePrepareEvidenceRelease(floor)) {
			count++
		}
	}
	return count
}
