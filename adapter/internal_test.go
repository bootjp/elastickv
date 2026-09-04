package adapter

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestInternalForwardObservesCommittedWrites(t *testing.T) {
	t.Parallel()

	reqs := []*pb.Request{{
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte("hot"),
			Value: []byte("value"),
		}},
	}}
	txn := &forwardObserverTxn{}
	var observed []*pb.Request
	internal := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil, WithInternalForwardWriteObserver(func(reqs []*pb.Request) {
		observed = reqs
	}))

	resp, err := internal.Forward(context.Background(), &pb.ForwardRequest{Requests: reqs})

	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, uint64(9), resp.CommitIndex)
	require.Len(t, observed, 1)
	require.Same(t, reqs[0], observed[0])
	require.Len(t, txn.reqs, 1)
	require.Same(t, reqs[0], txn.reqs[0])
	require.Equal(t, uint64(1), reqs[0].Ts)
}

func TestStampTxnTimestamps_RejectsMaxStartTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Ts:    ^uint64(0),
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
			},
		},
	}

	_, err := i.stampTxnTimestamps(context.Background(), reqs)
	require.ErrorIs(t, err, ErrTxnTimestampOverflow)
}

func TestFillForwardedTxnCommitTS_RejectsOverflow(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
			},
		},
	}

	_, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, ^uint64(0))
	require.ErrorIs(t, err, ErrTxnTimestampOverflow)
}

func TestFillForwardedTxnCommitTS_AssignsCommitTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	startTS := uint64(10)
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS)
	require.NoError(t, err)

	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, startTS+1, meta.CommitTS)
	require.Equal(t, meta.CommitTS, commitTS)
}

func TestFillForwardedTxnCommitTS_PreservesExistingCommitTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 42}),
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, 10)
	require.NoError(t, err)
	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, uint64(42), meta.CommitTS)
	require.Equal(t, meta.CommitTS, commitTS)
}

func TestFillForwardedTxnCommitTS_AssignsCommitTSForOnePhaseTxn(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	startTS := uint64(10)
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_NONE,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
				{
					Op:    pb.Op_PUT,
					Key:   []byte("k"),
					Value: []byte("v"),
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS)
	require.NoError(t, err)

	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, startTS+1, meta.CommitTS)
	require.Equal(t, meta.CommitTS, commitTS)
}

func TestFillForwardedTxnCommitTS_StampsCommitTSValueOffset(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	startTS := uint64(10)
	value := make([]byte, 16)
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_NONE,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
				{
					Op:                  pb.Op_PUT,
					Key:                 []byte("k"),
					Value:               value,
					CommitTsValueOffset: 4,
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS)
	require.NoError(t, err)
	require.Equal(t, startTS+1, commitTS)
	require.Equal(t, commitTS, binary.BigEndian.Uint64(value[4:12]))
	require.Zero(t, reqs[0].Mutations[1].CommitTsValueOffset)
}

func TestFillForwardedTxnCommitTS_PrepareAllowsAlreadyStampedOffsets(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	value := make([]byte, 16)
	mut := &pb.Mutation{
		Op:                  pb.Op_PUT,
		Key:                 []byte("k"),
		Value:               value,
		CommitTsValueOffset: 4,
	}
	require.NoError(t, kv.StampMutationCommitTS([]*pb.Mutation{mut}, 42))

	reqs := []*pb.Request{
		{
			IsTxn:     true,
			Phase:     pb.Phase_PREPARE,
			Ts:        10,
			Mutations: []*pb.Mutation{mut},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, 10)
	require.NoError(t, err)
	require.Zero(t, commitTS)
	require.Equal(t, uint64(42), binary.BigEndian.Uint64(value[4:12]))
	require.Zero(t, mut.CommitTsValueOffset)
}

func TestStampTxnTimestamps_UsesSingleTxnStartTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    0,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}
	commit := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    9,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(kv.TxnMetaPrefix),
				Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
			},
			{Op: pb.Op_PUT, Key: []byte("k")},
		},
	}
	reqs := []*pb.Request{prepare, commit}

	commitTS, err := i.stampTxnTimestamps(context.Background(), reqs)
	require.NoError(t, err)
	require.Equal(t, uint64(9), prepare.Ts)
	require.Equal(t, uint64(9), commit.Ts)

	meta, err := kv.DecodeTxnMeta(commit.Mutations[0].Value)
	require.NoError(t, err)
	require.Greater(t, meta.CommitTS, uint64(9))
	require.Equal(t, meta.CommitTS, commitTS)
}

// recordingWriteGate stands in for the sharded coordinator's route-floor check.
type recordingWriteGate struct {
	floor     uint64
	calls     int
	lastTS    uint64
	lastMuts  int
	rejectErr error
}

func (g *recordingWriteGate) EnsureMutationsWriteAllowed(muts []*pb.Mutation, commitTS uint64) error {
	g.calls++
	g.lastTS = commitTS
	g.lastMuts = len(muts)
	if commitTS != 0 && commitTS <= g.floor {
		return g.rejectErr
	}
	return nil
}

// A follower cannot stamp a raw write (its stamping path bails out when the
// group engine is not leader), so the leader assigns the timestamp here. Before
// the gate existed, that write went to Raft with no route-floor check at all.
func TestStampRawTimestamps_AppliesRouteFloorToForwardedWrites(t *testing.T) {
	t.Parallel()

	rejected := errors.New("route min_write_ts_exclusive rejects commit_ts")

	tests := []struct {
		name      string
		floor     uint64
		requestTS uint64
		wantErr   bool
	}{
		{
			name:      "unstamped write above the floor is admitted",
			floor:     0,
			requestTS: 0,
			wantErr:   false,
		},
		{
			// Any timestamp the leader can mint is at or below this floor.
			name:      "unstamped write at or below the floor is rejected",
			floor:     ^uint64(0) - 1,
			requestTS: 0,
			wantErr:   true,
		},
		{
			name:      "pre-stamped write below the floor is rejected too",
			floor:     100,
			requestTS: 42,
			wantErr:   true,
		},
		{
			name:      "pre-stamped write above the floor is admitted",
			floor:     5,
			requestTS: 42,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gate := &recordingWriteGate{floor: tt.floor, rejectErr: rejected}
			i := &Internal{clock: kv.NewHLC(), writeGate: gate}
			reqs := []*pb.Request{{
				IsTxn: false,
				Phase: pb.Phase_NONE,
				Ts:    tt.requestTS,
				Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
				},
			}}

			err := i.stampRawTimestamps(context.Background(), reqs)
			if tt.wantErr {
				require.ErrorIs(t, err, rejected)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, gate.calls)
			require.Equal(t, 1, gate.lastMuts)
			require.NotZero(t, gate.lastTS, "the gate must see the stamped timestamp, not zero")
			if tt.requestTS != 0 {
				require.Equal(t, tt.requestTS, gate.lastTS)
			}
		})
	}
}

// Deployments without a route table (the single-group coordinator) leave the
// gate unset; stamping must keep working there.
func TestStampRawTimestamps_WithoutWriteGate(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{{
		IsTxn:     false,
		Phase:     pb.Phase_NONE,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
	}}

	require.NoError(t, i.stampRawTimestamps(context.Background(), reqs))
	require.NotZero(t, reqs[0].Ts)
}

func TestStampTxnTimestamps_AppliesRouteFloorToForwardedCommitWrites(t *testing.T) {
	t.Parallel()

	rejected := errors.New("route min_write_ts_exclusive rejects commit_ts")
	gate := &recordingWriteGate{floor: 100, rejectErr: rejected}
	i := &Internal{writeGate: gate}
	reqs := []*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    10,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("z"), CommitTS: 101})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}}

	commitTS, err := i.stampTxnTimestamps(context.Background(), reqs)

	require.NoError(t, err)
	require.Equal(t, uint64(101), commitTS)
	require.Equal(t, 1, gate.calls)
	require.Equal(t, uint64(101), gate.lastTS)
	require.Equal(t, 1, gate.lastMuts, "txn metadata is not a user write and must not be gated")

	gate.floor = 101
	_, err = i.stampTxnTimestamps(context.Background(), reqs)
	require.ErrorIs(t, err, rejected)
}

func TestStampTxnTimestamps_DoesNotGatePrepareOrAbortCleanup(t *testing.T) {
	t.Parallel()

	rejected := errors.New("route min_write_ts_exclusive rejects commit_ts")
	gate := &recordingWriteGate{floor: 0, rejectErr: rejected}
	i := &Internal{writeGate: gate}
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_PREPARE,
			Ts:    10,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("z")})},
				{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
			},
		},
		{
			IsTxn: true,
			Phase: pb.Phase_ABORT,
			Ts:    10,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("z"), CommitTS: 11})},
				{Op: pb.Op_DEL, Key: []byte("z")},
			},
		},
	}

	commitTS, err := i.stampTxnTimestamps(context.Background(), reqs)

	require.NoError(t, err)
	require.Equal(t, uint64(11), commitTS)
	require.Zero(t, gate.calls)
}

type forwardObserverLeader struct{}

func (forwardObserverLeader) State() raftengine.State { return raftengine.StateLeader }
func (forwardObserverLeader) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "self", Address: "127.0.0.1:0"}
}
func (forwardObserverLeader) VerifyLeader(context.Context) error               { return nil }
func (forwardObserverLeader) LinearizableRead(context.Context) (uint64, error) { return 0, nil }

type forwardObserverTxn struct {
	reqs []*pb.Request
}

func (t *forwardObserverTxn) Commit(_ context.Context, reqs []*pb.Request) (*kv.TransactionResponse, error) {
	t.reqs = reqs
	return &kv.TransactionResponse{CommitIndex: 9}, nil
}

func (t *forwardObserverTxn) Abort(context.Context, []*pb.Request) (*kv.TransactionResponse, error) {
	return &kv.TransactionResponse{}, nil
}

func TestFillForwardedTxnCommitTS_FallsBackWhenRuntimeAllocatorLegacy(t *testing.T) {
	t.Parallel()

	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	i := &Internal{
		clock:       clock,
		tsAllocator: internalLegacyRuntimeAllocator{},
	}
	startTS := uint64(10)
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS)
	require.NoError(t, err)
	require.Greater(t, commitTS, startTS)
}

func TestStampRawTimestamps_FallsBackWhenRuntimeAllocatorLegacy(t *testing.T) {
	t.Parallel()

	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	i := &Internal{
		clock:       clock,
		tsAllocator: internalLegacyRuntimeAllocator{},
	}
	reqs := []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}}}}

	require.NoError(t, i.stampRawTimestamps(context.Background(), reqs))
	require.NotZero(t, reqs[0].Ts)
}

type internalLegacyRuntimeAllocator struct{}

func (internalLegacyRuntimeAllocator) Next(context.Context) (uint64, error) {
	return 0, errors.WithStack(kv.ErrTSOAllocatorRequired)
}

func TestInternalForward_RejectedOnDedicatedTSOGroup(t *testing.T) {
	t.Parallel()

	i := NewInternalWithEngine(nil, nil, nil, nil, WithKVForwardRejected())

	resp, err := i.Forward(context.Background(), &pb.ForwardRequest{
		Requests: []*pb.Request{{
			IsTxn:     false,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
		}},
	})

	require.ErrorIs(t, err, ErrKVForwardNotSupported)
	require.Nil(t, resp)
}

func TestInternalForward_RejectionPrecedesLeaderCheck(t *testing.T) {
	t.Parallel()

	i := NewInternalWithEngine(nil, nil, nil, nil, WithKVForwardRejected())

	_, err := i.Forward(context.Background(), &pb.ForwardRequest{})

	require.ErrorIs(t, err, ErrKVForwardNotSupported)
	require.NotErrorIs(t, err, ErrNotLeader)
}

type phaseDForwardAllocator struct {
	floor uint64
	// ceiling stands in for the allocation floor: anything beyond it has not
	// been issued by group 0 yet. Zero means unbounded.
	ceiling       uint64
	next          uint64
	validateCalls int
}

func (a *phaseDForwardAllocator) Next(context.Context) (uint64, error) {
	a.next++
	return a.floor + a.next, nil
}

func (a *phaseDForwardAllocator) NextAfter(_ context.Context, minTS uint64) (uint64, error) {
	a.next++
	ts := a.floor + a.next
	if ts <= minTS {
		ts = minTS + 1
	}
	return ts, nil
}

func (a *phaseDForwardAllocator) ValidateDurableTimestamp(_ context.Context, timestamp uint64) error {
	a.validateCalls++
	if timestamp == 0 || (a.ceiling != 0 && timestamp > a.ceiling) {
		return kv.ErrTSOTimestampInvalid
	}
	if timestamp <= a.floor {
		return errors.Join(kv.ErrTSOTimestampInvalid, kv.ErrTSOTimestampPrePhaseD)
	}
	return nil
}

func (a *phaseDForwardAllocator) PhaseDActive() bool   { return true }
func (a *phaseDForwardAllocator) PhaseDRequired() bool { return true }

func TestStampRawTimestamps_ValidatesForwardedTimestampUnderPhaseD(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	stale := []*pb.Request{{Ts: 50, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}}
	err := i.stampRawTimestamps(context.Background(), stale)
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)
	require.Equal(t, 1, alloc.validateCalls)

	// A timestamp the allocator vouches for still passes through unchanged.
	fresh := []*pb.Request{{Ts: 101, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}}
	require.NoError(t, i.stampRawTimestamps(context.Background(), fresh))
	require.Equal(t, uint64(101), fresh[0].Ts)

	// An unset timestamp is allocated, not validated.
	before := alloc.validateCalls
	unset := []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}}
	require.NoError(t, i.stampRawTimestamps(context.Background(), unset))
	require.NotZero(t, unset[0].Ts)
	require.Equal(t, before, alloc.validateCalls)
}

func TestFillForwardedTxnCommitTS_ValidatesForwardedCommitTSUnderPhaseD(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	stamped := func(commitTS uint64) []*pb.Request {
		return []*pb.Request{{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{{
				Op:    pb.Op_PUT,
				Key:   []byte(kv.TxnMetaPrefix),
				Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: commitTS}),
			}},
		}}
	}

	_, err := i.fillForwardedTxnCommitTS(context.Background(), stamped(50), 150)
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), stamped(101), 150)
	require.NoError(t, err)
	require.Equal(t, uint64(101), commitTS)
}

func TestForwardedTimestamps_UnvalidatedWithoutPhaseD(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{{Ts: 7, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}}
	require.NoError(t, i.stampRawTimestamps(context.Background(), reqs))
	require.Equal(t, uint64(7), reqs[0].Ts)
}

func TestFillForwardedTxnCommitTS_AllowsPrePhaseDResolutionReplay(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	resolution := func(commitTS uint64) []*pb.Request {
		return []*pb.Request{{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{{
				Op:    pb.Op_PUT,
				Key:   []byte(kv.TxnMetaPrefix),
				Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: commitTS}),
			}},
		}}
	}

	// Both halves predate Phase D: the whole transaction belongs to the legacy
	// era, so group 0 can never re-issue either value.
	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), resolution(60), 50)
	require.NoError(t, err)
	require.Equal(t, uint64(60), commitTS)

	// A post-Phase-D transaction may not claim a pre-Phase-D commit timestamp.
	_, err = i.fillForwardedTxnCommitTS(context.Background(), resolution(60), 150)
	require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)
}

func TestFillForwardedTxnCommitTS_StillRejectsUnallocatedCommitTS(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100, ceiling: 200}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	reqs := []*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte(kv.TxnMetaPrefix),
			Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 5_000}),
		}},
	}}

	_, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, 50)
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampInvalid)
	require.NotErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)
}

func TestFillForwardedTxnCommitTS_OnePhaseGetsNoLegacyExemption(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	onePhase := []*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte(kv.TxnMetaPrefix),
			Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 60}),
		}},
	}}
	_, err := i.fillForwardedTxnCommitTS(context.Background(), onePhase, 50)
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)

	// The same pair on an ABORT resolution is still admitted.
	abort := []*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_ABORT,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte(kv.TxnMetaPrefix),
			Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 60}),
		}},
	}}
	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), abort, 50)
	require.NoError(t, err)
	require.Equal(t, uint64(60), commitTS)
}

func TestStampTxnTimestamps_ValidatesForwardedStartTS(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100, ceiling: 200}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	prepare := func(startTS uint64) []*pb.Request {
		return []*pb.Request{{
			IsTxn:     true,
			Phase:     pb.Phase_PREPARE,
			Ts:        startTS,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
		}}
	}

	// Beyond the allocation floor: group 0 has not issued this.
	_, err := i.stampTxnTimestamps(context.Background(), prepare(5_000))
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampInvalid)
	require.NotErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)

	// An allocated one passes through unchanged. stampTxnTimestamps returns the
	// commit timestamp, which a PREPARE has none of, so the start timestamp is
	// read back off the request it was stamped onto.
	reqs := prepare(150)
	_, err = i.stampTxnTimestamps(context.Background(), reqs)
	require.NoError(t, err)
	require.Equal(t, uint64(150), reqs[0].Ts)

	// A transaction that began before Phase D may still prepare its intents.
	legacy := prepare(50)
	_, err = i.stampTxnTimestamps(context.Background(), legacy)
	require.NoError(t, err)
	require.Equal(t, uint64(50), legacy[0].Ts)
}

func TestFillForwardedTxnCommitTS_MixedPhaseBatchGetsNoLegacyExemption(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	meta := func(phase pb.Phase, commitTS uint64) *pb.Request {
		return &pb.Request{
			IsTxn: true,
			Phase: phase,
			Mutations: []*pb.Mutation{{
				Op:    pb.Op_PUT,
				Key:   []byte(kv.TxnMetaPrefix),
				Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: commitTS}),
			}},
		}
	}

	// One-phase first, resolution second: the batch must not inherit the
	// resolution's exemption.
	mixed := []*pb.Request{meta(pb.Phase_NONE, 60), meta(pb.Phase_COMMIT, 60)}
	_, err := i.fillForwardedTxnCommitTS(context.Background(), mixed, 50)
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)

	// Resolutions only still pass.
	resolutions := []*pb.Request{meta(pb.Phase_ABORT, 60), meta(pb.Phase_COMMIT, 60)}
	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), resolutions, 50)
	require.NoError(t, err)
	require.Equal(t, uint64(60), commitTS)
}

func TestStampTimestamps_RejectsInconsistentForwardEnvelope(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100, ceiling: 200}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	inner := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Ts:    150,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte(kv.TxnMetaPrefix),
			Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 5_000}),
		}},
	}

	_, err := i.stampTimestamps(context.Background(), &pb.ForwardRequest{
		IsTxn:    false,
		Requests: []*pb.Request{inner},
	})
	require.ErrorIs(t, err, kv.ErrInvalidRequest)

	// The consistent envelope reaches the transactional path, where the meta's
	// unissued commit timestamp is caught.
	_, err = i.stampTimestamps(context.Background(), &pb.ForwardRequest{
		IsTxn:    true,
		Requests: []*pb.Request{inner},
	})
	require.ErrorIs(t, err, kv.ErrTSOTimestampInvalid)

	// A genuinely raw envelope is unaffected.
	_, err = i.stampTimestamps(context.Background(), &pb.ForwardRequest{
		IsTxn: false,
		Requests: []*pb.Request{{
			Ts:        150,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
		}},
	})
	require.NoError(t, err)
}

func TestStampTimestamps_RejectsRawRequestInsideTxnEnvelope(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100, ceiling: 200}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	resolution := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    50,
		Mutations: []*pb.Mutation{{
			Op:    pb.Op_PUT,
			Key:   []byte(kv.TxnMetaPrefix),
			Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 60}),
		}},
	}
	raw := &pb.Request{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}}}

	_, err := i.stampTimestamps(context.Background(), &pb.ForwardRequest{
		IsTxn:    true,
		Requests: []*pb.Request{raw, resolution},
	})
	require.ErrorIs(t, err, kv.ErrInvalidRequest)

	// The all-transactional batch is unaffected.
	_, err = i.stampTimestamps(context.Background(), &pb.ForwardRequest{
		IsTxn:    true,
		Requests: []*pb.Request{resolution},
	})
	require.NoError(t, err)
}

func TestFillForwardedTxnCommitTS_ZeroValuedOnePhaseMetaBlocksExemption(t *testing.T) {
	t.Parallel()

	alloc := &phaseDForwardAllocator{floor: 100}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalTimestampAllocator(alloc))

	meta := func(phase pb.Phase, commitTS uint64) *pb.Request {
		return &pb.Request{
			IsTxn: true,
			Phase: phase,
			Mutations: []*pb.Mutation{{
				Op:    pb.Op_PUT,
				Key:   []byte(kv.TxnMetaPrefix),
				Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: commitTS}),
			}},
		}
	}

	// The one-phase meta carries no timestamp of its own; it would be filled
	// with the COMMIT's exempt one.
	mixed := []*pb.Request{meta(pb.Phase_NONE, 0), meta(pb.Phase_COMMIT, 60)}
	_, err := i.fillForwardedTxnCommitTS(context.Background(), mixed, 50)
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)
}

// The export defaults only fill in a bound the request left unset, so without a
// ceiling one ExportVersions call can scan and decode an arbitrary portion of
// the source store before producing its next streamed response. For a sparse
// family or route filter that accepts few rows, math.MaxUint64 removes the only
// work bound there is.
func TestExportRangeVersionsOptionsClampOversizedBounds(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	opts := i.exportRangeVersionsOptions(&pb.ExportRangeVersionsRequest{
		ChunkBytes:      ^uint32(0),
		MaxScannedBytes: ^uint64(0),
	})

	require.Equal(t, uint64(maxMigrationExportChunkBytes), opts.MaxBytes)
	require.Equal(t, uint64(maxMigrationExportScanBytes), opts.MaxScannedBytes)
}

// Unset bounds still take the defaults, and a request under the ceiling passes
// through so a caller can still ask for smaller chunks.
func TestExportRangeVersionsOptionsKeepDefaultsAndSmallerRequests(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	defaults := i.exportRangeVersionsOptions(&pb.ExportRangeVersionsRequest{})
	require.Equal(t, uint64(defaultMigrationExportChunkBytes), defaults.MaxBytes)
	require.Equal(t,
		uint64(defaultMigrationExportChunkBytes*defaultMigrationExportScanFactor),
		defaults.MaxScannedBytes)

	smaller := i.exportRangeVersionsOptions(&pb.ExportRangeVersionsRequest{
		ChunkBytes:      4096,
		MaxScannedBytes: 8192,
	})
	require.Equal(t, uint64(4096), smaller.MaxBytes)
	require.Equal(t, uint64(8192), smaller.MaxScannedBytes)
}
