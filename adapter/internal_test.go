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

type internalLegacyRuntimeAllocator struct{}

func (internalLegacyRuntimeAllocator) Next(context.Context) (uint64, error) {
	return 0, errors.WithStack(kv.ErrTSOAllocatorRequired)
}

// The dedicated TSO group runs TSOStateMachine, whose Apply halts on any tag it
// does not recognise. TransactionManager.Commit encodes KV requests with the
// 0x00 / 0x01 tags, so one forwarded PUT reaching group 0's Internal server
// would commit an entry that permanently stops group-0 apply and with it all
// centralized timestamp issuance. Forward must refuse before proposing.
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

// The guard must fire before the leader check so it cannot be masked by
// ErrNotLeader, and so it is refused on every replica rather than only where a
// leader would have proposed.
func TestInternalForward_RejectionPrecedesLeaderCheck(t *testing.T) {
	t.Parallel()

	i := NewInternalWithEngine(nil, nil, nil, nil, WithKVForwardRejected())

	_, err := i.Forward(context.Background(), &pb.ForwardRequest{})

	require.ErrorIs(t, err, ErrKVForwardNotSupported)
	require.NotErrorIs(t, err, ErrNotLeader)
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

// phaseDForwardAllocator is the receiver-side view of the dedicated TSO: it
// allocates above a floor and rejects anything at or below it as pre-Phase-D.
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

// Internal.Forward preserves a raw Request.Ts somebody else stamped rather than
// allocating one, and this receiver never reaches the coordinator's own
// validation. Without a check here a forwarding peer or a direct caller could
// persist a value group 0 has not allocated yet, and the TSO would later issue
// the same timestamp.
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

// The same for a commit timestamp that arrived already set in the transaction
// meta: it is the timestamp every mutation in the batch is persisted under. The
// start timestamp here is post-Phase-D, so this is a current transaction and not
// the legacy-resolution replay carved out below.
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

// Without Phase D in force the receiver must keep accepting pre-stamped
// timestamps exactly as before.
func TestForwardedTimestamps_UnvalidatedWithoutPhaseD(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{{Ts: 7, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}}
	require.NoError(t, i.stampRawTimestamps(context.Background(), reqs))
	require.Equal(t, uint64(7), reqs[0].Ts)
}

// A cross-shard transaction that began before the Phase-D marker can still have
// unresolved intents when the marker applies. Resolving them replays the commit
// timestamp the primary already recorded (LockResolver.resolveExpiredLock ->
// applyTxnResolution), and on a follower that replay travels through
// Internal.Forward. Rejecting it would leave the transaction partially resolved
// with its secondary keys locked, and the rollout does not require draining
// transactions before activating Phase D.
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

// The carve-out is only for timestamps below the Phase-D floor. A commit
// timestamp beyond the allocation floor is the value this check exists to
// reject, and a pre-Phase-D start timestamp must not launder it.
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

// The legacy carve-out is for replaying a commit timestamp the primary already
// recorded, which only a COMMIT or ABORT resolution does. forwardedTxnMetaMutation
// also accepts Phase_NONE, and a one-phase transaction chose its own commit
// timestamp with no recorded intent behind it -- handleOnePhaseTxnRequest would
// persist it straight away -- so it gets no exemption.
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

// A forwarded PREPARE carries no transaction meta, so the commit-timestamp check
// never sees it -- but handlePrepareRequest persists the intent at the
// caller-supplied start timestamp. A caller could otherwise submit
// AllocationFloor()+1 and let the TSO issue the same value later.
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

// commitSequential applies a batch in order, so a Phase_NONE request placed
// ahead of a COMMIT carrying the same pre-Phase-D commit timestamp would persist
// a one-phase write under the resolution's exemption. Every meta that carries
// the timestamp has to be a resolution, not just the last one the loop saw.
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

// Internal.Forward stamps from the envelope's IsTxn while
// TransactionManager.Commit applies from the inner requests' own IsTxn. A batch
// that says raw outside and carries a transactional request inside took the raw
// stamping path, which only looks at Request.Ts, and then went down the
// transactional apply path, where the FSM takes the persistence timestamp from a
// transaction meta nothing validated.
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
