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
	floor         uint64
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
	if timestamp == 0 {
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
// meta: it is the timestamp every mutation in the batch is persisted under.
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

	_, err := i.fillForwardedTxnCommitTS(context.Background(), stamped(50), 10)
	require.Error(t, err)
	require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), stamped(101), 10)
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
