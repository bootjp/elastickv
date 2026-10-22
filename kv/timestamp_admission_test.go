package kv

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func admissionTestStore(t *testing.T, phase byte, floor uint64) store.MVCCStore {
	t.Helper()
	st := store.NewMVCCStore()
	if phase != 0 {
		value := binary.BigEndian.AppendUint64([]byte{phase}, floor)
		require.NoError(t, st.PutAt(context.Background(), prepareEvidenceGateKey, value, 1, 0))
	}
	return st
}

func admissionTestRequests() []*pb.Request {
	return []*pb.Request{
		{Ts: 110, TimestampAdmissionFloor: 999},
		{IsTxn: true, Phase: pb.Phase_NONE, Ts: 5, TimestampAdmissionFloor: 999,
			Mutations: []*pb.Mutation{{Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{CommitTS: 120})}}},
		{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 6, TimestampAdmissionFloor: 999,
			Mutations: []*pb.Mutation{{Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrepareCommitTS: 130})}}},
	}
}

func TestStampTimestampAdmissionClearsUntrustedEpochBeforeRelease(t *testing.T) {
	for _, st := range []store.MVCCStore{nil, admissionTestStore(t, 0, 0)} {
		requests := admissionTestRequests()
		require.NoError(t, StampTimestampAdmission(context.Background(), nil, st, requests))
		for _, request := range requests {
			require.Zero(t, request.GetTimestampAdmissionFloor())
		}
	}
}

func TestStampTimestampAdmissionValidatesPersistenceNotPrepareStart(t *testing.T) {
	st := admissionTestStore(t, prepareCreationReleased, 100)
	for i, timestamp := range []uint64{110, 120, 130} {
		request := admissionTestRequests()[i]
		original := proto.CloneOf(request)
		allocator := &phaseDTestAllocator{}
		require.False(t, allocator.PhaseDRequired())
		require.NoError(t, StampTimestampAdmission(context.Background(), allocator, st, []*pb.Request{request}))
		require.Equal(t, uint64(1), allocator.validateCalls.Load())
		require.Equal(t, timestamp, allocator.validated.Load())
		require.Zero(t, allocator.nextCalls.Load(), "admission must never mint or retimestamp")
		require.Equal(t, uint64(100), request.GetTimestampAdmissionFloor())
		original.TimestampAdmissionFloor = 100
		require.True(t, proto.Equal(original, request), "start timestamps, metadata and values must remain unchanged")
	}
}

func TestStampTimestampAdmissionResolutionRemainsAvailableWhileSealed(t *testing.T) {
	st := admissionTestStore(t, prepareCreationSealed, 0)
	resolution := []*pb.Request{nil,
		{IsTxn: true, Phase: pb.Phase_COMMIT, TimestampAdmissionFloor: 777},
		{IsTxn: true, Phase: pb.Phase_ABORT, TimestampAdmissionFloor: 888},
	}
	require.NoError(t, StampTimestampAdmission(context.Background(), nil, st, resolution))
	require.Equal(t, uint64(777), resolution[1].GetTimestampAdmissionFloor())
	require.Equal(t, uint64(888), resolution[2].GetTimestampAdmissionFloor())
	requests := append([]*pb.Request{}, resolution...)
	requests = append(requests, admissionTestRequests()...)
	require.ErrorIs(t, StampTimestampAdmission(context.Background(), nil, st, requests), ErrPrepareCreationSealed)
	for _, request := range requests[3:] {
		require.Zero(t, request.GetTimestampAdmissionFloor())
	}
}

func TestStampTimestampAdmissionRejectsMissingValidatorAndUnissuedValues(t *testing.T) {
	st := admissionTestStore(t, prepareCreationReleased, 100)
	requests := admissionTestRequests()
	require.ErrorIs(t, StampTimestampAdmission(context.Background(), &fakeTSOAllocator{}, st, requests), ErrTSOProtocolUnsupported)
	allocator := &phaseDTestAllocator{validateErr: ErrTSOTimestampInvalid}
	require.ErrorIs(t, StampTimestampAdmission(context.Background(), allocator, st, requests), ErrTSOTimestampInvalid)
	for _, request := range requests {
		require.Zero(t, request.GetTimestampAdmissionFloor())
	}
	allocator.validateErr = nil
	requests = append(admissionTestRequests(), &pb.Request{Ts: 100, TimestampAdmissionFloor: 999})
	require.ErrorIs(t, StampTimestampAdmission(context.Background(), allocator, st, requests), ErrTSOTimestampInvalid)
	for _, request := range requests {
		require.Zero(t, request.GetTimestampAdmissionFloor(), "a partial validation must not admit a request prefix")
	}
}

func TestTimestampAdmissionOwnedRequestCannotBeRetaggedByRetry(t *testing.T) {
	st := admissionTestStore(t, 0, 0)
	allocator := &phaseDTestAllocator{}
	admission := &transactionTimestampAdmission{store: st, allocator: func() TimestampAllocator { return allocator }}
	request := &pb.Request{Ts: 110, TimestampAdmissionFloor: 999,
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("old")}}}
	owned, err := admission.prepare(context.Background(), []*pb.Request{request})
	require.NoError(t, err)
	value := binary.BigEndian.AppendUint64([]byte{prepareCreationReleased}, 100)
	require.NoError(t, st.PutAt(context.Background(), prepareEvidenceGateKey, value, 2, 0))
	require.NoError(t, StampTimestampAdmission(context.Background(), allocator, st, []*pb.Request{request}))
	require.Equal(t, uint64(100), request.GetTimestampAdmissionFloor())
	require.Zero(t, owned[0].GetTimestampAdmissionFloor(), "a queued legacy request must retain its old admission epoch")
	request.Ts = 150
	request.Mutations[0].Value[0] = 'n'
	require.Equal(t, uint64(110), owned[0].GetTs())
	require.Equal(t, []byte("old"), owned[0].Mutations[0].Value, "queued values must not be changed by a later retry")
}

func TestStampTimestampAdmissionDeduplicatesValidationWithinCommit(t *testing.T) {
	st := admissionTestStore(t, prepareCreationReleased, 100)
	allocator := &phaseDTestAllocator{}
	requests := []*pb.Request{{Ts: 110}, {Ts: 110}}
	require.NoError(t, StampTimestampAdmission(context.Background(), allocator, st, requests))
	require.Equal(t, uint64(1), allocator.validateCalls.Load())
	for _, request := range requests {
		require.Equal(t, uint64(100), request.GetTimestampAdmissionFloor())
	}
}

func TestTimestampAdmissionTransactionManagerAndLocalProxyAreWired(t *testing.T) {
	for _, proxy := range []bool{false, true} {
		t.Run(map[bool]string{false: "manager", true: "sharded_proxy"}[proxy], func(t *testing.T) {
			st := admissionTestStore(t, prepareCreationReleased, 100)
			allocator := &phaseDTestAllocator{}
			engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 1}
			var tx Transactional
			if proxy {
				group := &ShardGroup{Engine: engine, Store: st}
				group.Txn = NewLeaderProxyForShardGroup(group)
				groups := map[uint64]*ShardGroup{0: {Engine: engine, TSOState: NewTSOStateMachine(NewHLC())}, 1: group}
				coord := NewShardedCoordinator(distribution.NewEngineWithDefaultRoute(), groups, 1, NewHLC(), st)
				coord.WithTSOAllocator(allocator)
				t.Cleanup(func() { require.NoError(t, coord.Close()) })
				tx = group.Txn
			} else {
				tm := NewTransactionWithProposer(engine, WithTimestampAdmission(st, func() TimestampAllocator { return allocator }))
				t.Cleanup(tm.Close)
				tx = tm
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			request := &pb.Request{Ts: 110, TimestampAdmissionFloor: 999}
			_, err := tx.Commit(ctx, []*pb.Request{request})
			require.NoError(t, err)
			payloads := engine.proposedPayloads()
			require.Len(t, payloads, 1)
			requests, err := decodeRaftRequests(payloads[0])
			require.NoError(t, err)
			require.Equal(t, uint64(100), requests[0].GetTimestampAdmissionFloor())
			require.Equal(t, uint64(110), requests[0].GetTs())
			require.Equal(t, uint64(1), allocator.validateCalls.Load())
			require.Equal(t, uint64(999), request.GetTimestampAdmissionFloor(), "queued requests must own their protobuf")
		})
	}
}

func TestTimestampAdmissionAutomaticWiringRequiresDedicatedState(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		timestampGroup *ShardGroup
		wantAdmission  bool
	}{
		{name: "no_group_zero"},
		{name: "group_zero_without_tso", timestampGroup: &ShardGroup{Engine: &recordingTSOEngine{}}},
		{name: "dedicated_tso", timestampGroup: &ShardGroup{Engine: &recordingTSOEngine{}, TSOState: NewTSOStateMachine(NewHLC())}, wantAdmission: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 1}
			group := &ShardGroup{Engine: engine, Store: admissionTestStore(t, 0, 0)}
			proxy := NewLeaderProxyForShardGroup(group)
			group.Txn = proxy
			groups := map[uint64]*ShardGroup{1: group}
			if scenario.timestampGroup != nil {
				groups[0] = scenario.timestampGroup
			}
			coord := NewShardedCoordinator(distribution.NewEngineWithDefaultRoute(), groups, 1, NewHLC(), group.Store)
			t.Cleanup(func() { require.NoError(t, coord.Close()) })
			require.Equal(t, scenario.wantAdmission, proxy.tm.timestampAdmission.Load() != nil)
		})
	}
}

func TestTimestampAdmissionExplicitOptionSurvivesLegacyCoordinator(t *testing.T) {
	engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 1}
	st := admissionTestStore(t, prepareCreationReleased, 100)
	allocator := &phaseDTestAllocator{}
	group := &ShardGroup{Engine: engine, Store: st}
	proxy := NewLeaderProxyForShardGroup(group, WithTimestampAdmission(st, func() TimestampAllocator { return allocator }))
	group.Txn = proxy
	configured := proxy.tm.timestampAdmission.Load()
	coord := NewShardedCoordinator(distribution.NewEngineWithDefaultRoute(), map[uint64]*ShardGroup{1: group}, 1, NewHLC(), st)
	t.Cleanup(func() { require.NoError(t, coord.Close()) })
	require.Same(t, configured, proxy.tm.timestampAdmission.Load())
	_, err := group.Txn.Commit(context.Background(), []*pb.Request{{Ts: 110}})
	require.NoError(t, err)
	require.Equal(t, uint64(1), allocator.validateCalls.Load())
}

func TestTimestampAdmissionSingleCoordinatorUsesCurrentAllocator(t *testing.T) {
	st := admissionTestStore(t, prepareCreationReleased, 100)
	engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 1}
	tm := NewTransactionWithProposer(engine)
	t.Cleanup(tm.Close)
	allocator := &phaseDTestAllocator{next: 140}
	coord := NewCoordinatorWithEngine(tm, engine, WithTimestampAdmissionStore(st), WithTSOAllocator(allocator))
	t.Cleanup(func() { require.NoError(t, coord.Close()) })
	_, err := coord.dispatchRaw(context.Background(), []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}}, keyviz.LabelLegacy)
	require.NoError(t, err)
	requests, err := decodeRaftRequests(engine.proposedPayloads()[0])
	require.NoError(t, err)
	require.Equal(t, uint64(100), requests[0].GetTimestampAdmissionFloor())
	require.Equal(t, uint64(140), allocator.validated.Load())
}

type admissionDeadlineAllocator struct {
	phaseDTestAllocator
	deadlines []time.Time
	validated func()
}

func (a *admissionDeadlineAllocator) ValidateDurableTimestamp(ctx context.Context, _ uint64) error {
	deadline, _ := ctx.Deadline()
	a.deadlines = append(a.deadlines, deadline)
	if a.validated != nil {
		a.validated()
	}
	return nil
}

func TestStampTimestampAdmissionSharesOneBudgetAndHonorsEarlierDeadline(t *testing.T) {
	st := admissionTestStore(t, prepareCreationReleased, 100)
	for _, timeout := range []time.Duration{time.Minute, time.Second} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		allocator := &admissionDeadlineAllocator{}
		started := time.Now()
		err := StampTimestampAdmission(ctx, allocator, st, []*pb.Request{{Ts: 110}, {Ts: 110}, {Ts: 120}})
		cancel()
		require.NoError(t, err)
		require.Len(t, allocator.deadlines, 2)
		require.False(t, allocator.deadlines[0].IsZero())
		require.Equal(t, allocator.deadlines[0], allocator.deadlines[1])
		require.WithinDuration(t, started.Add(min(timeout, timestampAdmissionValidationBudget)), allocator.deadlines[0], time.Second/10)
	}
}

func TestStampTimestampAdmissionStopsFurtherValidationAfterBudgetCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allocator := &admissionDeadlineAllocator{validated: cancel}
	requests := []*pb.Request{{Ts: 110, TimestampAdmissionFloor: 999}, {Ts: 120, TimestampAdmissionFloor: 999}}
	err := StampTimestampAdmission(ctx, allocator, admissionTestStore(t, prepareCreationReleased, 100), requests)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, allocator.deadlines, 1)
	for _, request := range requests {
		require.Zero(t, request.GetTimestampAdmissionFloor())
	}
}
