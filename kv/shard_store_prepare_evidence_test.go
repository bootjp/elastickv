package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type prepareSafetyEngine struct {
	*recordingTSOEngine
	members []raftengine.Server
	version atomic.Uint64
	pending atomic.Bool
}

func (e *prepareSafetyEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{Servers: e.members}, nil
}

func (e *prepareSafetyEngine) Status() raftengine.Status {
	s := e.recordingTSOEngine.Status()
	s.Leader = e.Leader()
	s.ConfigurationIndex, s.PendingConfChange = e.version.Load(), e.pending.Load()
	return s
}

func newPrepareSafetyGate(t *testing.T) (*ShardStore, *prepareSafetyEngine, *kvFSM) {
	t.Helper()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	engine := &prepareSafetyEngine{recordingTSOEngine: &recordingTSOEngine{
		state: raftengine.StateLeader, leader: raftengine.LeaderInfo{ID: "self"},
		apply: func(data []byte) error {
			result := fsm.Apply(data)
			if result == nil {
				return nil
			}
			err, isError := result.(error)
			if !isError {
				return fmt.Errorf("unexpected fence response %T", result)
			}
			return err
		},
	}, members: []raftengine.Server{{ID: "self", Address: "local", Suffrage: "voter"}}}
	state := NewTSOStateMachine(NewHLC())
	require.Nil(t, state.Apply(marshalTSOCutover()))
	shards := NewShardStore(nil, map[uint64]*ShardGroup{
		0: {Engine: engine, TSOState: state}, 1: {Engine: engine, Store: st},
	})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })
	return shards, engine, fsm
}

type prepareSafetyPeer struct {
	pb.UnimplementedInternalServer
	probe func(context.Context, *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error)
}

func (s *prepareSafetyPeer) GetPrepareEvidenceState(ctx context.Context, req *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
	return s.probe(ctx, req)
}

func startPrepareSafetyPeer(t *testing.T, peer pb.InternalServer) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	pb.RegisterInternalServer(srv, peer)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); _ = lis.Close() })
	return lis.Addr().String()
}

func TestPrepareEvidenceSafetyRejectsEveryUnsupportedMemberBeforeFence(t *testing.T) {
	for _, groupID := range []uint64{0, 1} {
		for _, role := range []string{"voter", "learner"} {
			t.Run(fmt.Sprintf("group-%d-%s", groupID, role), func(t *testing.T) {
				shards, engine, _ := newPrepareSafetyGate(t)
				old := startPrepareSafetyPeer(t, &pb.UnimplementedInternalServer{})
				other := &prepareSafetyEngine{recordingTSOEngine: &recordingTSOEngine{state: raftengine.StateFollower},
					members: []raftengine.Server{{ID: "old", Address: old, Suffrage: role}}}
				shards.groups[groupID].Engine = other
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.ErrorIs(t, shards.CheckPhaseDProtocol(ctx), ErrTSOProtocolUnsupported)
				require.Empty(t, engine.proposedPayloads(), "unsupported voter or learner must block even the floor-zero marker")
				enabled, err := PrepareEvidenceEnabled(ctx, shards.groups[1].Store)
				require.NoError(t, err)
				require.False(t, enabled)
			})
		}
	}
}

func TestPrepareEvidenceSafetyNegotiatesTokenAndMembershipBeforeMarker(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("configuration-changed-%t", changed), func(t *testing.T) {
			shards, engine, _ := newPrepareSafetyGate(t)
			var probes atomic.Int32
			peer := &prepareSafetyPeer{probe: func(ctx context.Context, req *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
				md, _ := metadata.FromIncomingContext(ctx)
				if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer safety-token" {
					return nil, status.Error(codes.Unauthenticated, "missing peer token")
				}
				if !req.GetCapabilityOnly() {
					return nil, status.Error(codes.InvalidArgument, "expected capability-only request")
				}
				probes.Add(1)
				if changed {
					engine.version.Add(1)
				}
				return &pb.PrepareEvidenceStateResponse{Supported: true, CutoverApplied: true, DedicatedTsoConfigured: true}, nil
			}}
			address := startPrepareSafetyPeer(t, peer)
			// One shared engine backs groups 0 and 1, so both groups must probe.
			engine.members = append(engine.members, raftengine.Server{ID: "learner", Address: address, Suffrage: "learner"})
			for _, group := range shards.groups {
				group.SetLeaderReadToken("safety-token")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := shards.CheckPhaseDProtocol(ctx)
			if changed {
				require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
				require.Empty(t, engine.proposedPayloads())
			} else {
				require.NoError(t, err)
				require.Equal(t, int32(2), probes.Load())
				require.Equal(t, [][]byte{EncodePrepareEvidenceFence(0)}, engine.proposedPayloads())
			}
		})
	}
}

func TestPrepareEvidenceSafetyFloorZeroDrainsNormalAndStagedLocks(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprintf("staged-%t", staged), func(t *testing.T) {
			shards, _, fsm := newPrepareSafetyGate(t)
			ctx := context.Background()
			// Put the unproven secondary after a full page of proven locks.
			for i := 0; i <= txnLockBatchScanPageSize; i++ {
				key := txnLockKey([]byte(fmt.Sprintf("key-%06d", i)))
				if staged {
					key = distribution.MigrationStagedDataKey(7, key)
				}
				proof := uint64(501)
				if i == txnLockBatchScanPageSize {
					proof = 0
				}
				require.NoError(t, fsm.store.PutAt(ctx, key, encodeTxnLock(txnLock{StartTS: 500, CommitTS: proof, PrimaryKey: []byte("elsewhere")}), 500, 0))
			}
			require.ErrorIs(t, shards.CheckPhaseDProtocol(ctx), ErrTSOPhaseDActivationPending)
			enabled, unproven, floor, err := InspectPrepareEvidence(ctx, fsm.store, 0)
			require.NoError(t, err)
			require.True(t, enabled)
			require.True(t, unproven, "floor zero must scan all start timestamps and all pages")
			require.Zero(t, floor)
			key := txnLockKey([]byte(fmt.Sprintf("key-%06d", txnLockBatchScanPageSize)))
			if staged {
				key = distribution.MigrationStagedDataKey(7, key)
			}
			require.NoError(t, fsm.store.DeleteAt(ctx, key, 600))
			require.NoError(t, shards.CheckPhaseDProtocol(ctx))
			require.NoError(t, shards.PreparePhaseD(ctx, 700))
		})
	}
}

func TestPrepareEvidenceSafetyMarkerRejectsNewLegacyButAllowsRetryAndResolution(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			_, _, fsm := newPrepareSafetyGate(t)
			legacy := preparedEvidenceRequest(pb.Phase_PREPARE, 0)
			require.NoError(t, applyFSMRequest(t, fsm, legacy))
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
			require.NoError(t, applyFSMRequest(t, fsm, legacy), "same durable legacy lock may retry")
			fresh := preparedEvidenceRequest(pb.Phase_PREPARE, 0)
			fresh.Ts = 1000
			fresh.Mutations[1].Key = []byte("fresh-primary")
			fresh.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("fresh-primary"), LockTTLms: defaultTxnLockTTLms})
			require.Error(t, applyFSMRequest(t, fsm, fresh), "marker must reject a new proof-zero transaction even above the old floor")
			require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(phase, 20)))
			_, unproven, _, err := InspectPrepareEvidence(context.Background(), fsm.store, 0)
			require.NoError(t, err)
			require.False(t, unproven)
		})
	}
}

func TestPrepareEvidenceSafetyMarkerBlocksLateLegacyImportAtAnyStart(t *testing.T) {
	for _, floor := range []uint64{0, 100} {
		for _, start := range []uint64{10, 1000} {
			t.Run(fmt.Sprintf("floor-%d-start-%d", floor, start), func(t *testing.T) {
				_, _, fsm := newPrepareSafetyGate(t)
				require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(floor)))
				key := txnLockKey([]byte("late-secondary"))
				command, err := MarshalMigrationImportCommand(&pb.ImportRangeVersionsRequest{
					JobId: 7, BracketId: 1, BatchSeq: 1, Cursor: []byte("end"),
					Versions: []*pb.MVCCVersion{{Key: key, CommitTs: start, Value: encodeTxnLock(txnLock{StartTS: start, PrimaryKey: []byte("primary")})}},
				})
				require.NoError(t, err)
				result := fsm.Apply(command)
				applyErr, ok := result.(error)
				require.True(t, ok, "late import unexpectedly accepted: %v", result)
				require.ErrorIs(t, applyErr, ErrBackupTimestampFenced)
				_, err = fsm.store.GetAt(context.Background(), distribution.MigrationStagedDataKey(7, key), ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			})
		}
	}
}

func TestPrepareEvidenceSafetyMarkerIsExcludedFromMigration(t *testing.T) {
	brackets, err := distribution.PlanExportBrackets([]byte(txnBackupPrefix), []byte("!txn|meta}"))
	require.NoError(t, err)
	for _, key := range [][]byte{backupTimestampFloorKey, prepareEvidenceEnabledKey, prepareEvidenceGateKey, prepareEvidenceTimestampFloorKey,
		replicatedPinStateKey, []byte(store.ReplicatedControlPrefix + "future-state")} {
		require.True(t, distribution.IsMigrationKnownInternalKey(key),
			"the source must not export a group-local protocol marker as user data")
		for _, bracket := range brackets {
			require.False(t, bracket.ContainsRawKey(key), "bracket %d exports the protocol marker %q", bracket.BracketID, key)
		}
	}
}

func TestReplicatedControlMigrationPreservesNormalizedUserKey(t *testing.T) {
	raw := replicatedPinStateKey
	normalized := ExtractTxnUserKey(raw)
	require.Equal(t, []byte("ttl-inline-control|backup-pins"), normalized)
	end := append(bytes.Clone(normalized), 0)
	brackets, err := distribution.PlanExportBrackets(normalized, end)
	require.NoError(t, err)
	userBracketSeen := false
	for _, bracket := range brackets {
		require.False(t, bracket.ContainsRawKey(raw), "raw journal must not enter its normalized route's export")
		if bracket.Family == distribution.MigrationFamilyUser {
			userBracketSeen = true
			require.True(t, bracket.ContainsRawKey(normalized), "a literal user key with the normalized name remains migratable")
		}
	}
	require.True(t, userBracketSeen)
	require.False(t, isTxnPeerLocalControlKey(normalized))
}

func TestPrepareEvidenceSafetyMigrationCannotReplaceLocalProtocolState(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, tombstone := range []bool{false, true} {
			for _, key := range [][]byte{backupTimestampFloorKey, prepareEvidenceEnabledKey, prepareEvidenceGateKey, prepareEvidenceTimestampFloorKey,
				replicatedPinStateKey, []byte(store.ReplicatedControlPrefix + "future-state")} {
				t.Run(fmt.Sprintf("enabled-%t-delete-%t-%s", enabled, tombstone, key), func(t *testing.T) {
					runPrepareEvidenceSafetyMigrationCase(t, enabled, tombstone, key)
				})
			}
		}
	}
}

func runPrepareEvidenceSafetyMigrationCase(t *testing.T, enabled, tombstone bool, key []byte) {
	t.Helper()
	_, _, fsm := newPrepareSafetyGate(t)
	if bytes.Equal(key, replicatedPinStateKey) {
		seedActivatedPinJournal(t, fsm.store)
		require.NoError(t, fsm.reloadReplicatedBackupPins(t.Context()))
	}
	if enabled {
		require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	}
	ctx := context.Background()
	before, beforeErr := fsm.store.GetAt(ctx, key, ^uint64(0))
	userKey := []byte("ordinary-imported-user")
	userVersion := &pb.MVCCVersion{Key: userKey, CommitTs: 1000, Value: []byte("value")}
	control := &pb.MVCCVersion{Key: key, CommitTs: 1000, Value: []byte{1}, Tombstone: tombstone}
	if tombstone {
		control.Value = nil
	}
	request := &pb.ImportRangeVersionsRequest{JobId: 7, BracketId: 1, BatchSeq: 1,
		Cursor: []byte("end"), Versions: []*pb.MVCCVersion{userVersion, control}}
	command, err := MarshalMigrationImportCommand(request)
	require.NoError(t, err)
	result := fsm.Apply(command)
	applyErr, ok := result.(error)
	require.True(t, ok, "a control-key import must be a deterministic rejection, got %T", result)
	require.ErrorIs(t, applyErr, ErrInvalidRequest)
	for _, rawKey := range [][]byte{key, userKey} {
		_, err = fsm.store.GetAt(ctx, distribution.MigrationStagedDataKey(7, rawKey), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound, "the entire rejected batch must remain unapplied")
	}
	after, afterErr := fsm.store.GetAt(ctx, key, ^uint64(0))
	if beforeErr != nil {
		require.ErrorIs(t, beforeErr, store.ErrKeyNotFound)
		require.ErrorIs(t, afterErr, store.ErrKeyNotFound)
	} else {
		require.NoError(t, afterErr)
		require.Equal(t, before, after)
	}
	// Reuse sequence one: rejected protocol rows must not advance import progress.
	request.Versions = []*pb.MVCCVersion{userVersion}
	command, err = MarshalMigrationImportCommand(request)
	require.NoError(t, err)
	_, ok = fsm.Apply(command).(store.ImportVersionsResult)
	require.True(t, ok, "the same batch sequence must still accept ordinary user data")
	value, err := fsm.store.GetAt(ctx, distribution.MigrationStagedDataKey(7, userKey), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestPrepareEvidenceSafetyMarkerCannotBeForgedByTwoPhaseWrite(t *testing.T) {
	_, _, fsm := newPrepareSafetyGate(t)
	meta := TxnMeta{PrimaryKey: prepareEvidenceEnabledKey, LockTTLms: defaultTxnLockTTLms}
	req := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 10, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(TxnMetaPrefix), Value: EncodeTxnMeta(meta)},
		{Op: pb.Op_PUT, Key: prepareEvidenceEnabledKey, Value: []byte{1}},
	}}
	prepareErr := applyFSMRequest(t, fsm, req)
	if prepareErr == nil {
		meta.CommitTS, req.Phase = 20, pb.Phase_COMMIT
		req.Mutations[0].Value = EncodeTxnMeta(meta)
		require.NoError(t, applyFSMRequest(t, fsm, req))
	}
	enabled, err := PrepareEvidenceEnabled(context.Background(), fsm.store)
	require.NoError(t, err)
	require.False(t, enabled, "a client 2PC must not manufacture the negotiated protocol marker")
	require.ErrorIs(t, prepareErr, ErrInvalidRequest)
}

func TestPrepareEvidenceSafetyExistingForgedIntentCannotMaterializeMarker(t *testing.T) {
	_, _, fsm := newPrepareSafetyGate(t)
	ctx := context.Background()
	// Simulate state prepared before the write-namespace protection existed.
	require.NoError(t, fsm.store.PutAt(ctx, txnLockKey(prepareEvidenceEnabledKey), encodeTxnLock(txnLock{
		StartTS: 10, PrimaryKey: prepareEvidenceEnabledKey, IsPrimaryKey: true,
	}), 10, 0))
	require.NoError(t, fsm.store.PutAt(ctx, txnIntentKey(prepareEvidenceEnabledKey), encodeTxnIntent(txnIntent{
		StartTS: 10, Op: txnIntentOpPut, Value: []byte{1},
	}), 10, 0))
	commit := &pb.Request{IsTxn: true, Phase: pb.Phase_COMMIT, Ts: 10, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(TxnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: prepareEvidenceEnabledKey, CommitTS: 20})},
		{Op: pb.Op_PUT, Key: prepareEvidenceEnabledKey},
	}}
	require.ErrorIs(t, applyFSMRequest(t, fsm, commit), ErrInvalidRequest)
	enabled, err := PrepareEvidenceEnabled(ctx, fsm.store)
	require.NoError(t, err)
	require.False(t, enabled)
}

func TestPrepareEvidenceSafetyPendingFreezesCachedBatchWindow(t *testing.T) {
	gate := &testPhaseDGate{prepareErr: ErrTSOPhaseDActivationPending}
	allocator, fsm, _, _ := pendingTestAllocator(t, gate)
	batch, err := NewBatchAllocator(allocator, 8)
	require.NoError(t, err)
	_, err = batch.Next(context.Background())
	require.NoError(t, err, "prime a preactivation batch that still has unused slots")
	_, err = allocator.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.NotZero(t, fsm.PendingPhaseDFloor())
	_, err = batch.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending, "pending must stop ordinary issuance from cached windows too")
}

func TestPrepareEvidenceSafetyRequiresEveryMembersCutoverAdoption(t *testing.T) {
	for _, groupID := range []uint64{0, 1} {
		for _, role := range []string{"voter", "learner"} {
			t.Run(fmt.Sprintf("group-%d-%s", groupID, role), func(t *testing.T) {
				shards, engine, _ := newPrepareSafetyGate(t)
				var adopted atomic.Bool
				address := startPrepareSafetyPeer(t, &prepareSafetyPeer{probe: func(context.Context, *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
					return &pb.PrepareEvidenceStateResponse{Supported: true, CutoverApplied: adopted.Load(), DedicatedTsoConfigured: true}, nil
				}})
				member := &prepareSafetyEngine{recordingTSOEngine: &recordingTSOEngine{state: raftengine.StateLeader,
					leader: raftengine.LeaderInfo{ID: "self"}, apply: engine.apply}, members: []raftengine.Server{
					{ID: "self", Suffrage: "voter"}, {ID: "remote", Address: address, Suffrage: role},
				}}
				shards.groups[groupID].Engine = member
				require.NoError(t, shards.CheckPrepareEvidenceMember(context.Background(), groupID, address), "admission negotiates support without requiring adoption")
				require.ErrorIs(t, shards.CheckPhaseDProtocol(context.Background()), ErrTSOPhaseDActivationPending)
				require.Empty(t, engine.proposedPayloads(), "no marker before every member has adopted dedicated TSO")
				adopted.Store(true)
				require.NoError(t, shards.CheckPhaseDProtocol(context.Background()))
			})
		}
	}
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("local-missing-%t", missing), func(t *testing.T) {
			shards, engine, _ := newPrepareSafetyGate(t)
			shards.groups[0].TSOState = NewTSOStateMachine(NewHLC())
			if missing {
				shards.groups[0].TSOState = nil
			}
			require.ErrorIs(t, shards.CheckPhaseDProtocol(context.Background()), ErrTSOPhaseDActivationPending)
			require.Empty(t, engine.proposedPayloads())
		})
	}
}

func prepareSafetyCreationRequest(kind string, timestamp uint64) *pb.Request {
	switch kind {
	case "onephase":
		return onePhaseReq(timestamp-1, timestamp, 0, []byte("late"), []byte("value"))
	case "prepare":
		request := preparedEvidenceRequest(pb.Phase_PREPARE, timestamp)
		request.Ts = timestamp - 1
		return request
	default:
		return &pb.Request{Ts: timestamp, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("late"), Value: []byte("value")}}}
	}
}

func newPrepareSafetyFSM(t *testing.T, st store.MVCCStore) *kvFSM {
	t.Helper()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	return fsm
}

func prepareSafetyAdmittedCreation(kind string, timestamp, floor uint64) *pb.Request {
	request := prepareSafetyCreationRequest(kind, timestamp)
	request.TimestampAdmissionFloor = floor
	return request
}

func TestPrepareEvidenceSafetyCreationSealRejectsLateRequests(t *testing.T) {
	for _, kind := range []string{"raw", "onephase", "prepare"} {
		t.Run(kind, func(t *testing.T) {
			shards, _, fsm := newPrepareSafetyGate(t)
			ctx := context.Background()
			require.NoError(t, shards.CheckPhaseDProtocol(ctx))
			// The old request already chose a timestamp above the eventual B=100.
			payload, err := proto.Marshal(prepareSafetyCreationRequest(kind, 1000))
			require.NoError(t, err)
			require.NoError(t, shards.SealPhaseDCreation(ctx))
			require.NoError(t, shards.PreparePhaseD(ctx, 100))
			result, ok := fsm.Apply(payload).(error)
			require.True(t, ok)
			require.ErrorIs(t, result, ErrPrepareCreationSealed)
			for _, key := range [][]byte{[]byte("late"), txnLockKey([]byte("primary")), txnIntentKey([]byte("primary"))} {
				_, err = fsm.store.GetAt(ctx, key, ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound, "rejected creation must leave no user data or intent")
			}
			require.NoError(t, shards.FinishPhaseD(ctx, 100))
			require.ErrorIs(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation(kind, 100, 100)), ErrPrepareAdmissionFenced)
			require.NoError(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation(kind, 1001, 100)))
		})
	}
}

func TestPrepareEvidenceSafetyCreationSealAllowsExistingResolution(t *testing.T) {
	for _, proof := range []uint64{0, 20} {
		for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
			t.Run(fmt.Sprintf("proof-%d-%s", proof, phase), func(t *testing.T) {
				_, _, fsm := newPrepareSafetyGate(t)
				if proof != 0 {
					require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
				}
				require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, proof)))
				require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
				require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
				require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(100)))
				require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(phase, 20)))
				_, err := fsm.store.GetAt(context.Background(), txnLockKey([]byte("primary")), ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
				value, err := fsm.store.GetAt(context.Background(), []byte("primary"), 20)
				if phase == pb.Phase_COMMIT {
					require.NoError(t, err)
					require.Equal(t, []byte("value"), value)
				} else {
					require.ErrorIs(t, err, store.ErrKeyNotFound)
				}
				require.ErrorIs(t, applyFSMRequest(t, fsm, prepareSafetyCreationRequest("raw", 1000)), ErrPrepareCreationSealed)
			})
		}
	}
}

func TestPrepareEvidenceSafetyCreationGateSurvivesRecovery(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		for _, released := range []bool{false, true} {
			t.Run(fmt.Sprintf("snapshot-%t-released-%t", snapshot, released), func(t *testing.T) {
				dir := t.TempDir()
				st, err := store.NewPebbleStore(dir)
				require.NoError(t, err)
				closed := false
				t.Cleanup(func() {
					if !closed {
						require.NoError(t, st.Close())
					}
				})
				fsm := newPrepareSafetyFSM(t, st)
				require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
				require.NoError(t, applyFSMRequest(t, fsm, preparedEvidenceRequest(pb.Phase_PREPARE, 20)))
				require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
				require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(100)))
				if released {
					require.Nil(t, fsm.Apply(EncodePrepareEvidenceRelease(100)))
				}
				var recovered *kvFSM
				if snapshot {
					snap, err := fsm.Snapshot()
					require.NoError(t, err)
					defer snap.Close()
					var raw bytes.Buffer
					_, err = snap.WriteTo(&raw)
					require.NoError(t, err)
					destination, err := store.NewPebbleStore(t.TempDir())
					require.NoError(t, err)
					recovered = newPrepareSafetyFSM(t, destination)
					t.Cleanup(func() { require.NoError(t, recovered.store.Close()) })
					// Restore must replace the opposite in-memory gate state, not retain it.
					require.Nil(t, recovered.Apply(EncodePrepareEvidenceFence(100)))
					require.Nil(t, recovered.Apply(EncodePrepareEvidenceSeal()))
					if !released {
						require.Nil(t, recovered.Apply(EncodePrepareEvidenceRelease(100)))
					}
					require.NoError(t, recovered.Restore(bytes.NewReader(raw.Bytes())))
				} else {
					require.NoError(t, st.Close())
					closed = true
					reopened, err := store.NewPebbleStore(dir)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, reopened.Close()) })
					recovered = newPrepareSafetyFSM(t, reopened)
				}
				creation := prepareSafetyAdmittedCreation("raw", 1000, 100)
				if released {
					require.Nil(t, recovered.Apply(EncodePrepareEvidenceSeal()), "a stale seal cannot re-close a released gate")
					require.NoError(t, applyFSMRequest(t, recovered, creation))
				} else {
					require.ErrorIs(t, applyFSMRequest(t, recovered, creation), ErrPrepareCreationSealed)
				}
				require.NoError(t, applyFSMRequest(t, recovered, preparedEvidenceRequest(pb.Phase_COMMIT, 20)))
				require.Nil(t, recovered.Apply(EncodePrepareEvidenceRelease(100)))
				require.Nil(t, recovered.Apply(EncodePrepareEvidenceRelease(100)), "release is retryable after recovery")
				require.ErrorIs(t, applyFSMRequest(t, recovered, prepareSafetyAdmittedCreation("raw", 100, 100)), ErrPrepareAdmissionFenced)
				require.NoError(t, applyFSMRequest(t, recovered, creation))
				value, err := recovered.store.GetAt(context.Background(), []byte("primary"), 20)
				require.NoError(t, err)
				require.Equal(t, []byte("value"), value)
			})
		}
	}
}

func TestPrepareEvidenceSafetyReleaseValidationAndRetry(t *testing.T) {
	_, _, fsm := newPrepareSafetyGate(t)
	require.ErrorIs(t, haltApplyOf(fsm.Apply(EncodePrepareEvidenceSeal())), ErrTSOProtocolUnsupported)
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(100)))
	require.ErrorIs(t, haltApplyOf(fsm.Apply(EncodePrepareEvidenceRelease(100))), ErrTxnInvalidMeta, "release before seal must fail")
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
	for _, floor := range []uint64{0, 101} {
		require.ErrorIs(t, haltApplyOf(fsm.Apply(EncodePrepareEvidenceRelease(floor))), ErrTxnInvalidMeta)
		require.ErrorIs(t, applyFSMRequest(t, fsm, prepareSafetyCreationRequest("raw", 1000)), ErrPrepareCreationSealed)
	}
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceRelease(100)))
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceRelease(100)))
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
	for _, floor := range []uint64{99, 101, 200} {
		require.ErrorIs(t, haltApplyOf(fsm.Apply(EncodePrepareEvidenceFence(floor))), ErrTxnInvalidMeta,
			"a released Phase-D barrier cannot be lowered or raised")
	}
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
	require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(100)))
	for _, floor := range []uint64{0, 99, 101, 200} {
		require.ErrorIs(t, haltApplyOf(fsm.Apply(EncodePrepareEvidenceRelease(floor))), ErrTxnInvalidMeta, "release identity must not change on retry")
	}
	fsm = newPrepareSafetyFSM(t, fsm.store)
	enabled, unproven, floor, err := InspectPrepareEvidence(context.Background(), fsm.store, 0)
	require.NoError(t, err)
	require.True(t, enabled)
	require.False(t, unproven)
	require.Equal(t, uint64(100), floor, "invalid fence attempts must not change the persisted barrier")
	require.NoError(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation("raw", 1000, 100)))
	require.ErrorIs(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation("raw", 1001, 200)), ErrPrepareAdmissionFenced)
	require.ErrorIs(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation("raw", 100, 100)), ErrPrepareAdmissionFenced)
}

func TestPrepareEvidenceSafetyPartialReleaseFanoutRetries(t *testing.T) {
	shards, _, first := newPrepareSafetyGate(t)
	other, engine, second := newPrepareSafetyGate(t)
	shards.groups[2] = other.groups[1]
	t.Cleanup(func() { delete(shards.groups, 2) })
	ctx := context.Background()
	require.NoError(t, shards.CheckPhaseDProtocol(ctx))
	require.NoError(t, shards.SealPhaseDCreation(ctx))
	require.NoError(t, shards.PreparePhaseD(ctx, 100))
	releaseErr := errors.New("second group unavailable during release")
	engine.setProposeError(releaseErr)
	require.ErrorIs(t, shards.FinishPhaseD(ctx, 100), releaseErr)
	require.NoError(t, applyFSMRequest(t, first, prepareSafetyAdmittedCreation("raw", 1000, 100)))
	require.ErrorIs(t, applyFSMRequest(t, second, prepareSafetyCreationRequest("raw", 1000)), ErrPrepareCreationSealed)
	engine.setProposeError(nil)
	require.NoError(t, shards.FinishPhaseD(ctx, 100))
	require.NoError(t, shards.FinishPhaseD(ctx, 100))
	for _, fsm := range []*kvFSM{first, second} {
		require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
		require.NoError(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation("raw", 1000, 100)))
		require.ErrorIs(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation("raw", 100, 100)), ErrPrepareAdmissionFenced)
	}
}

func TestPrepareEvidenceSafetyPreCutoverRequestsCannotLandAfterRelease(t *testing.T) {
	for _, kind := range []string{"raw", "onephase"} {
		t.Run(kind, func(t *testing.T) {
			shards, _, dataFSM := newPrepareSafetyGate(t)
			initial, state, _, _ := pendingTestAllocator(t, shards)
			shards.groups[0].TSOState = state
			allocator, err := NewRaftTSOAllocator(initial.group, initial.clock,
				WithTSOCutoverFloorProvider(shards), WithTSOPhaseDActivationGate(shards))
			require.NoError(t, err)
			// A different data leader may have issued from a higher leased HLC
			// before cutover. Keep its request off the Raft log until release.
			legacyClock := NewHLC()
			legacyClock.SetPhysicalCeiling(initial.clock.PhysicalCeiling() + time.Minute.Milliseconds())
			chosen, err := legacyClock.NextFenced()
			require.NoError(t, err)
			payload, err := proto.Marshal(prepareSafetyCreationRequest(kind, chosen))
			require.NoError(t, err)
			window, err := allocator.ReserveBatchAfter(context.Background(), 1, 0, true, true)
			require.NoError(t, err)
			require.Greater(t, chosen, window.Base, "the fresh committed floor cannot see this uncommitted request")
			result := dataFSM.Apply(payload)
			applyErr, _ := result.(error)
			require.ErrorIs(t, applyErr, ErrPrepareAdmissionFenced, "old %s chosen_ts=%d landed after release above the already-issued read_ts=%d (result %T)", kind, chosen, window.Base, result)
			_, err = dataFSM.store.GetAt(context.Background(), []byte("late"), ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestPrepareEvidenceSafetyReleasedGateRequiresExactAdmissionEpoch(t *testing.T) {
	for _, kind := range []string{"raw", "onephase", "prepare"} {
		t.Run(kind, func(t *testing.T) {
			_, _, fsm := newPrepareSafetyGate(t)
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(100)))
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceRelease(100)))
			for _, epoch := range []uint64{0, 99, 101} {
				require.ErrorIs(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation(kind, 1000, epoch)), ErrPrepareAdmissionFenced)
			}
			require.NoError(t, applyFSMRequest(t, fsm, prepareSafetyAdmittedCreation(kind, 1000, 100)))
		})
	}
}

func TestPrepareEvidenceSafetyPostCutoverReadVoucherDoesNotBypassBackupPin(t *testing.T) {
	for _, backup := range []bool{false, true} {
		t.Run(fmt.Sprintf("backup-%t", backup), func(t *testing.T) {
			tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
			t.Cleanup(tracker.Close)
			fsm := newBackupTestFSM(t, tracker)
			t.Cleanup(func() { require.NoError(t, fsm.store.Close()) })
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(100)))
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceSeal()))
			require.Nil(t, fsm.Apply(EncodePrepareEvidenceRelease(100)))
			if backup {
				require.Nil(t, fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
					PinID: backupTrackerTestPinID(1), ReadTS: 75, Deadline: time.Now().Add(time.Minute),
				})))
			}
			request := prepareSafetyAdmittedCreation("prepare", 101, 100)
			request.Ts = 50
			err := applyFSMRequest(t, fsm, request)
			if backup {
				require.ErrorIs(t, err, ErrBackupTimestampFenced, "a real backup still fences the old read timestamp")
				return
			}
			require.NoError(t, err, "Phase-D B fences the new durable proof, not an old applied-read voucher")
			commit := preparedEvidenceRequest(pb.Phase_COMMIT, 101)
			commit.Ts = 50
			require.NoError(t, applyFSMRequest(t, fsm, commit))
			value, err := fsm.store.GetAt(context.Background(), []byte("primary"), 101)
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
		})
	}
}
