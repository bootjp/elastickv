package store_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Preserve the real memory backend's optional interfaces: hiding migration
// capabilities behind a spy would not test the actual FSM dispatch behavior.
type scopeAuditBackend interface {
	store.MVCCStore
	store.RaftEntryApplier
	store.RaftControlMetadataStore
	store.ReplicatedControlStore
	store.MigrationPromoter
	store.MigrationCleaner
	store.MigrationOwnershipStore
	store.MigrationOwnershipRefresher
	store.MigrationOwnershipPartitionStore
	store.MigrationTargetReadinessWriter
	store.MigrationTargetReadinessRaftWriter
	store.MigrationTargetReadinessReader
	store.MigrationServingAdmissionReader
	store.MigrationServingSummaryReader
	store.MigrationRetirementReader
	store.MigrationImportMetadataReader
	store.MigrationPromotionStateReader
	store.VersionPresenceReader
	store.RetentionController
}

type scopeAuditStore struct {
	scopeAuditBackend
	scopes int
}

func (s *scopeAuditStore) ApplyRaftEntry(ctx context.Context, index uint64, fn func(context.Context) error) error {
	s.scopes++
	return s.scopeAuditBackend.ApplyRaftEntry(ctx, index, fn)
}

func newScopeAuditFSM(t *testing.T) (*scopeAuditStore, kv.FSM) {
	t.Helper()
	return scopeAuditFSMForStore(t, store.NewMVCCStore())
}

func scopeAuditFSMForStore(t *testing.T, backend store.MVCCStore, opts ...kv.FSMOption) (*scopeAuditStore, kv.FSM) {
	t.Helper()
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	capabilities, ok := backend.(scopeAuditBackend)
	require.True(t, ok)
	observed := &scopeAuditStore{scopeAuditBackend: capabilities}
	return observed, kv.NewKvFSMWithHLC(observed, kv.NewHLC(), opts...)
}

func TestActualFSMRawMigrationTrackerScopeRouting(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "pebble"}[disk], func(t *testing.T) {
			backend := store.NewMVCCStore()
			if disk {
				var err error
				backend, err = store.NewPebbleStore(t.TempDir())
				require.NoError(t, err)
			}
			s, fsm := scopeAuditFSMForStore(t, backend, kv.WithRouteHistory(nil, 1))
			configuration, ok := fsm.(interface{ RecordAppliedConfiguration(uint64) error })
			require.True(t, ok)
			require.NoError(t, configuration.RecordAppliedConfiguration(9))
			state := store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true, RetentionPinTS: 1, MinWriteTSExclusive: 10,
				RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 1}
			require.NoError(t, s.ApplyTargetStagedReadiness(t.Context(), state))
			require.NoError(t, s.AcquireMigrationOwnership(t.Context(), store.MigrationOwnershipState{
				JobID: 7, Generation: "0123456789abcdef0123456789abcdef", RouteID: 1, DescriptorHash: bytes.Repeat([]byte{1}, 32),
				LocalGroupID: 1, OwnerGroupID: 1, CatalogVersion: 1, ConfigurationIndex: 9, Role: store.MigrationOwner,
				RouteStart: state.RouteStart, RouteEnd: state.RouteEnd, MinWriteTSExclusive: 10, WriteFloorKnown: true, SourceTrackerVersion: 1,
			}, 11))
			indexed, ok := fsm.(interface{ SetApplyIndex(uint64) })
			require.True(t, ok)
			indexed.SetApplyIndex(12)
			applyScopeAuditRequests(t, fsm,
				&pb.Request{Ts: 30, ObservedRouteVersion: kv.EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("a")}}},
				&pb.Request{Ts: 20, ObservedRouteVersion: kv.EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("o"), Value: []byte("b")}}})
			require.Equal(t, 1, s.scopes)
			states, err := s.MigrationTargetReadinessStates(t.Context())
			require.NoError(t, err)
			require.Len(t, states, 1)
			require.Equal(t, uint64(20), states[0].MinAdmittedTS, "second request must see and lower the staged tracker")
			for _, key := range []string{"n", "o"} {
				_, err := s.GetAt(t.Context(), []byte(key), 30)
				require.NoError(t, err)
			}
			indexed.SetApplyIndex(13)
			applyScopeAuditRequests(t, fsm,
				&pb.Request{Ts: 40, ObservedRouteVersion: kv.EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("p"), Value: []byte("prefix-victim")}}},
				&pb.Request{Ts: 41, ObservedRouteVersion: kv.EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte("p")}}})
			require.Equal(t, 2, s.scopes)
			_, err = s.GetAt(t.Context(), []byte("p"), 41)
			require.ErrorIs(t, err, store.ErrKeyNotFound, "actual FSM prefix delete must see the earlier staged key")
		})
	}
}

func applyScopeAuditRequests(t *testing.T, fsm kv.FSM, requests ...*pb.Request) {
	t.Helper()
	payload, err := proto.Marshal(&pb.RaftCommand{Requests: requests})
	require.NoError(t, err)
	// The production RaftCommand batch frame is 0x01 (kv/fsm.go).
	require.Nil(t, fsm.Apply(append([]byte{0x01}, payload...)))
}

func scopeAuditTxnRequest(phase pb.Phase, primary, key string, start, commit uint64) *pb.Request {
	value := []byte("committed-" + key)
	if phase == pb.Phase_COMMIT {
		value = nil
	}
	return &pb.Request{IsTxn: true, Phase: phase, Ts: start, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte(primary), LockTTLms: 1000, CommitTS: commit})},
		{Op: pb.Op_PUT, Key: []byte(key), Value: value},
	}}
}

func TestActualFSMPrimarySecondaryDoNotEnterRawScope(t *testing.T) {
	primary, primaryFSM := newScopeAuditFSM(t)
	secondary, secondaryFSM := newScopeAuditFSM(t)
	for _, participant := range []struct {
		store *scopeAuditStore
		fsm   kv.FSM
		keys  []string
	}{
		{primary, primaryFSM, []string{"primary", "primary-extra"}},
		{secondary, secondaryFSM, []string{"secondary", "secondary-extra"}},
	} {
		requests := make([]*pb.Request, 0, 2)
		for _, key := range participant.keys {
			requests = append(requests, scopeAuditTxnRequest(pb.Phase_PREPARE, "primary", key, 10, 20))
		}
		applyScopeAuditRequests(t, participant.fsm, requests...)
		require.Zero(t, participant.store.scopes, "transaction batch must bypass the raw-only scope")
	}
	for _, participant := range []struct {
		store *scopeAuditStore
		fsm   kv.FSM
		keys  []string
	}{
		{primary, primaryFSM, []string{"primary", "primary-extra"}},
		{secondary, secondaryFSM, []string{"secondary", "secondary-extra"}},
	} {
		requests := make([]*pb.Request, 0, 2)
		for _, key := range participant.keys {
			requests = append(requests, scopeAuditTxnRequest(pb.Phase_COMMIT, "primary", key, 10, 20))
		}
		applyScopeAuditRequests(t, participant.fsm, requests...)
		require.Zero(t, participant.store.scopes)
		for _, key := range participant.keys {
			value, err := participant.store.GetAt(t.Context(), []byte(key), 20)
			require.NoError(t, err)
			require.Equal(t, []byte("committed-"+key), value)
		}
	}
}

func TestActualFSMRawControlAndSnapshotScopeRouting(t *testing.T) {
	s, fsm := newScopeAuditFSM(t)
	applyScopeAuditRequests(t, fsm,
		&pb.Request{Ts: 10, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("raw-a"), Value: []byte("a")}}},
		&pb.Request{Ts: 11, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("raw-b"), Value: []byte("b")}}})
	require.Equal(t, 1, s.scopes)
	require.Nil(t, fsm.Apply(kv.EncodeWatermarkAdvance(30)))
	require.Equal(t, 1, s.scopes, "reserved watermark control dispatches before raw batches")
	require.Equal(t, uint64(30), s.LastCommitTS())
	// Exercise the storage control API separately; the v5 FSM wrapper is not
	// wired here, and this test makes no manifest/identity acceptance claim.
	require.NoError(t, s.ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
		return s.StageRaftControlMetadata(ctx, []byte("P"), []byte("retained-policy"))
	}))
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	target, targetFSM := newScopeAuditFSM(t)
	require.NoError(t, targetFSM.Restore(&payload))
	value, err := target.GetAt(t.Context(), []byte("raw-a"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("a"), value)
	policy, err := target.ReadRaftControlMetadata(t.Context(), []byte("P"))
	require.NoError(t, err)
	require.Equal(t, []byte("retained-policy"), policy)
}

func TestActualFSMMigrationsBypassIndexedScope(t *testing.T) {
	s, fsm := newScopeAuditFSM(t)
	importPayload, err := kv.MarshalMigrationImportCommand(&pb.ImportRangeVersionsRequest{
		JobId: 9, BracketId: 1, BatchSeq: 1, Versions: []*pb.MVCCVersion{{Key: []byte("user|key"), Value: []byte("migrated"), CommitTs: 10, KeyFamily: distribution.MigrationFamilyUser}},
	})
	require.NoError(t, err)
	result, ok := fsm.Apply(importPayload).(store.ImportVersionsResult)
	require.True(t, ok)
	require.Equal(t, uint64(10), result.MaxImportedTS)
	require.Zero(t, s.scopes)
	value, err := s.GetAt(t.Context(), distribution.MigrationStagedDataKey(9, []byte("user|key")), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("migrated"), value)
	promote, err := kv.MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 9})
	require.NoError(t, err)
	promoted, ok := fsm.Apply(promote).(store.PromoteVersionsResult)
	require.True(t, ok)
	require.Equal(t, uint64(1), promoted.PromotedRows)
	value, err = s.GetAt(t.Context(), []byte("user|key"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("migrated"), value)
	cleanup, err := kv.MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 9, MaxCommitTs: 10, KeyFamily: distribution.MigrationFamilyUser})
	require.NoError(t, err)
	cleaned, ok := fsm.Apply(cleanup).(store.CleanupVersionsResult)
	require.True(t, ok)
	require.Equal(t, uint64(1), cleaned.DeletedRows)
	retire, err := kv.MarshalMigrationRetireCommand(9)
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(retire))
	readiness, err := kv.MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{JobId: 77, MigrationJobId: 77, Armed: true, MinWriteTsExclusive: 40})
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(readiness))
	states, err := s.MigrationTargetReadinessStates(t.Context())
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Equal(t, uint64(77), states[0].JobID)
	require.Zero(t, s.scopes, "typed migrations must never hit the new indexed-scope rejection")
}
