package kv

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const onlineRegressionGeneration = "regression-owner-generation"

func onlineRegressionSession() store.OnlineMigrationSession {
	id := shadowCaptureID(10)
	id.Lineage = store.OnlineMigrationLineage(onlineRegressionGeneration)
	return store.OnlineMigrationSession{Version: store.OnlineMigrationVersion, Identity: id, OwnershipGeneration: onlineRegressionGeneration, StartIndex: 10, LastIndex: 10, ConfigurationIndex: 9, RouteStart: []byte("m"), RouteEnd: []byte("z")}
}

func onlineRegressionFSM(t *testing.T, sealed bool) (*kvFSM, store.MVCCStore) {
	t.Helper()
	st := g17OpenStore(t, t.TempDir())
	f := g17FSM(t, st, WithRouteHistory(nil, 1))
	require.NoError(t, f.RecordAppliedConfiguration(9))
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	writer, ok := st.(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 10, func(ctx context.Context) error {
		if err := store.StartOnlineMigrationSession(ctx, st, onlineRegressionSession()); err != nil {
			return err
		}
		return writer.ApplyTargetStagedReadinessAt(ctx, store.TargetStagedReadinessState{JobID: 9, MigrationJobID: 9, Armed: true, TrackWrites: true, SourceWriteFence: sealed, RetentionPinTS: 1, MinWriteTSExclusive: 1, RouteStart: []byte("m"), RouteEnd: []byte("z")}, 10)
	}))
	if sealed {
		require.NoError(t, applier.ApplyRaftEntry(t.Context(), 11, func(ctx context.Context) error { _, err := store.SealOnlineMigrationSession(ctx, st, 9, 9); return err }))
	}
	return f, st
}

func TestOnlineCaptureAmbiguousLegacyDeltaDeletionIntersects(t *testing.T) {
	t.Parallel()
	request := &pb.Request{Ts: 20, Mutations: []*pb.Mutation{{Op: pb.Op_DEL, Key: store.LegacyListMetaDeltaKey([]byte("x"), 10, 0)}}}
	require.True(t, onlineRequestIntersects(request, onlineRegressionSession()), "legacy list delta belongs to x as well as its ambiguous physical base key")
}

func TestOnlineCaptureSealedUnrelatedImportKeepsSeal(t *testing.T) {
	t.Parallel()
	f, st := onlineRegressionFSM(t, true)
	command, err := MarshalMigrationImportCommand(&pb.ImportRangeVersionsRequest{JobId: 100, BracketId: 1, BatchSeq: 1})
	require.NoError(t, err)
	f.SetApplyIndex(12)
	_ = f.Apply(command)
	session, err := store.ReadOnlineMigrationSession(t.Context(), st, 9)
	require.NoError(t, err)
	require.Empty(t, session.Invalid)
	require.True(t, session.Sealed)
	require.Zero(t, session.Sequence)
}

func TestOnlineCaptureUnsupportedReplicaHalts(t *testing.T) {
	t.Parallel()
	f := g17FSM(t, store.NewMVCCStore(), WithRouteHistory(nil, 1))
	require.NoError(t, f.RecordAppliedConfiguration(9))
	lineage := onlineRegressionSession().Identity.Lineage
	req := &pb.TargetStagedReadinessRequest{JobId: 9, MigrationJobId: 9, Armed: true, TrackWrites: true, RetentionPinTs: 1, MinWriteTsExclusive: 1, RouteStart: []byte("m"), RouteEnd: []byte("z"), RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9, OnlineCaptureVersion: store.OnlineMigrationVersion, OnlineLineage: lineage[:], OnlineSourceGeneration: onlineRegressionGeneration}
	command, err := MarshalTargetStagedReadinessCommand(req)
	require.NoError(t, err)
	f.SetApplyIndex(10)
	require.Error(t, haltApplyOf(f.Apply(command)), "local support mismatch must never advance past a committed control")
}

func TestOnlineCaptureCorruptStoredManifestHalts(t *testing.T) {
	t.Parallel()
	f, st := onlineRegressionFSM(t, false)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	key := binary.BigEndian.AppendUint64([]byte("R/online-split/v1/source/"), 9)
	key = binary.BigEndian.AppendUint64(key, 0)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 11, func(ctx context.Context) error { return metadata.StageRaftControlMetadata(ctx, key, []byte("{}")) }))
	lineage := onlineRegressionSession().Identity.Lineage
	request := &pb.TargetStagedReadinessRequest{JobId: 9, MigrationJobId: 9, Armed: true, TrackWrites: true, RetentionPinTs: 1, MinWriteTsExclusive: 1, RouteStart: []byte("m"), RouteEnd: []byte("z"), RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9, OnlineCaptureVersion: store.OnlineMigrationVersion, OnlineLineage: lineage[:], OnlineSourceGeneration: onlineRegressionGeneration}
	command, err := MarshalTargetStagedReadinessCommand(request)
	require.NoError(t, err)
	f.SetApplyIndex(12)
	require.Error(t, haltApplyOf(f.Apply(command)))
	index, _ := g17AppliedIndex(t, st)
	require.Equal(t, uint64(11), index)
}

func TestOnlineCaptureTargetLockOnlyIsNotFresh(t *testing.T) {
	t.Parallel()
	st := g17OpenStore(t, t.TempDir())
	f := g17FSM(t, st, WithRouteHistory(nil, 2))
	require.NoError(t, f.RecordAppliedConfiguration(9))
	require.NoError(t, st.PutAt(t.Context(), txnLockKey([]byte("n")), encodeTxnLock(txnLock{StartTS: 5, PrimaryKey: []byte("n")}), 5, 0))
	manifest, err := store.EncodeOnlineMigrationManifest(onlineRegressionSession())
	require.NoError(t, err)
	request := &pb.TargetStagedReadinessRequest{JobId: 9, MigrationJobId: 9, Armed: true, MinWriteTsExclusive: 1, RouteStart: []byte("m"), RouteEnd: []byte("z"), RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9, OnlineCaptureVersion: store.OnlineMigrationVersion, OnlineTargetManifest: manifest}
	command, err := MarshalTargetStagedReadinessCommand(request)
	require.NoError(t, err)
	f.SetApplyIndex(10)
	require.NotNil(t, f.Apply(command), "an unresolved lock is retained target state")
	_, err = store.ReadOnlineMigrationTarget(t.Context(), st, 9)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestOnlinePreparedTargetRetriesAfterUnrelatedLock(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ready bool
	}{{"sealed reattestation", false}, {"ready", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			st := g17OpenStore(t, t.TempDir())
			f := g17FSM(t, st, WithRouteHistory(nil, 2))
			source := onlineRegressionSession()
			require.NoError(t, f.RecordAppliedConfiguration(9))
			manifest, err := store.EncodeOnlineMigrationManifest(source)
			require.NoError(t, err)
			req := &pb.TargetStagedReadinessRequest{
				JobId: source.Identity.JobID, MigrationJobId: source.Identity.JobID, Armed: true,
				MinWriteTsExclusive: 1, RouteStart: source.RouteStart, RouteEnd: source.RouteEnd,
				RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9,
				OnlineCaptureVersion: store.OnlineMigrationVersion, OnlineTargetManifest: manifest,
			}
			apply := func(index uint64) any {
				t.Helper()
				cmd, err := MarshalTargetStagedReadinessCommand(req)
				require.NoError(t, err)
				f.SetApplyIndex(index)
				return f.Apply(cmd)
			}
			target := func() store.OnlineMigrationTarget {
				t.Helper()
				state, err := store.ReadOnlineMigrationTarget(ctx, st, req.GetJobId())
				require.NoError(t, err)
				return state
			}
			require.Nil(t, apply(10))
			prepared := target()
			require.False(t, prepared.Ready)

			require.NoError(t, f.RecordAppliedConfiguration(11))
			source.Sealed = true
			req.OnlineTargetManifest, err = store.EncodeOnlineMigrationManifest(source)
			require.NoError(t, err)
			req.ExpectedConfigurationIndex = 11
			req.OnlineReady = tc.ready
			key := []byte("a") // Outside the moving range [m, z).
			require.NoError(t, st.PutAt(ctx, txnLockKey(key), encodeTxnLock(txnLock{PrimaryKey: key, StartTS: 5}), 5, 0))
			for index := uint64(12); index < 18; index++ {
				requireOnlineVerdict(t, apply(index), ErrTxnLocked)
			}
			require.Equal(t, prepared, target())
			require.NoError(t, st.DeleteAt(ctx, txnLockKey(key), 6))
			require.Nil(t, apply(18))
			state := target()
			require.Equal(t, uint64(11), state.ConfigurationIndex)
			require.Equal(t, tc.ready, state.Ready)
			if !tc.ready {
				req.OnlineReady = true
				require.Nil(t, apply(19))
			}
			state = target()
			require.True(t, state.Ready)
			require.Equal(t, source, state.Source)
		})
	}
}

func TestOnlineSealRetryWithUnrelatedLock(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sealed bool
	}{{"initial seal", false}, {"replayed seal", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f, st := onlineRegressionFSM(t, tc.sealed)
			if !tc.sealed {
				// Persist the write fence, as the runner does before its first seal.
				writer, ok := st.(store.MigrationTargetReadinessRaftWriter)
				require.True(t, ok)
				guard := store.TargetStagedReadinessState{JobID: 9, MigrationJobID: 9, Armed: true, TrackWrites: true, SourceWriteFence: true, RetentionPinTS: 1, MinWriteTSExclusive: 1, RouteStart: []byte("m"), RouteEnd: []byte("z")}
				require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 11))
			}
			lockKey := []byte("a")
			require.NoError(t, st.PutAt(t.Context(), txnLockKey(lockKey), encodeTxnLock(txnLock{PrimaryKey: lockKey, StartTS: 7}), 7, 0))
			before, err := store.ReadOnlineMigrationSession(t.Context(), st, 9)
			require.NoError(t, err)
			req := &pb.TargetStagedReadinessRequest{JobId: 9, MigrationJobId: 9, Armed: true, TrackWrites: true, SourceWriteFence: true, RetentionPinTs: 1, MinWriteTsExclusive: 1, RouteStart: before.RouteStart, RouteEnd: before.RouteEnd, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9, OnlineCaptureVersion: store.OnlineMigrationVersion, OnlineSeal: true, OnlineLineage: before.Identity.Lineage[:]}
			cmd, err := MarshalTargetStagedReadinessCommand(req)
			require.NoError(t, err)
			f.SetApplyIndex(12)
			response := f.Apply(cmd)
			if tc.sealed {
				require.Nil(t, response)
			} else {
				requireOnlineVerdict(t, response, ErrTxnLocked)
			}
			after, err := store.ReadOnlineMigrationSession(t.Context(), st, 9)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestOnlineTrackerOwnershipGeneration(t *testing.T) {
	t.Parallel()
	st := g17OpenStore(t, t.TempDir())
	f := g17FSM(t, st, WithRouteHistory(nil, 1))
	require.NoError(t, f.RecordAppliedConfiguration(9))

	generation := "owner-before-acquire"
	lineage := store.OnlineMigrationLineage(generation)
	req := &pb.TargetStagedReadinessRequest{
		JobId: 9, MigrationJobId: 9, Armed: true, TrackWrites: true, RetentionPinTs: 1, MinWriteTsExclusive: 1,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9,
		OnlineCaptureVersion: store.OnlineMigrationVersion, OnlineLineage: lineage[:], OnlineSourceGeneration: "different-owner-generation",
	}
	apply := func(index uint64) any {
		t.Helper()
		cmd, err := MarshalTargetStagedReadinessCommand(req)
		require.NoError(t, err)
		f.SetApplyIndex(index)
		return f.Apply(cmd)
	}

	requireOnlineVerdict(t, apply(10), store.ErrOnlineMigrationIdentity)
	for i, nonce := range []string{"", strings.Repeat("x", 15), strings.Repeat("x", 129)} {
		invalid := store.OnlineMigrationLineage(nonce)
		req.OnlineSourceGeneration, req.OnlineLineage = nonce, invalid[:]
		requireOnlineVerdict(t, apply(11+uint64(i)), store.ErrOnlineMigrationIdentity)
	}
	_, err := store.ReadOnlineMigrationSession(t.Context(), st, 9)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	req.OnlineSourceGeneration, req.OnlineLineage = generation, lineage[:]
	require.Nil(t, apply(14))
	require.Nil(t, apply(15))
	session, err := store.ReadOnlineMigrationSession(t.Context(), st, 9)
	require.NoError(t, err)
	require.Equal(t, generation, session.OwnershipGeneration)
	require.Equal(t, uint64(14), session.StartIndex)
	require.Equal(t, lineage, session.Identity.Lineage)

	session.OwnershipGeneration = "different-owner"
	corrupt, err := store.EncodeOnlineMigrationManifest(session)
	require.NoError(t, err)
	_, err = store.DecodeOnlineMigrationManifest(corrupt)
	require.ErrorIs(t, err, store.ErrMigrationCaptureCorrupt)
}
