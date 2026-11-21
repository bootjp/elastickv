package kv

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestOnlineTargetLockOnlyAndReplicaCapability(t *testing.T) {
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	id := store.MigrationCaptureIdentity{SourceGroupID: 1, JobID: 9, Generation: 1, Index: 10, Lineage: [32]byte{1}}
	session := store.OnlineMigrationSession{Version: 1, Identity: id, StartIndex: 10, LastIndex: 10, ConfigurationIndex: 1, RouteStart: []byte("m"), RouteEnd: []byte("z")}
	manifest, err := store.EncodeOnlineMigrationManifest(session)
	require.NoError(t, err)
	req := &pb.TargetStagedReadinessRequest{JobId: 9, MigrationJobId: 9, Armed: true, MinWriteTsExclusive: 1, OnlineTargetManifest: manifest, OnlineCaptureVersion: 1, RouteStart: session.RouteStart, RouteEnd: session.RouteEnd, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 1}
	f := &kvFSM{store: st, shardGroupID: 2, pendingApplyIdx: 10}
	f.migrationConfigurationIndex.Store(1)
	key := []byte("a") // Outside the moving range, but the target must be lock-free group-wide.
	require.NoError(t, st.PutAt(t.Context(), txnLockKey(key), encodeTxnLock(txnLock{PrimaryKey: key, StartTS: 1}), 2, 0))
	requireOnlineVerdict(t, f.applyOnlineMigrationReadiness(t.Context(), req), store.ErrOnlineMigrationTargetNotEmpty)
	_, err = store.ReadOnlineMigrationTarget(t.Context(), st, 9)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	f.store = store.NewMVCCStore()
	h, ok := f.applyOnlineMigrationReadiness(t.Context(), req).(interface{ HaltApply() error })
	require.True(t, ok)
	require.ErrorIs(t, h.HaltApply(), store.ErrNotSupported)
	f.store = st
	// Malformed replicated input is a request verdict; corrupt persisted
	// metadata must halt the replica at that entry.
	req.OnlineTargetManifest = []byte("bad")
	response := f.applyOnlineMigrationReadiness(t.Context(), req)
	requireOnlineVerdict(t, response, store.ErrOnlineMigrationIdentity)
	require.False(t, onlineMigrationControlVerdict(store.ErrMigrationCaptureCorrupt))
}

func TestOnlineLegacyDeltaBothOwnersAndSeal(t *testing.T) {
	key := store.LegacyListMetaDeltaKey([]byte("x"), 5, 0)
	req := &pb.Request{Ts: 10, Mutations: []*pb.Mutation{{Op: pb.Op_DEL, Key: key}}}
	session := store.OnlineMigrationSession{RouteStart: []byte("m"), RouteEnd: []byte("z")}
	require.True(t, onlineRequestIntersects(req, session))
	require.False(t, onlinePointRequests([]*pb.Request{req}))
	require.True(t, onlineMaintenanceIntersects(key, session))
	session.RouteStart, session.RouteEnd = []byte("d"), []byte("e")
	require.True(t, onlineRequestIntersects(req, session))
	session.RouteStart, session.RouteEnd = []byte("i"), []byte("j")
	require.False(t, onlineRequestIntersects(req, session))
}

func TestOnlineMaintenanceRespectsSealedSource(t *testing.T) {
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	s := store.OnlineMigrationSession{Version: 1, Identity: store.MigrationCaptureIdentity{SourceGroupID: 1, JobID: 9, Generation: 1, Index: 10, Lineage: [32]byte{1}}, RouteStart: []byte("m"), RouteEnd: []byte("z"), StartIndex: 10, LastIndex: 10, ConfigurationIndex: 3}
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	cleaner, ok := st.(store.MigrationCleaner)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 10, func(tx context.Context) error { return store.StartOnlineMigrationSession(tx, st, s) }))
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 11, func(tx context.Context) error {
		var e error
		s, e = store.SealOnlineMigrationSession(tx, st, 9, 3)
		return e
	}))
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 12, func(tx context.Context) error {
		after, e := store.SealOnlineMigrationSession(tx, st, 9, 5)
		require.Equal(t, s, after)
		return e
	}))
	guard := &store.OnlineMaintenanceGuard{Sessions: []store.OnlineMigrationSession{s}, Intersects: onlineMaintenanceIntersects}
	for _, key := range []string{"a", "x"} {
		require.NoError(t, st.PutAt(t.Context(), []byte(key), []byte("live"), 15, 0))
	}
	for _, key := range []string{"a", "x"} {
		opts := store.CleanupVersionsOptions{JobID: 22, AppliedIndex: 17, StartKey: []byte(key), EndKey: nextScanCursor([]byte(key)), MaxVersions: 5, MaxCommitTS: 20, OnlineGuard: guard}
		_, err := cleaner.CleanupVersions(t.Context(), opts)
		if key == "a" {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, store.ErrOnlineMigrationSealed)
		}
	}
	value, err := st.GetAt(t.Context(), []byte("x"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("live"), value)
	require.ErrorIs(t, store.InvalidateOnlineMigration(t.Context(), st, s, "late control"), store.ErrOnlineMigrationSealed)
	got, err := store.ReadOnlineMigrationSession(t.Context(), st, 9)
	require.NoError(t, err)
	require.Equal(t, s, got)
}

func TestOnlineJoiningAndSnapshotReattestation(t *testing.T) {
	openFSM := func() *kvFSM {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		f, ok := NewKvFSMWithHLC(st, nil, WithRouteHistory(nil, 2)).(*kvFSM)
		require.True(t, ok)
		return f
	}
	session := store.OnlineMigrationSession{Version: 1, Identity: store.MigrationCaptureIdentity{SourceGroupID: 1, JobID: 9, Generation: 1, Index: 10, Lineage: [32]byte{1}}, StartIndex: 10, LastIndex: 10, ConfigurationIndex: 5, RouteStart: []byte("m"), RouteEnd: []byte("z")}
	manifest, err := store.EncodeOnlineMigrationManifest(session)
	require.NoError(t, err)
	prepare := &pb.TargetStagedReadinessRequest{JobId: 9, MigrationJobId: 9, Armed: true, MinWriteTsExclusive: 1, OnlineTargetManifest: manifest, OnlineCaptureVersion: 1, RouteStart: session.RouteStart, RouteEnd: session.RouteEnd, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 5}
	apply := func(f *kvFSM, index uint64, req *pb.TargetStagedReadinessRequest) any {
		command, e := MarshalTargetStagedReadinessCommand(req)
		require.NoError(t, e)
		f.SetApplyIndex(index)
		return f.Apply(command)
	}
	config := namespaceCommand(5, 1)
	joining := namespaceCommand(5, 1)
	joining.Joining = []NamespaceMemberCapability{{MemberID: 2, Version: MigrationNamespaceVersion, ConfigurationIndex: 5}}
	// Joining authorization already in the log when the online proposal arrives.
	prior := openFSM()
	require.NoError(t, prior.RecordAppliedNamespaceConfiguration(5, []uint64{1}))
	require.Nil(t, applyNamespaceCommand(t, prior, 6, joining))
	requireOnlineVerdict(t, apply(prior, 10, prepare), store.ErrOnlineMigrationIdentity)
	// Reverse the order. The same unsupported candidate cannot be authorized.
	leader := openFSM()
	require.NoError(t, leader.RecordAppliedNamespaceConfiguration(5, []uint64{1}))
	require.Nil(t, applyNamespaceCommand(t, leader, 6, config))
	require.Nil(t, apply(leader, 10, prepare))
	requireOnlineVerdict(t, applyNamespaceCommand(t, leader, 11, joining), ErrMigrationNamespaceProtocol)
	joining.Joining[0].OnlineCaptureVersion = store.OnlineMigrationVersion
	require.Nil(t, applyNamespaceCommand(t, leader, 12, joining))
	snapshot, err := leader.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snapshot.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	learner := openFSM()
	require.NoError(t, learner.Restore(&buf))
	// Both actual FSM instances install the expanded roster from the same
	// starting image and independently validate Ready against their own state.
	session.Sealed = true
	seal, err := store.EncodeOnlineMigrationManifest(session)
	require.NoError(t, err)
	ready := proto.CloneOf(prepare)
	ready.OnlineTargetManifest = seal
	ready.OnlineReady = true
	ready.ExpectedConfigurationIndex = 13
	for _, f := range []*kvFSM{leader, learner} {
		require.NoError(t, f.RecordAppliedNamespaceConfiguration(13, []uint64{1, 2}))
		tail := proto.CloneOf(ready)
		tail.OnlineReady = false
		// The new target configuration must admit tail imports before Ready.
		// A follower recovers the same initial cursor from the native snapshot.
		require.Nil(t, apply(f, 14, tail))
		target, e := store.ReadOnlineMigrationTarget(t.Context(), f.store, 9)
		require.NoError(t, e)
		require.False(t, target.Ready)
		require.Equal(t, uint64(13), target.ConfigurationIndex)
		require.Equal(t, session.StartIndex, target.LastSourceIndex)
		require.NoError(t, f.verifyOnlineImportConfiguration(t.Context(), &pb.ImportRangeVersionsRequest{JobId: 9, OnlineManifest: seal}))
		require.Nil(t, apply(f, 15, ready))
		requireOnlineVerdict(t, apply(f, 16, tail), store.ErrOnlineMigrationTargetClosed)
		state, e := store.ReadOnlineMigrationTarget(t.Context(), f.store, 9)
		require.NoError(t, e)
		require.True(t, state.Ready)
		require.Equal(t, uint64(13), state.ConfigurationIndex)
		require.Equal(t, session, state.Source)
	}
}

func requireOnlineVerdict(t *testing.T, response any, want error) {
	t.Helper()
	err, ok := response.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, want)
}
