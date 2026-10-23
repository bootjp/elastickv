package etcd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const externalDestinationConfigurationIndex = uint64(42)

func prepareExternalConfigurationFixture(t *testing.T, format string, bearing bool) string {
	t.Helper()
	st := restartOriginStore(t, format)
	// Deliberately greater than the destination epoch: max(source, destination)
	// and trusting any copied epoch must both fail this regression.
	require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 9000), 10, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("ordinary"), []byte("value"), 20, 0))
	if indexed, ok := st.(interface{ SetDurableAppliedIndex(uint64) error }); ok {
		require.NoError(t, indexed.SetDurableAppliedIndex(9001))
	}
	if bearing {
		writer, ok := st.(store.MigrationTargetReadinessWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadiness(t.Context(), store.TargetStagedReadinessState{
			JobID: 7, MigrationJobID: 7, Armed: true, MinWriteTSExclusive: 100,
			RouteStart: []byte("m"), RouteEnd: []byte("z"),
		}))
	}
	var raw bytes.Buffer
	var snapshot interface {
		io.WriterTo
		io.Closer
	}
	var err error
	if format == "external-native" {
		snapshot, err = st.Snapshot()
	} else {
		snapshot, err = kv.NewKvFSMWithHLC(st, nil).Snapshot()
	}
	require.NoError(t, err)
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	input := filepath.Join(t.TempDir(), "input")
	require.NoError(t, os.WriteFile(input, raw.Bytes(), 0o600))
	dir := filepath.Join(t.TempDir(), "raft")
	peers := []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}}
	if format == "external-native" {
		_, err = PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
			InputFSMPath: input, DataDir: dir, Index: externalDestinationConfigurationIndex, Term: 7, Peers: peers,
		})
	} else {
		_, err = PreparePhysicalSnapshotRestore(PhysicalSnapshotRestoreOptions{
			InputFSMPath: input, DataDir: dir, Index: externalDestinationConfigurationIndex, Term: 7, Peers: peers,
		})
	}
	require.NoError(t, err)
	return dir
}

func externalConfigurationOpen(ctx context.Context, dir string, fsm StateMachine) (*Engine, error) {
	return Open(ctx, OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:12001", DataDir: dir,
		StateMachine: fsm, TickInterval: 10 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1})
}

func assertExternalConfigurationBound(t *testing.T, engine *Engine, st store.MVCCStore) {
	t.Helper()
	index, supported := engine.AppliedConfigurationIndex()
	require.True(t, supported, "destination ConfState must establish its own durable epoch without another ConfChange")
	require.Equal(t, externalDestinationConfigurationIndex, index)
	durable, err := kv.MigrationConfigurationIndex(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, index, durable)
	config, err := engine.Configuration(t.Context())
	require.NoError(t, err)
	require.Len(t, config.Servers, 1)
	require.Equal(t, "n1", config.Servers[0].ID)
}

func TestExternalOriginDestinationConfigurationAndNativeRestart(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "10000")
	for _, format := range []string{"mvcc", "native", "sst", "external-native"} {
		t.Run(format, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			dir := prepareExternalConfigurationFixture(t, format, false)
			for restart := range 3 {
				st := restartOriginStore(t, format)
				fsm := kv.NewKvFSMWithHLC(st, nil, kv.WithRouteHistory(nil, 1))
				engine, err := externalConfigurationOpen(ctx, dir, fsm)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, engine.Close()) })
				assertExternalConfigurationBound(t, engine, st)
				if restart == 1 {
					assertExternalConfigurationDoesNotAuthorizeSource(t, ctx, engine, st)
					snapshot, snapErr := fsm.Snapshot()
					require.NoError(t, snapErr)
					index := engine.appliedIndex.Load()
					require.Greater(t, index, externalDestinationConfigurationIndex)
					require.NoError(t, engine.persistLocalSnapshot(snapshotRequest{index: index, snapshot: snapshot}))
				}
				if restart == 2 {
					require.Greater(t, engine.Status().LastSnapshotIndex, externalDestinationConfigurationIndex,
						"native snapshot data index must not replace the destination's configuration epoch")
					require.ErrorIs(t, kv.CheckMigrationRequest(ctx, st, 1, &pb.Request{Ts: 200,
						ObservedRouteVersion: kv.EncodeObservedRouteVersion(1),
						Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("blocked")}},
					}), kv.ErrRouteCutoverPending)
				}
				require.NoError(t, engine.Close())
			}
		})
	}
}

func assertExternalConfigurationDoesNotAuthorizeSource(t *testing.T, ctx context.Context, engine *Engine, st store.MVCCStore) {
	t.Helper()
	control, err := kv.MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
		JobId: 7, MigrationJobId: 7, Armed: true, TrackWrites: true,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 1,
		MinWriteTsExclusive: 100, RetentionPinTs: 1,
	})
	require.NoError(t, err)
	result, err := engine.Propose(ctx, control)
	require.NoError(t, err)
	require.Nil(t, result.Response)
	require.ErrorIs(t, kv.CheckMigrationRequest(ctx, st, 1, &pb.Request{Ts: 200,
		ObservedRouteVersion: kv.EncodeObservedRouteVersion(1),
		Mutations:            []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("blocked")}},
	}), kv.ErrRouteCutoverPending, "a configuration epoch alone is not SOURCE ownership evidence")
	_, err = st.GetAt(ctx, []byte("n"), 200)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

type externalConfigurationFailureFSM struct {
	StateMachine
	indices []uint64
	failure error
}

func (f *externalConfigurationFailureFSM) RestoreWithContext(ctx context.Context, r io.Reader, opts raftengine.SnapshotRestoreOptions) error {
	restorer, ok := f.StateMachine.(raftengine.ContextSnapshotRestorer)
	if !ok {
		return errors.WithStack(ErrExternalSnapshotRestoreUnsupported)
	}
	return errors.WithStack(restorer.RestoreWithContext(ctx, r, opts))
}

func (f *externalConfigurationFailureFSM) RecordAppliedConfiguration(index uint64) error {
	f.indices = append(f.indices, index)
	return f.failure
}

func (f *externalConfigurationFailureFSM) AppliedConfigurationIndex() uint64 { return 0 }

func TestExternalOriginConfigurationFailureStopsOpen(t *testing.T) {
	for _, bearing := range []bool{false, true} {
		t.Run(map[bool]string{false: "configuration-write", true: "external-validation"}[bearing], func(t *testing.T) {
			dir := prepareExternalConfigurationFixture(t, "native", bearing)
			st := restartOriginStore(t, "native")
			failure := errors.New("destination configuration write failed")
			fsm := &externalConfigurationFailureFSM{StateMachine: kv.NewKvFSMWithHLC(st, nil), failure: failure}
			engine, err := externalConfigurationOpen(t.Context(), dir, fsm)
			if engine != nil {
				t.Cleanup(func() { require.NoError(t, engine.Close()) })
			}
			if bearing {
				require.ErrorIs(t, err, store.ErrExternalSnapshotMigrationState)
				require.Empty(t, fsm.indices, "failed external validation must not bind any destination epoch")
			} else {
				require.ErrorIs(t, err, failure)
				require.Equal(t, []uint64{externalDestinationConfigurationIndex}, fsm.indices)
			}
			require.Nil(t, engine)
			marker, readErr := readExternalRestoreOrigin(dir)
			require.NoError(t, readErr)
			require.False(t, marker.Completed)
		})
	}
}
