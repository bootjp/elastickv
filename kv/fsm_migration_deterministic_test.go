package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type forbiddenApplyWatcher struct{}

func (forbiddenApplyWatcher) Current() (RouteSnapshot, bool) {
	panic("migration apply consulted the catalog watcher")
}

func (forbiddenApplyWatcher) SnapshotAt(uint64) (RouteSnapshot, bool) {
	panic("migration apply consulted watcher history")
}

func deterministicMigrationGuard(t *testing.T, st store.MVCCStore, source bool) {
	t.Helper()
	writer, ok := st.(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), store.TargetStagedReadinessState{
		JobID: 7, MigrationJobID: 7, Armed: true, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		ExpectedCutoverVersion: 10, MinWriteTSExclusive: 100, TrackWrites: source, RetentionPinTS: 1,
	}, 8))
}

func deterministicMigrationOwner(t *testing.T, st store.MVCCStore) {
	t.Helper()
	deterministicMigrationGuard(t, st, false)
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	require.NoError(t, writer.AcquireMigrationOwnership(t.Context(), store.MigrationOwnershipState{
		Generation: "0123456789abcdef0123456789abcdef", RouteID: 3, DescriptorHash: make([]byte, 32),
		JobID: 7, LocalGroupID: 1, OwnerGroupID: 1, Role: store.MigrationOwner,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), CatalogVersion: 10,
		ConfigurationIndex: 3, MinWriteTSExclusive: 100, Staged: true,
	}, 9))
}

func deterministicRawRequest(version uint64, keys ...string) *pb.Request {
	r := &pb.Request{Ts: 200, ObservedRouteVersion: version}
	for _, key := range keys {
		r.Mutations = append(r.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("value")})
	}
	return r
}

func TestDeterministicMigrationApplyRequiresCompleteProof(t *testing.T) {
	for _, kind := range []string{"target-guard", "source-tracker", "future-owner", "mixed-owner", "exact-owner"} {
		t.Run(kind, func(t *testing.T) {
			st := store.NewMVCCStore()
			f := &kvFSM{store: st, routes: forbiddenApplyWatcher{}, shardGroupID: 1}
			r := deterministicRawRequest(10, "n")
			switch kind {
			case "target-guard", "source-tracker":
				deterministicMigrationGuard(t, st, kind == "source-tracker")
			default:
				deterministicMigrationOwner(t, st)
			}
			if kind == "future-owner" {
				r.ObservedRouteVersion = 11
			}
			if kind == "mixed-owner" {
				r = deterministicRawRequest(10, "b", "n")
			}
			err := f.applyRequestErr(t.Context(), r)
			if kind == "exact-owner" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			require.NoError(t, haltApplyOf(applyErrorResponse(err)))
			exists, err := st.ExistsAt(t.Context(), []byte("b"), 200)
			require.NoError(t, err)
			require.False(t, exists)
			exists, err = st.ExistsAt(t.Context(), []byte("n"), 200)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestDeterministicMigrationUnaffectedLegacyRequest(t *testing.T) {
	st := store.NewMVCCStore()
	deterministicMigrationOwner(t, st)
	e := distribution.NewEngine()
	applyComposed1Snapshot(t, e, 10, []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive}})
	f := &kvFSM{store: st, routes: WrapDistributionEngine(e), shardGroupID: 1}
	require.NoError(t, f.applyRequestErr(t.Context(), deterministicRawRequest(0, "b")))
}

func TestCheckMigrationRequestIsReadOnlyAndAdvisory(t *testing.T) {
	st := store.NewMVCCStore()
	deterministicMigrationOwner(t, st)
	r := deterministicRawRequest(10, "n")
	require.NoError(t, CheckMigrationRequest(t.Context(), st, 1, r))
	exists, err := st.ExistsAt(t.Context(), []byte("n"), 200)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, CheckMigrationRequest(t.Context(), st, 1, deterministicRawRequest(0, "b")))
	r.ObservedRouteVersion = 11
	f := &kvFSM{store: st, routes: forbiddenApplyWatcher{}, shardGroupID: 1}
	require.ErrorIs(t, f.applyRequestErr(t.Context(), r), ErrRouteCutoverPending)
	require.ErrorIs(t, CheckMigrationRequest(t.Context(), st, 1, r), ErrRouteCutoverPending)
	exists, err = st.ExistsAt(t.Context(), []byte("n"), 200)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDeterministicMigrationResolutionDrainsAndRetries(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			f := &kvFSM{store: store.NewMVCCStore(), shardGroupID: 1}
			r := applyRouteContextTxn(pb.Phase_PREPARE, 20, 60, []byte("n"))
			r.ObservedRouteVersion = 0
			require.NoError(t, f.applyRequestErr(t.Context(), r))
			retireServingSource(t, f.store)
			f.routes = forbiddenApplyWatcher{}
			r.Phase = phase
			r.ObservedRouteVersion = 999
			require.NoError(t, f.applyRequestErr(t.Context(), r))
			require.NoError(t, f.applyRequestErr(t.Context(), r), "idempotent resolution cannot require fresh ownership")
			value, err := f.store.GetAt(t.Context(), []byte("n"), 200)
			if phase == pb.Phase_COMMIT {
				require.NoError(t, err)
				require.Equal(t, []byte("new"), value)
			} else {
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
		})
	}
}

func TestDeterministicMigrationRemotePrimaryAndStagedReadAlias(t *testing.T) {
	st := store.NewMVCCStore()
	deterministicMigrationOwner(t, st)
	f := &kvFSM{store: st, routes: forbiddenApplyWatcher{}, shardGroupID: 1}
	r := applyRouteContextTxn(pb.Phase_PREPARE, 150, 200, []byte("n"))
	r.ObservedRouteVersion = 10
	r.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b-remote"), LockTTLms: 1000, CommitTS: 200})
	r.ReadKeys = [][]byte{distribution.MigrationStagedDataKey(7, []byte("n"))}
	require.NoError(t, f.applyRequestErr(t.Context(), r), "remote primary is not part of this data group's footprint")
	r.Phase = pb.Phase_COMMIT
	r.ObservedRouteVersion = 999
	require.NoError(t, f.applyRequestErr(t.Context(), r))
	require.NoError(t, f.applyRequestErr(t.Context(), r), "resolved secondary is a no-op without consulting remote ownership")
	_, err := st.GetAt(t.Context(), txnCommitKey([]byte("b-remote"), 150), 200)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestDeterministicMigrationResolutionRejectsUnpreparedAndMismatchedKeys(t *testing.T) {
	for _, mismatch := range []string{"start", "primary", "chosen", "missing-primary", "extra-key"} {
		t.Run(mismatch, func(t *testing.T) {
			f := &kvFSM{store: store.NewMVCCStore(), shardGroupID: 1}
			r := applyRouteContextTxn(pb.Phase_PREPARE, 20, 60, []byte("n"))
			r.ObservedRouteVersion = 0
			r.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("n"), PrepareCommitTS: 60})
			// Install the same durable V3 lock as an already upgraded PREPARE;
			// this fixture does not activate the separate Phase-D protocol.
			lock := txnLock{StartTS: 20, CommitTS: 60, PrimaryKey: []byte("n"), IsPrimaryKey: true}
			require.NoError(t, f.store.PutAt(t.Context(), txnLockKey([]byte("n")), encodeTxnLock(lock), 20, 0))
			require.NoError(t, f.store.PutAt(t.Context(), txnIntentKey([]byte("n")), encodeTxnIntent(txnIntent{StartTS: 20, Value: []byte("prepared")}), 20, 0))
			retireServingSource(t, f.store)
			f.routes = forbiddenApplyWatcher{}
			r.Phase = pb.Phase_COMMIT
			r.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("n"), CommitTS: 60})
			switch mismatch {
			case "start":
				r.Ts++
			case "primary":
				r.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("other"), CommitTS: 60})
			case "chosen":
				r.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("n"), CommitTS: 61})
			case "missing-primary":
				r.Mutations[1].Key = []byte("o")
				r.Mutations[0].Value = EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("o"), CommitTS: 60})
			case "extra-key":
				r.Mutations = append(r.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: []byte("o"), Value: []byte("forged")})
			}
			err := f.applyRequestErr(t.Context(), r)
			switch mismatch {
			case "extra-key":
				require.NoError(t, err, "missing secondary cannot materialize caller-supplied data")
			case "chosen":
				require.ErrorIs(t, err, ErrTxnInvalidMeta)
			default:
				require.ErrorIs(t, err, ErrRouteCutoverPending)
			}
			_, err = f.store.GetAt(t.Context(), []byte("o"), 200)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestDeterministicMigrationCoalescedContextIsPerRequest(t *testing.T) {
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	deterministicMigrationOwner(t, st)
	e := distribution.NewEngine()
	applyComposed1Snapshot(t, e, 999, []distribution.RouteDescriptor{{
		RouteID: 1, GroupID: 1, State: distribution.RouteStateWriteFenced,
	}})
	f := &kvFSM{store: st, routes: WrapDistributionEngine(e), shardGroupID: 1, pendingApplyIdx: 20}
	requests := []*pb.Request{
		deterministicRawRequest(10, "n"),
		deterministicRawRequest(0, "b"),
		deterministicRawRequest(11, "o"),
		deterministicRawRequest(10, "p"),
	}
	requests[3].Ts = 203
	data, err := proto.Marshal(&pb.RaftCommand{Requests: requests})
	require.NoError(t, err)
	response := f.Apply(data)
	require.NoError(t, haltApplyOf(response))
	results, ok := response.(*fsmApplyResponse)
	require.True(t, ok)
	require.NoError(t, results.results[0])
	require.ErrorIs(t, results.results[1], ErrRouteWriteFenced, "legacy request cannot inherit the preceding ownership context")
	require.ErrorIs(t, results.results[2], ErrRouteCutoverPending)
	require.NoError(t, results.results[3])
	for _, key := range []string{"n", "p"} {
		exists, err := st.ExistsAt(t.Context(), []byte(key), 300)
		require.NoError(t, err)
		require.True(t, exists)
	}
	for _, key := range []string{"b", "o"} {
		exists, err := st.ExistsAt(t.Context(), []byte(key), 300)
		require.NoError(t, err)
		require.False(t, exists)
	}
}
