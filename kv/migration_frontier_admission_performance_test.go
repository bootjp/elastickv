package kv

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type frontierAdmissionCountingStore struct {
	store.MVCCStore
	store.MigrationOwnershipStore
	admissionReads atomic.Int64
	ledgerReads    atomic.Int64
}

func (s *frontierAdmissionCountingStore) MigrationServingState(ctx context.Context) (store.MigrationServingState, error) {
	s.ledgerReads.Add(1)
	return s.MigrationOwnershipStore.MigrationServingState(ctx)
}

func (s *frontierAdmissionCountingStore) MigrationServingAdmissionState(ctx context.Context) (store.MigrationServingState, error) {
	s.admissionReads.Add(1)
	reader, ok := s.MVCCStore.(store.MigrationServingAdmissionReader)
	if !ok {
		return store.MigrationServingState{}, store.ErrNotSupported
	}
	return reader.MigrationServingAdmissionState(ctx)
}

func (s *frontierAdmissionCountingStore) VersionExistsAtOrBefore(ctx context.Context, key []byte, ts uint64) (bool, error) {
	reader, ok := s.MVCCStore.(store.VersionPresenceReader)
	if !ok {
		return false, store.ErrNotSupported
	}
	return reader.VersionExistsAtOrBefore(ctx, key, ts)
}

func TestMigrationFrontierAdmissionOrdinaryReadsOnlyInspectSelectedStore(t *testing.T) {
	for _, backend := range []string{"memory", "pebble"} {
		t.Run(backend, func(t *testing.T) { frontierAdmissionOrdinaryReadSuite(t, backend) })
	}
}

func frontierAdmissionOrdinaryReadSuite(t *testing.T, backend string) {
	t.Helper()
	for _, operation := range []string{"get", "scan", "keys", "reverse", "latest", "exists", "batch-exists", "committed"} {
		t.Run(operation, func(t *testing.T) {
			engine := distribution.NewEngine()
			applyComposed1Snapshot(t, engine, 10, []distribution.RouteDescriptor{
				{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1},
				{RouteID: 2, Start: []byte("m"), End: []byte("z"), GroupID: 2},
			})
			groups := make(map[uint64]*ShardGroup)
			stores := make([]*frontierAdmissionCountingStore, 0, 2)
			for _, id := range []uint64{1, 2} {
				st := store.NewMVCCStore()
				if backend == "pebble" {
					var err error
					st, err = store.NewPebbleStore(t.TempDir())
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, st.Close()) })
				}
				ownership, ok := st.(store.MigrationOwnershipStore)
				require.True(t, ok)
				counting := &frontierAdmissionCountingStore{MVCCStore: st, MigrationOwnershipStore: ownership}
				groups[id] = &ShardGroup{Store: counting}
				stores = append(stores, counting)
			}
			require.NoError(t, stores[0].PutAt(t.Context(), []byte("b"), []byte("value"), 5, 0))
			shards := NewShardStore(engine, groups)
			frontierAdmissionRead(t, shards, operation)
			require.Zero(t, stores[0].ledgerReads.Load(), "the legacy reader may take Pebble applyMu and DB locks")
			require.Positive(t, stores[0].admissionReads.Load(), "the selected store must still prove admission")
			require.Zero(t, stores[1].ledgerReads.Load(), "unselected shard must not be visited")
			require.Zero(t, stores[1].admissionReads.Load(), "common reads must not scale with all groups")
		})
	}
}

func frontierAdmissionRead(t *testing.T, shards *ShardStore, operation string) {
	t.Helper()
	ctx := t.Context()
	key, end := []byte("b"), []byte("c")
	switch operation {
	case "get":
		value, err := shards.GetAt(ctx, key, 20)
		require.NoError(t, err)
		require.Equal(t, []byte("value"), value)
	case "scan":
		rows, err := shards.ScanAt(ctx, key, end, 10, 20)
		require.NoError(t, err)
		require.Len(t, rows, 1)
	case "keys":
		keys, err := shards.ScanKeysAt(ctx, key, end, 10, 20)
		require.NoError(t, err)
		require.Equal(t, [][]byte{key}, keys)
	case "reverse":
		rows, err := shards.ReverseScanAt(ctx, key, end, 10, 20)
		require.NoError(t, err)
		require.Len(t, rows, 1)
	case "latest":
		ts, found, err := shards.LatestCommitTS(ctx, key)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 5, ts)
	case "exists":
		found, answered, err := shards.VersionExistsAtOrBeforeGroupWithReadFence(ctx, key, 1, 20, 0)
		require.NoError(t, err)
		require.True(t, answered)
		require.True(t, found)
	case "batch-exists":
		found, answered, err := shards.VersionsExistAtOrBeforeGroupWithReadFence(ctx, [][]byte{key}, 1, 20, 0)
		require.NoError(t, err)
		require.True(t, answered)
		require.Equal(t, []bool{true}, found)
	case "committed":
		found, err := shards.CommittedVersionAt(ctx, key, 5)
		require.NoError(t, err)
		require.True(t, found)
	default:
		t.Fatalf("unknown read %q", operation)
	}
}
