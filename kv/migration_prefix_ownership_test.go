package kv

import (
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func prefixOwnershipFixture(t *testing.T, defect string) (*ShardStore, *kvFSM) {
	t.Helper()
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, End: []byte("pm"), GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
		{RouteID: 2, Start: []byte("pm"), GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
	}
	if defect == "catalog-hole" {
		routes[0].End = []byte("pl")
	}
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 7, Routes: routes}))
	st := store.NewMVCCStore()
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	for i, route := range routes {
		if i == 0 && defect == "legacy-neighbor" {
			continue
		}
		encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
		require.NoError(t, err)
		digest := sha256.Sum256(encoded)
		proof := store.MigrationOwnershipState{Generation: "0123456789abcdef0123456789abcdef",
			JobID: route.RouteID + 10, LocalGroupID: 1, OwnerGroupID: 1, Role: store.MigrationOwner,
			RouteID: route.RouteID, DescriptorHash: digest[:], RouteStart: route.Start, RouteEnd: route.End,
			CatalogVersion: 7, ConfigurationIndex: 1, MinWriteTSExclusive: 100, WriteFloorKnown: true}
		if i == 1 {
			alterPrefixOwnershipProof(&proof, defect)
		}
		require.NoError(t, writer.ClearMigrationStateWithOwnership(t.Context(), proof, route.RouteID+1))
	}
	for _, key := range []string{"pa", "pz", "z"} {
		require.NoError(t, st.PutAt(t.Context(), []byte(key), []byte(key), 120, 0))
	}
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: st}})
	return shards, &kvFSM{store: st, shardGroupID: 1, routes: forbiddenApplyWatcher{}}
}

func alterPrefixOwnershipProof(proof *store.MigrationOwnershipState, defect string) {
	switch defect {
	case "proof-hole":
		proof.RouteStart = []byte("pn")
	case "nonowner":
		proof.OwnerGroupID, proof.Role = 2, store.MigrationNonOwner
	case "wrong-floor":
		proof.MinWriteTSExclusive = 150
	case "future-owner":
		proof.CatalogVersion = 8
	}
}

func prefixOwnershipRequest(prefix []byte, ts uint64) *pb.Request {
	return &pb.Request{Ts: ts, ObservedRouteVersion: 7,
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: prefix}}}
}

func assertPrefixOwnershipValues(t *testing.T, st store.MVCCStore, prefix []byte, deleted bool) {
	t.Helper()
	for _, key := range []string{"pa", "pz", "z"} {
		value, err := st.GetAt(t.Context(), []byte(key), ^uint64(0))
		if deleted && (len(prefix) == 0 || key != "z") {
			require.ErrorIs(t, err, store.ErrKeyNotFound, key)
		} else {
			require.NoError(t, err, key)
			require.Equal(t, []byte(key), value)
		}
	}
}

func TestMigrationPrefixOwnershipPublicDeletesAcrossRoutes(t *testing.T) {
	for _, defect := range []string{"", "legacy-neighbor"} {
		assertPrefixOwnershipPublicDeletes(t, defect)
	}
}

func assertPrefixOwnershipPublicDeletes(t *testing.T, defect string) {
	t.Helper()
	for _, prefix := range []string{"p", ""} {
		for _, mode := range []string{"normal", "raft", "raft-index", "batch"} {
			t.Run(defect+"/"+mode+"/"+prefix, func(t *testing.T) {
				shards, f := prefixOwnershipFixture(t, defect)
				require.NoError(t, deletePrefixOwnershipFixture(t, shards, []byte(prefix), mode, 200))
				assertPrefixOwnershipValues(t, f.store, []byte(prefix), true)
			})
		}
	}
}

func deletePrefixOwnershipFixture(t *testing.T, s *ShardStore, prefix []byte, mode string, ts uint64) error {
	t.Helper()
	switch mode {
	case "normal":
		return s.DeletePrefixAt(t.Context(), prefix, nil, ts)
	case "raft":
		return s.DeletePrefixAtRaft(t.Context(), prefix, nil, ts)
	case "raft-index":
		return s.DeletePrefixAtRaftAt(t.Context(), prefix, nil, ts, 20)
	default:
		return s.DeletePrefixesAtRaftAt(t.Context(), []store.PrefixDelete{{Prefix: prefix}}, ts, 20)
	}
}

func TestMigrationPrefixOwnershipFSMAndCoordinatorAcrossRoutes(t *testing.T) {
	for _, prefix := range []string{"p", ""} {
		t.Run("FSM/"+prefix, func(t *testing.T) {
			_, f := prefixOwnershipFixture(t, "")
			before, err := readMigrationServingState(t.Context(), f.store)
			require.NoError(t, err)
			f.SetApplyIndex(20)
			require.NoError(t, applyFSMRequest(t, f, prefixOwnershipRequest([]byte(prefix), 200)))
			assertPrefixOwnershipValues(t, f.store, []byte(prefix), true)
			after, err := readMigrationServingState(t.Context(), f.store)
			require.NoError(t, err)
			require.Equal(t, before, after, "range partitioning must not clip or rewrite ownership authority")
		})
		t.Run("coordinator/"+prefix, func(t *testing.T) {
			shards, f := prefixOwnershipFixture(t, "")
			tx := NewTransactionWithProposer(&migrationAdmissionProposer{fsm: f})
			t.Cleanup(tx.Close)
			shards.groups[1].Txn = tx
			coordinator := NewShardedCoordinator(shards.engine, shards.groups, 1, NewHLC(), shards)
			t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
			_, err := coordinator.Dispatch(t.Context(), &OperationGroup[OP]{ObservedRouteVersion: 7,
				Elems: []*Elem[OP]{{Op: DelPrefix, Key: []byte(prefix)}}})
			require.NoError(t, err)
			assertPrefixOwnershipValues(t, f.store, []byte(prefix), true)
		})
	}
}

func TestMigrationPrefixOwnershipFSMDoesNotInventLegacyNeighborAuthority(t *testing.T) {
	_, f := prefixOwnershipFixture(t, "legacy-neighbor")
	f.SetApplyIndex(20)
	require.ErrorIs(t, applyFSMRequest(t, f, prefixOwnershipRequest(nil, 200)), ErrRouteCutoverPending)
	assertPrefixOwnershipValues(t, f.store, nil, false)
}

func TestMigrationPrefixOwnershipRejectsIncompleteAuthorityBeforeEffects(t *testing.T) {
	for _, defect := range []string{"proof-hole", "catalog-hole", "nonowner", "future-owner", "wrong-floor", "low-ts"} {
		t.Run(defect, func(t *testing.T) {
			for _, prefix := range []string{"p", ""} {
				shards, f := prefixOwnershipFixture(t, defect)
				ts := uint64(200)
				if defect == "low-ts" {
					ts = 100
				}
				expected := ErrRouteCutoverPending
				if defect == "low-ts" {
					expected = ErrRouteWriteTimestampTooLow
				}
				require.ErrorIs(t, deletePrefixOwnershipFixture(t, shards, []byte(prefix), "normal", ts), expected)
				assertPrefixOwnershipValues(t, f.store, []byte(prefix), false)
				f.SetApplyIndex(20)
				if defect == "wrong-floor" {
					// A durable floor of 150 is valid authority at 200; the
					// public cache's smaller floor must not authorize at 150.
					ts = 150
				}
				if defect == "wrong-floor" {
					expected = ErrRouteWriteTimestampTooLow
				}
				require.ErrorIs(t, applyFSMRequest(t, f, prefixOwnershipRequest([]byte(prefix), ts)), expected)
				assertPrefixOwnershipValues(t, f.store, []byte(prefix), false)
			}
		})
	}
}
