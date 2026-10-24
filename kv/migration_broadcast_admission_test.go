package kv

import (
	"context"
	"crypto/sha256"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type broadcastCommitRecorder struct {
	inner   Transactional
	commits atomic.Int32
}

func (r *broadcastCommitRecorder) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	r.commits.Add(1)
	return r.inner.Commit(ctx, reqs)
}

func (r *broadcastCommitRecorder) Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	return r.inner.Abort(ctx, reqs)
}

func realPrefixBroadcastFixture(t *testing.T, retiredSource bool) (*ShardedCoordinator, map[uint64]*broadcastCommitRecorder) {
	t.Helper()
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, End: []byte("p"), GroupID: 11, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("p"), End: []byte("q"), GroupID: 22, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
		{RouteID: 3, Start: []byte("q"), GroupID: 33, State: distribution.RouteStateActive},
	}
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 7, Routes: routes}))
	groups := make(map[uint64]*ShardGroup)
	recorders := make(map[uint64]*broadcastCommitRecorder)
	for _, gid := range []uint64{11, 22, 33} {
		st := store.NewMVCCStore()
		f, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(WrapDistributionEngine(engine), gid)).(*kvFSM)
		require.True(t, ok)
		raft, stop := newSingleRaft(t, "prefix-"+strconv.FormatUint(gid, 10), f)
		t.Cleanup(stop)
		if gid == 22 || (gid == 11 && retiredSource) {
			installBroadcastOwnership(t, f, raft, routes[1], gid)
		}
		tx := NewLeaderProxyWithEngine(raft, WithMigrationRequestAdmission(st, gid))
		t.Cleanup(tx.tm.Close)
		recorder := &broadcastCommitRecorder{inner: tx}
		recorders[gid] = recorder
		groups[gid] = &ShardGroup{Engine: raft, Store: st, Txn: recorder}
	}
	for gid, key := range map[uint64]string{11: "a", 22: "pa", 33: "z"} {
		require.NoError(t, groups[gid].Store.PutAt(t.Context(), []byte(key), []byte(key), 120, 0))
	}
	shards := NewShardStore(engine, groups)
	c := NewShardedCoordinator(engine, groups, 11, NewHLC(), shards).WithAllShardGroups(33, 11, 22)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c, recorders
}

func installBroadcastOwnership(t *testing.T, f *kvFSM, proposer raftengine.Proposer, route distribution.RouteDescriptor, gid uint64) {
	t.Helper()
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	role := pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER
	if gid != route.GroupID {
		role = pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_NONOWNER
	}
	command, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 7,
		Mode:                      pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: f.AppliedConfigurationIndex(),
		Ownership: &pb.MigrationOwnershipProof{Generation: "0123456789abcdef0123456789abcdef",
			LocalGroupId: gid, OwnerGroupId: route.GroupID, CatalogVersion: 7, RouteId: route.RouteID,
			RouteStart: route.Start, RouteEnd: route.End, MinWriteTsExclusive: 100,
			WriteFloorKnown: true, DescriptorHash: digest[:], Role: role}})
	require.NoError(t, err)
	result, err := proposer.ProposeAdmin(t.Context(), command)
	require.NoError(t, err)
	response, ok := result.Response.(store.CleanupVersionsResult)
	require.True(t, ok, "%v", result.Response)
	require.True(t, response.Done)
}

func TestMigrationBroadcastRefusesBeforeAnyParticipantCommit(t *testing.T) {
	c, recorders := realPrefixBroadcastFixture(t, true)
	_, err := c.Dispatch(t.Context(), &OperationGroup[OP]{ObservedRouteVersion: 7,
		Elems: []*Elem[OP]{{Op: DelPrefix}}})
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	assertBroadcastUnchanged(t, c, recorders)
}

func assertBroadcastUnchanged(t *testing.T, c *ShardedCoordinator, recorders map[uint64]*broadcastCommitRecorder) {
	t.Helper()
	for _, gid := range []uint64{11, 22, 33} {
		assert.Zero(t, recorders[gid].commits.Load(), "group %d must not receive Commit", gid)
	}
	for gid, key := range map[uint64]string{11: "a", 22: "pa", 33: "z"} {
		value, getErr := c.groups[gid].Store.GetAt(t.Context(), []byte(key), ^uint64(0))
		require.NoError(t, getErr, "group %d must remain unchanged", gid)
		require.Equal(t, []byte(key), value)
	}
}

func TestMigrationBroadcastPreservesObservedVersionWithoutRepinning(t *testing.T) {
	for _, version := range []uint64{0, 6, 8} {
		t.Run(strconv.FormatUint(version, 10), func(t *testing.T) {
			c, recorders := realPrefixBroadcastFixture(t, false)
			_, err := c.Dispatch(t.Context(), &OperationGroup[OP]{ObservedRouteVersion: version,
				Elems: []*Elem[OP]{{Op: DelPrefix, Key: []byte("p")}}})
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			assertBroadcastUnchanged(t, c, recorders)
		})
	}
}

func TestMigrationBroadcastTimestampSealAndParticipantSelection(t *testing.T) {
	c, recorders := realPrefixBroadcastFixture(t, false)
	for _, command := range [][]byte{EncodePrepareEvidenceFence(0), EncodePrepareEvidenceSeal()} {
		result, err := c.groups[33].Engine.ProposeAdmin(t.Context(), command)
		require.NoError(t, err)
		require.Nil(t, result.Response)
	}
	_, err := c.Dispatch(t.Context(), &OperationGroup[OP]{ObservedRouteVersion: 7,
		Elems: []*Elem[OP]{{Op: DelPrefix, Key: []byte("p")}}})
	require.ErrorIs(t, err, ErrPrepareCreationSealed)
	assertBroadcastUnchanged(t, c, recorders)
	// Explicit participant selection must not probe or commit the excluded
	// group's timestamp gate, nor substitute the slice index for its group ID.
	c.WithAllShardGroups(22, 11)
	_, err = c.Dispatch(t.Context(), &OperationGroup[OP]{ObservedRouteVersion: 7,
		Elems: []*Elem[OP]{{Op: DelPrefix, Key: []byte("p")}}})
	require.NoError(t, err)
	require.Equal(t, int32(1), recorders[11].commits.Load())
	require.Equal(t, int32(1), recorders[22].commits.Load())
	require.Zero(t, recorders[33].commits.Load())
}

func TestMigrationBroadcastFullyAdmittedPrefixCommitsAllParticipants(t *testing.T) {
	c, recorders := realPrefixBroadcastFixture(t, false)
	_, err := c.Dispatch(t.Context(), &OperationGroup[OP]{ObservedRouteVersion: 7,
		Elems: []*Elem[OP]{{Op: DelPrefix, Key: []byte("p")}}})
	require.NoError(t, err)
	for _, gid := range []uint64{11, 22, 33} {
		require.Equal(t, int32(1), recorders[gid].commits.Load(), "group %d", gid)
	}
	_, err = c.groups[22].Store.GetAt(t.Context(), []byte("pa"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	for gid, key := range map[uint64]string{11: "a", 33: "z"} {
		value, getErr := c.groups[gid].Store.GetAt(t.Context(), []byte(key), ^uint64(0))
		require.NoError(t, getErr)
		require.Equal(t, []byte(key), value)
	}
}
