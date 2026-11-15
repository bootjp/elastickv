package kv

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type twoGroupNativeEngine struct {
	raftengine.Engine
	f                     *kvFSM
	index                 uint64
	term                  uint64
	lostReply             bool
	transferBeforePropose bool
}

type twoGroupFollowerEngine struct{ raftengine.Engine }

func (twoGroupFollowerEngine) State() raftengine.State { return raftengine.StateFollower }

type twoGroupRemoteTestPeer struct {
	group  uint64
	leader *twoGroupNativeEngine
	lookup TwoGroupHomeLookup
	calls  int
}

func (p *twoGroupRemoteTestPeer) PrepareTwoGroupOnLeader(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest) error {
	p.calls++
	if group != p.group {
		return ErrTwoGroupAdmission
	}
	_, _, err := ProposeTwoGroupPrepareOnLeader(ctx, p.group, manifest, p.leader, p.leader, p.lookup,
		func(context.Context) error { return nil })
	return err
}

func (e *twoGroupNativeEngine) State() raftengine.State            { return raftengine.StateLeader }
func (e *twoGroupNativeEngine) VerifyLeader(context.Context) error { return nil }
func (e *twoGroupNativeEngine) LinearizableRead(context.Context) (uint64, error) {
	return e.index, nil
}
func (e *twoGroupNativeEngine) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateLeader, Term: e.term, AppliedIndex: e.index}
}
func (e *twoGroupNativeEngine) ProposeIfTerm(_ context.Context, term uint64, data []byte) (*raftengine.ProposalResult, error) {
	if e.transferBeforePropose {
		e.term++
		e.transferBeforePropose = false
	}
	if term != e.term {
		return nil, raftengine.ErrStaleAdmissionTerm
	}
	e.index++
	result := e.f.ApplyWithIdentityForTwoGroupTest(e.index, e.term, data)
	if e.lostReply {
		e.lostReply = false
		e.term++
		return nil, errors.New("reply lost after apply")
	}
	return &raftengine.ProposalResult{CommitIndex: e.index, Response: result}, nil
}

func TestTwoGroupCoordinatorSecondaryTransferLeavesNoBinding(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3, transferBeforePropose: true}
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{
		1: {Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) {
		return twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	unresolved, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped,
		nil, 0, nil)
	require.True(t, unresolved)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	id := twoGroupOperationID([]byte("home-key"), 10)
	_, found, err := LookupTwoGroupHomeRecord(t.Context(), homeMetadata, id)
	require.NoError(t, err)
	require.True(t, found)
	_, found, err = LookupTimestampBinding(t.Context(), secondaryMetadata, id)
	require.NoError(t, err)
	require.False(t, found)
}

func TestTwoGroupOptInFencesLegacyDispatchBeforePrepare(t *testing.T) {
	coord := (&ShardedCoordinator{}).WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) {
		t.Fatal("lookup reached from legacy dispatch")
		return nil, nil
	})
	_, err := coord.dispatchMultiShardTxn(t.Context(), 10, 42, 0, []byte("home-key"),
		map[uint64][]*pb.Mutation{1: {}, 2: {}}, []uint64{1, 2}, nil, 0, nil)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
}

func TestTwoGroupPrivatePrepareRequiresOptIn(t *testing.T) {
	coord := &ShardedCoordinator{}
	_, err := coord.prepareTwoGroup(t.Context(), 12, 43, []byte("home-key"),
		map[uint64][]*pb.Mutation{1: {}, 2: {}}, map[uint64][][]byte{1: {[]byte("read-key")}}, 7,
		map[uint64][][]byte{1: {[]byte("bypass-key")}})
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
}

func (f *kvFSM) ApplyWithIdentityForTwoGroupTest(index, term uint64, data []byte) any {
	f.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(index, term, 0, data))
	return f.Apply(data)
}

type twoGroupNativeLookup struct {
	metadata store.RaftControlMetadataStore
	engine   *twoGroupNativeEngine
}

func (l twoGroupNativeLookup) LookupTwoGroupHome(ctx context.Context, id [16]byte) ([]byte, uint64, uint64, error) {
	raw, found, err := LookupTwoGroupHomeRecord(ctx, l.metadata, id)
	if err != nil || !found {
		return nil, 0, 0, errors.Join(ErrTwoGroupAdmission, err)
	}
	return raw, l.engine.index, l.engine.term, nil
}

func (l twoGroupNativeLookup) LookupTwoGroupDecision(ctx context.Context, id [16]byte) ([]byte, uint64, uint64, error) {
	raw, found, err := LookupTwoGroupHomeDecision(ctx, l.metadata, id)
	if err != nil || !found {
		return nil, 0, 0, errors.Join(ErrTransactionOutcomeUnknown, err)
	}
	return raw, l.engine.index, l.engine.term, nil
}

func TestTwoGroupCoordinatorReconcilesLostPrimaryDecisionReply(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{
		1: {Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) {
		return twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	unresolved, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped, nil, 0, nil)
	require.NoError(t, err)
	require.False(t, unresolved)
	manifest, _, err := coord.buildTwoGroupManifest(t.Context(), 10, 42, []byte("home-key"), grouped, []uint64{1, 2}, nil, 0, nil)
	require.NoError(t, err)
	homeEngine.lostReply = true
	require.NoError(t, coord.DecideTwoGroup(t.Context(), manifest, pb.Phase_COMMIT))
	for _, metadata := range []store.RaftControlMetadataStore{homeMetadata, secondaryMetadata} {
		_, err := metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(twoGroupOperationID([]byte("home-key"), 10)))
		require.NoError(t, err)
	}
	require.NoError(t, coord.DecideTwoGroup(t.Context(), manifest, pb.Phase_COMMIT))
}

func TestTwoGroupResolverRoutesThroughTerminalDecision(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	groups := map[uint64]*ShardGroup{
		1: {Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}
	coord := &ShardedCoordinator{groups: groups, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) {
		return twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	_, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped, nil, 0, nil)
	require.NoError(t, err)
	manifest, _, err := coord.buildTwoGroupManifest(t.Context(), 10, 42, []byte("home-key"), grouped, []uint64{1, 2}, nil, 0, nil)
	require.NoError(t, err)
	routes := distribution.NewEngine()
	routes.UpdateRoute([]byte("a"), []byte("m"), 1)
	routes.UpdateRoute([]byte("m"), nil, 2)
	ss := NewShardStore(routes, groups)
	lr := &LockResolver{store: ss, groups: groups}
	require.NoError(t, lr.resolveExpiredLock(t.Context(), groups[2], []byte("secondary-key"),
		txnLock{StartTS: 10, PrimaryKey: []byte("home-key")}))
	id := twoGroupOperationID([]byte("home-key"), 10)
	_, err = secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "unknown home outcome must retain the secondary PREPARE")
	require.NoError(t, coord.proposeTwoGroupTerminal(t.Context(), 1, manifest, pb.Phase_COMMIT, TwoGroupDecisionObservation{}))
	type ownedKey struct{}
	hook := func(ctx context.Context, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		if group != 2 || purpose != AdmissionMaintenance {
			return nil, nil, ErrTwoGroupAdmission
		}
		if ctx.Value(ownedKey{}) == nil {
			ctx = context.WithValue(ctx, ownedKey{}, true)
		}
		return ctx, func(err error) error { return err }, nil
	}
	lr.operationAdmission = hook
	lr.log = slog.Default()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, err := admittedOperation(ctx, hook, 2, AdmissionMaintenance, func(owned context.Context) (struct{}, error) {
			_, resolveErr := lr.resolveScannedLock(owned, 2, groups[2], &store.KVPair{
				Key: txnLockKey([]byte("secondary-key")), Value: encodeTxnLock(txnLock{
					StartTS: 10, PrimaryKey: []byte("home-key"), TTLExpireAt: 1,
				}),
			})
			return struct{}{}, resolveErr
		})
		resultCh <- err
	}()
	select {
	case err := <-resultCh:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("nested lock resolver admission and terminal apply did not complete")
	}
	_, err = secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
	require.NoError(t, err)
	require.NoError(t, ss.resolveTxnLockForKey(t.Context(), groups[2], []byte("secondary-key"),
		txnLock{StartTS: 10, PrimaryKey: []byte("home-key")}))
	status, commitTS, done, err := ss.primaryTxnRecordedStatus(t.Context(), []byte("home-key"), 10)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, txnStatusCommitted, status)
	require.Equal(t, uint64(42), commitTS)
}

func TestTwoGroupRemoteSecondaryLeaderKeepsPreparedLockPending(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	groups := map[uint64]*ShardGroup{
		1: {Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}
	coord := &ShardedCoordinator{groups: groups, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) {
		return twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	_, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped, nil, 0, nil)
	require.NoError(t, err)
	manifest, _, err := coord.buildTwoGroupManifest(t.Context(), 10, 42, []byte("home-key"), grouped, []uint64{1, 2}, nil, 0, nil)
	require.NoError(t, err)
	groups[2].Engine = twoGroupFollowerEngine{secondaryEngine}
	err = coord.DecideTwoGroup(t.Context(), manifest, pb.Phase_COMMIT)
	require.ErrorIs(t, err, raftengine.ErrNotLeader)
	id := twoGroupOperationID([]byte("home-key"), 10)
	_, err = homeMetadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "remote secondary PREPARE must not allow primary COMMIT")
	require.NoError(t, coord.proposeTwoGroupTerminal(t.Context(), 1, manifest, pb.Phase_COMMIT, TwoGroupDecisionObservation{}))
	routes := distribution.NewEngine()
	routes.UpdateRoute([]byte("a"), []byte("m"), 1)
	routes.UpdateRoute([]byte("m"), nil, 2)
	lr := &LockResolver{store: NewShardStore(routes, groups), groups: groups, log: slog.Default()}
	err = lr.resolveExpiredLock(t.Context(), groups[2], []byte("secondary-key"),
		txnLock{StartTS: 10, PrimaryKey: []byte("home-key")})
	require.ErrorIs(t, err, raftengine.ErrNotLeader)
	outcome, err := lr.resolveScannedLock(t.Context(), 2, groups[2], &store.KVPair{
		Key: txnLockKey([]byte("secondary-key")), Value: encodeTxnLock(txnLock{
			StartTS: 10, PrimaryKey: []byte("home-key"), TTLExpireAt: 1,
		}),
	})
	require.NoError(t, err)
	require.Equal(t, lockResolveIgnored, outcome, "remote-only resolution must not count as resolved")
	_, err = secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = secondary.store.GetAt(t.Context(), txnLockKey([]byte("secondary-key")), ^uint64(0))
	require.NoError(t, err, "remote-only terminal path must not report success or clean the lock")
}

func TestTwoGroupExpiredPrimaryAbortWritesBothTerminalRecords(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	groups := map[uint64]*ShardGroup{
		1: {Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}
	coord := &ShardedCoordinator{groups: groups, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) {
		return twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	_, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped, nil, 0, nil)
	require.NoError(t, err)
	routes := distribution.NewEngine()
	routes.UpdateRoute([]byte("a"), []byte("m"), 1)
	routes.UpdateRoute([]byte("m"), nil, 2)
	ss := NewShardStore(routes, groups)
	aborted, err := ss.tryAbortExpiredPrimary(t.Context(), []byte("home-key"), 10)
	require.NoError(t, err)
	require.True(t, aborted)
	id := twoGroupOperationID([]byte("home-key"), 10)
	for _, metadata := range []store.RaftControlMetadataStore{homeMetadata, secondaryMetadata} {
		raw, err := metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
		require.NoError(t, err)
		record, err := parseTwoGroupTerminalRecord(raw)
		require.NoError(t, err)
		require.Equal(t, pb.Phase_ABORT, record.GetDecision())
	}
	status, _, done, err := ss.primaryTxnRecordedStatus(t.Context(), []byte("home-key"), 10)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, txnStatusRolledBack, status)
}

type twoGroupTestAllocator struct{}

func (twoGroupTestAllocator) Next(context.Context) (uint64, error) { return 100, nil }
func (twoGroupTestAllocator) NextAfter(_ context.Context, min uint64) (uint64, error) {
	return min + 1, nil
}
func (twoGroupTestAllocator) ValidateDurableTimestamp(context.Context, uint64) error { return nil }

func TestTwoGroupCoordinatorProposesHomeBeforeSecondaryAfterLostReply(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2, lostReply: true}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{
		1: {Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(gid uint64) (TwoGroupHomeLookup, error) {
		require.Equal(t, uint64(1), gid)
		// The lookup observes the term after the lost-reply transfer.
		return twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	unresolved, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped,
		nil, 0, nil)
	require.NoError(t, err)
	require.False(t, unresolved)
	id := twoGroupOperationID([]byte("home-key"), 10)
	_, found, err := LookupTwoGroupHomeRecord(t.Context(), homeMetadata, id)
	require.NoError(t, err)
	require.True(t, found)
	_, found, err = LookupTimestampBinding(t.Context(), secondaryMetadata, id)
	require.NoError(t, err)
	require.True(t, found)
}

func TestTwoGroupRemoteLeaderReobservesAppliedHome(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2, lostReply: true}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	remote := &twoGroupRemoteTestPeer{group: 2, leader: secondaryEngine,
		lookup: twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}}
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{
		1: {Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: twoGroupFollowerEngine{}, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) { return remote.lookup, nil })
	coord.WithTwoGroupRemotePrepare(func(gid uint64) (TwoGroupRemotePreparer, error) {
		require.Equal(t, uint64(2), gid)
		return remote, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	unresolved, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped, nil, 0, nil)
	require.NoError(t, err)
	require.False(t, unresolved)
	require.Equal(t, 1, remote.calls)
	id := twoGroupOperationID([]byte("home-key"), 10)
	_, found, err := LookupTimestampBinding(t.Context(), secondaryMetadata, id)
	require.NoError(t, err)
	require.True(t, found)
}

func TestTwoGroupRemotePrimaryLostReplyReconcilesBeforeSecondary(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2, lostReply: true}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	lookup := twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}
	remote := &twoGroupRemoteTestPeer{group: 1, leader: homeEngine}
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{
		1: {Engine: twoGroupFollowerEngine{}, Store: home.store, Txn: nilDecisionTransactional{}},
		2: {Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}},
	}, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) { return lookup, nil })
	coord.WithTwoGroupRemotePrepare(func(gid uint64) (TwoGroupRemotePreparer, error) {
		require.Equal(t, uint64(1), gid)
		return remote, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	unresolved, err := coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped, nil, 0, nil)
	require.NoError(t, err)
	require.False(t, unresolved)
	require.Equal(t, 1, remote.calls)
	id := twoGroupOperationID([]byte("home-key"), 10)
	_, found, err := LookupTimestampBinding(t.Context(), secondaryMetadata, id)
	require.NoError(t, err)
	require.True(t, found)
}

func TestTwoGroupRemoteLeaderRejectsBeforeHomeAndTransfer(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	lookup := twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}
	manifest := twoGroupPrepareFixture()
	_, _, err := ProposeTwoGroupPrepareOnLeader(t.Context(), 2, manifest, secondaryEngine, secondaryEngine,
		lookup, func(context.Context) error { return nil })
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	_, found, err := LookupTimestampBinding(t.Context(), secondaryMetadata, [16]byte{1})
	require.NoError(t, err)
	require.False(t, found)
	_, _, err = ProposeTwoGroupPrepareOnLeader(t.Context(), 1, manifest, homeEngine, homeEngine,
		nil, func(context.Context) error { return nil })
	require.NoError(t, err)
	changed := proto.CloneOf(manifest)
	changed.Participants[1].Prepare.Mutations[1].Value = []byte("changed")
	_, _, err = ProposeTwoGroupPrepareOnLeader(t.Context(), 2, changed, secondaryEngine, secondaryEngine,
		lookup, func(context.Context) error { return nil })
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	_, _, err = ProposeTwoGroupPrepareOnLeader(t.Context(), 2, manifest, secondaryEngine, secondaryEngine,
		lookup, func(context.Context) error { return errors.New("session revoked") })
	require.ErrorContains(t, err, "session revoked")
	secondaryEngine.transferBeforePropose = true
	_, _, err = ProposeTwoGroupPrepareOnLeader(t.Context(), 2, manifest, secondaryEngine, secondaryEngine,
		lookup, func(context.Context) error { return nil })
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	_, found, err = LookupTimestampBinding(t.Context(), secondaryMetadata, [16]byte{1})
	require.NoError(t, err)
	require.False(t, found)
}
