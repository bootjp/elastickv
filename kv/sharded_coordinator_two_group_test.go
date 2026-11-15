package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type twoGroupNativeEngine struct {
	raftengine.Engine
	f                     *kvFSM
	index                 uint64
	term                  uint64
	lostReply             bool
	transferBeforePropose bool
}

func (e *twoGroupNativeEngine) State() raftengine.State            { return raftengine.StateLeader }
func (e *twoGroupNativeEngine) VerifyLeader(context.Context) error { return nil }
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
	unresolved, err := coord.PrepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped,
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
	unresolved, err := coord.PrepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), grouped,
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
