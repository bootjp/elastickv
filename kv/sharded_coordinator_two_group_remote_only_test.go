package kv

import (
	"context"
	"errors"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type remoteOnlyTwoGroupFixture struct {
	coord             *ShardedCoordinator
	homeEngine        *twoGroupNativeEngine
	secondaryEngine   *twoGroupNativeEngine
	homeMetadata      store.RaftControlMetadataStore
	secondaryMetadata store.RaftControlMetadataStore
	remoteTerminal    *twoGroupRemoteTerminalTestPeer
	grouped           map[uint64][]*pb.Mutation
}

func newRemoteOnlyTwoGroupFixture(t *testing.T, remoteGroup uint64) remoteOnlyTwoGroupFixture {
	t.Helper()
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	homeEngine := &twoGroupNativeEngine{f: home, index: 1, term: 2}
	secondaryEngine := &twoGroupNativeEngine{f: secondary, index: 1, term: 3}
	groups := map[uint64]*ShardGroup{}
	if remoteGroup == 1 {
		groups[2] = &ShardGroup{Engine: secondaryEngine, Store: secondary.store, Txn: nilDecisionTransactional{}}
	} else {
		groups[1] = &ShardGroup{Engine: homeEngine, Store: home.store, Txn: nilDecisionTransactional{}}
	}
	lookup := twoGroupNativeLookup{metadata: homeMetadata, engine: homeEngine}
	remotePrepare := &twoGroupRemoteTestPeer{group: remoteGroup}
	remoteTerminal := &twoGroupRemoteTerminalTestPeer{group: remoteGroup}
	if remoteGroup == 1 {
		remotePrepare.leader = homeEngine
		remoteTerminal.engine, remoteTerminal.metadata = homeEngine, homeMetadata
		remoteTerminal.secondary = func(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest) error {
			_, _, err := VerifyTwoGroupPrepareOnLeader(ctx, group, manifest, secondaryEngine, secondaryMetadata,
				func(context.Context) error { return nil })
			return err
		}
	} else {
		remotePrepare.leader, remotePrepare.lookup = secondaryEngine, lookup
		remoteTerminal.engine, remoteTerminal.metadata, remoteTerminal.home = secondaryEngine, secondaryMetadata, lookup
	}
	coord := &ShardedCoordinator{groups: groups, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) { return lookup, nil })
	coord.WithTwoGroupRemoteEvidence(func(group uint64) (TwoGroupPrepareEvidenceProbe, error) {
		if group != remoteGroup {
			return nil, ErrTwoGroupAdmission
		}
		return &twoGroupEvidenceProbeStub{group: group}, nil
	})
	coord.WithTwoGroupRemotePrepare(func(group uint64) (TwoGroupRemotePreparer, error) {
		if group != remoteGroup {
			return nil, ErrTwoGroupAdmission
		}
		return remotePrepare, nil
	})
	coord.WithTwoGroupRemoteTerminal(func(group uint64) (TwoGroupRemoteTerminal, error) {
		if group != remoteGroup {
			return nil, ErrTwoGroupAdmission
		}
		return remoteTerminal, nil
	})
	return remoteOnlyTwoGroupFixture{coord: coord, homeEngine: homeEngine, secondaryEngine: secondaryEngine,
		homeMetadata: homeMetadata, secondaryMetadata: secondaryMetadata, remoteTerminal: remoteTerminal,
		grouped: map[uint64][]*pb.Mutation{
			1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
			2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
		}}
}

func (f remoteOnlyTwoGroupFixture) prepare(t *testing.T) *pb.TwoGroupPrepareManifest {
	t.Helper()
	unresolved, err := f.coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), f.grouped, nil, 0, nil)
	require.NoError(t, err)
	require.False(t, unresolved)
	manifest, _, err := f.coord.buildTwoGroupManifest(t.Context(), 10, 42, []byte("home-key"), f.grouped,
		[]uint64{1, 2}, nil, 0, nil)
	require.NoError(t, err)
	return manifest
}

func TestTwoGroupRemoteOnlyPrepareAndTerminal(t *testing.T) {
	for _, remoteGroup := range []uint64{1, 2} {
		t.Run(map[uint64]string{1: "primary", 2: "secondary"}[remoteGroup], func(t *testing.T) {
			f := newRemoteOnlyTwoGroupFixture(t, remoteGroup)
			require.NotContains(t, f.coord.groups, remoteGroup)
			manifest := f.prepare(t)
			require.NoError(t, f.coord.decideTwoGroup(t.Context(), manifest, pb.Phase_COMMIT))
			var id [16]byte
			copy(id[:], manifest.GetOperationId())
			for _, metadata := range []store.RaftControlMetadataStore{f.homeMetadata, f.secondaryMetadata} {
				raw, err := metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
				require.NoError(t, err)
				record, err := parseTwoGroupTerminalRecord(raw)
				require.NoError(t, err)
				require.Equal(t, pb.Phase_COMMIT, record.GetDecision())
			}
		})
	}
}

func TestTwoGroupRemoteOnlySecondaryFailureKeepsOutcomeUnknown(t *testing.T) {
	f := newRemoteOnlyTwoGroupFixture(t, 2)
	manifest := f.prepare(t)
	f.remoteTerminal.failDecision = errors.New("remote secondary unavailable")
	require.ErrorIs(t, f.coord.decideTwoGroup(t.Context(), manifest, pb.Phase_COMMIT), ErrTransactionOutcomeUnknown)
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	_, found, err := LookupTwoGroupHomeDecision(t.Context(), f.homeMetadata, id)
	require.NoError(t, err)
	require.True(t, found)
	_, err = f.secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestTwoGroupRemoteOnlyAbort(t *testing.T) {
	for _, remoteGroup := range []uint64{1, 2} {
		t.Run(map[uint64]string{1: "primary", 2: "secondary"}[remoteGroup], func(t *testing.T) {
			f := newRemoteOnlyTwoGroupFixture(t, remoteGroup)
			manifest := f.prepare(t)
			require.NoError(t, f.coord.decideTwoGroup(t.Context(), manifest, pb.Phase_ABORT))
			var id [16]byte
			copy(id[:], manifest.GetOperationId())
			for _, metadata := range []store.RaftControlMetadataStore{f.homeMetadata, f.secondaryMetadata} {
				raw, err := metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
				require.NoError(t, err)
				record, err := parseTwoGroupTerminalRecord(raw)
				require.NoError(t, err)
				require.Equal(t, pb.Phase_ABORT, record.GetDecision())
			}
		})
	}
}

func TestTwoGroupRemoteOnlyPrimaryLostReplyReconciles(t *testing.T) {
	f := newRemoteOnlyTwoGroupFixture(t, 1)
	manifest := f.prepare(t)
	f.homeEngine.lostReply = true
	require.NoError(t, f.coord.decideTwoGroup(t.Context(), manifest, pb.Phase_COMMIT))
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	_, found, err := LookupTwoGroupHomeDecision(t.Context(), f.homeMetadata, id)
	require.NoError(t, err)
	require.True(t, found)
	_, err = f.secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
	require.NoError(t, err)
}

func TestTwoGroupRemoteOnlySecondaryRequiresPrimaryDecision(t *testing.T) {
	f := newRemoteOnlyTwoGroupFixture(t, 2)
	manifest := f.prepare(t)
	require.ErrorIs(t, f.remoteTerminal.DecideTwoGroupOnLeader(t.Context(), 2, manifest, pb.Phase_COMMIT),
		ErrTransactionOutcomeUnknown)
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	_, err := f.secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(id))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestTwoGroupRemoteOnlyPrimaryTransferKeepsDecisionUnknown(t *testing.T) {
	f := newRemoteOnlyTwoGroupFixture(t, 1)
	manifest := f.prepare(t)
	f.homeEngine.transferBeforePropose = true
	require.ErrorIs(t, f.coord.decideTwoGroup(t.Context(), manifest, pb.Phase_COMMIT), ErrTransactionOutcomeUnknown)
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	_, found, err := LookupTwoGroupHomeDecision(t.Context(), f.homeMetadata, id)
	require.NoError(t, err)
	require.False(t, found)
}

func TestTwoGroupRemoteOnlyReadOnlyShardFailsBeforePrepare(t *testing.T) {
	f := newRemoteOnlyTwoGroupFixture(t, 2)
	unresolved, err := f.coord.prepareTwoGroup(t.Context(), 10, 42, []byte("home-key"), f.grouped,
		map[uint64][][]byte{3: {[]byte("read-key")}}, 0, nil)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	require.False(t, unresolved)
	id := twoGroupOperationID([]byte("home-key"), 10)
	_, found, err := LookupTwoGroupHomeRecord(t.Context(), f.homeMetadata, id)
	require.NoError(t, err)
	require.False(t, found)
}
