package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTwoGroupTerminalOpcodeCannotAliasBareRequest(t *testing.T) {
	bare, err := proto.Marshal(&pb.Request{ObservedRouteVersion: 1})
	require.NoError(t, err)
	require.Equal(t, byte(0x30), bare[0], "route-version-only legacy requests expose the old collision")
	require.NotEqual(t, raftEncodeTwoGroupTerminal, bare[0])
	require.Error(t, proto.Unmarshal([]byte{raftEncodeTwoGroupTerminal, 1}, new(pb.Request)))
}

type localTwoGroupDecisionLookup struct {
	store   store.RaftControlMetadataStore
	barrier uint64
	term    uint64
}

func (l localTwoGroupDecisionLookup) LookupTwoGroupDecision(ctx context.Context, id [16]byte) ([]byte, uint64, uint64, error) {
	raw, found, err := LookupTwoGroupHomeDecision(ctx, l.store, id)
	if err != nil || !found {
		return nil, 0, 0, ErrTransactionOutcomeUnknown
	}
	return raw, l.barrier, l.term, nil
}

func prepareTwoGroupTerminalFSMs(t *testing.T) (*kvFSM, *kvFSM, store.RaftControlMetadataStore, store.RaftControlMetadataStore, *pb.TwoGroupPrepareManifest) {
	t.Helper()
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	manifest := twoGroupPrepareFixture()
	homeEntry, err := marshalTwoGroupPrepareEntry(1, manifest, TwoGroupAdmissionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, home, 2, 1, homeEntry))
	observed, err := ObserveTwoGroupHome(t.Context(), localTwoGroupHomeLookup{store: homeMetadata, barrier: 2, term: 1}, manifest)
	require.NoError(t, err)
	secondaryEntry, err := marshalTwoGroupPrepareEntry(2, manifest, observed)
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, secondary, 2, 1, secondaryEntry))
	return home, secondary, homeMetadata, secondaryMetadata, manifest
}

func TestTwoGroupTerminalCommitRequiresHomeDecision(t *testing.T) {
	home, secondary, homeMetadata, secondaryMetadata, manifest := prepareTwoGroupTerminalFSMs(t)
	_, err := ObserveTwoGroupDecision(t.Context(), localTwoGroupDecisionLookup{store: homeMetadata, barrier: 2, term: 1}, manifest, pb.Phase_COMMIT)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	_, err = marshalTwoGroupTerminalEntry(2, manifest, pb.Phase_COMMIT, TwoGroupDecisionObservation{})
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	homeEntry, err := marshalTwoGroupTerminalEntry(1, manifest, pb.Phase_COMMIT, TwoGroupDecisionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, home, 3, 1, homeEntry))
	observed, err := ObserveTwoGroupDecision(t.Context(), localTwoGroupDecisionLookup{store: homeMetadata, barrier: 3, term: 1}, manifest, pb.Phase_COMMIT)
	require.NoError(t, err)
	secondaryEntry, err := marshalTwoGroupTerminalEntry(2, manifest, pb.Phase_COMMIT, observed)
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, secondary, 3, 1, secondaryEntry))
	for _, metadata := range []store.RaftControlMetadataStore{homeMetadata, secondaryMetadata} {
		raw, err := metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(twoGroupFixtureID))
		require.NoError(t, err)
		record, err := parseTwoGroupTerminalRecord(raw)
		require.NoError(t, err)
		require.Equal(t, pb.Phase_COMMIT, record.GetDecision())
	}
	value, err := secondary.store.GetAt(t.Context(), []byte("secondary-key"), 42)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	_, err = home.store.GetAt(t.Context(), txnCommitKey([]byte("home-key"), 10), ^uint64(0))
	require.NoError(t, err)
}

func TestTwoGroupTerminalAbortIsImmutableAndKeepsBinding(t *testing.T) {
	home, secondary, homeMetadata, secondaryMetadata, manifest := prepareTwoGroupTerminalFSMs(t)
	homeAbort, err := marshalTwoGroupTerminalEntry(1, manifest, pb.Phase_ABORT, TwoGroupDecisionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, home, 3, 1, homeAbort))
	observed, err := ObserveTwoGroupDecision(t.Context(), localTwoGroupDecisionLookup{store: homeMetadata, barrier: 3, term: 1}, manifest, pb.Phase_ABORT)
	require.NoError(t, err)
	secondaryAbort, err := marshalTwoGroupTerminalEntry(2, manifest, pb.Phase_ABORT, observed)
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, secondary, 3, 1, secondaryAbort))
	require.Nil(t, applyTwoGroupTestEntry(t, home, 4, 2, homeAbort))
	commit, err := marshalTwoGroupTerminalEntry(1, manifest, pb.Phase_COMMIT, TwoGroupDecisionObservation{})
	require.NoError(t, err)
	result := applyTwoGroupTestEntry(t, home, 5, 2, commit)
	applyErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, ErrTwoGroupAdmission)
	for _, metadata := range []store.RaftControlMetadataStore{homeMetadata, secondaryMetadata} {
		binding, found, err := LookupTimestampBinding(t.Context(), metadata, twoGroupFixtureID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, pb.Phase_PREPARE, binding.Phase)
	}
	_, err = home.store.GetAt(t.Context(), txnRollbackKey([]byte("home-key"), 10), ^uint64(0))
	require.NoError(t, err)
}

func TestTwoGroupTerminalDecisionSurvivesPebbleReopenAndCompaction(t *testing.T) {
	path := t.TempDir()
	st, err := store.NewPebbleStore(path)
	require.NoError(t, err)
	f, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(nil, 1)).(*kvFSM)
	require.True(t, ok)
	f.twoGroupPrepareEnabled.Store(true)
	f.SetApplyIndex(1)
	require.NoError(t, f.applyPrepareEvidenceFence(1))
	manifest := twoGroupPrepareFixture()
	prepare, err := marshalTwoGroupPrepareEntry(1, manifest, TwoGroupAdmissionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, f, 2, 1, prepare))
	terminal, err := marshalTwoGroupTerminalEntry(1, manifest, pb.Phase_ABORT, TwoGroupDecisionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, f, 3, 1, terminal))
	require.NoError(t, st.Compact(t.Context(), st.LastCommitTS()))
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	metadata, ok := reopened.(store.RaftControlMetadataStore)
	require.True(t, ok)
	raw, found, err := LookupTwoGroupHomeDecision(t.Context(), metadata, twoGroupFixtureID)
	require.NoError(t, err)
	require.True(t, found)
	record, err := parseTwoGroupTerminalRecord(raw)
	require.NoError(t, err)
	require.Equal(t, pb.Phase_ABORT, record.GetDecision())
	_, err = reopened.GetAt(t.Context(), txnRollbackKey([]byte("home-key"), 10), ^uint64(0))
	require.NoError(t, err)
}
