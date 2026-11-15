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

func restoreTwoGroupTestFSM(t *testing.T, source *kvFSM, group uint64) (*kvFSM, store.RaftControlMetadataStore) {
	t.Helper()
	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	var image bytes.Buffer
	_, err = snapshot.WriteTo(&image)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())

	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	restored, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(nil, group)).(*kvFSM)
	require.True(t, ok)
	restored.twoGroupPrepareEnabled.Store(true)
	require.NoError(t, restored.Restore(bytes.NewReader(image.Bytes())))
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	return restored, metadata
}

func TestTwoGroupPrepareAndTerminalSurviveBothGroupSnapshotRestores(t *testing.T) {
	home, secondary, _, _, manifest := prepareTwoGroupTerminalFSMs(t)

	home, homeMetadata := restoreTwoGroupTestFSM(t, home, 1)
	secondary, secondaryMetadata := restoreTwoGroupTestFSM(t, secondary, 2)
	for _, tc := range []struct {
		name     string
		fsm      *kvFSM
		metadata store.RaftControlMetadataStore
		group    uint64
	}{
		{name: "home", fsm: home, metadata: homeMetadata, group: 1},
		{name: "secondary", fsm: secondary, metadata: secondaryMetadata, group: 2},
	} {
		t.Run(tc.name+" prepare", func(t *testing.T) {
			key := []byte("home-key")
			if tc.group == 2 {
				key = []byte("secondary-key")
			}
			binding, found, err := LookupTimestampBinding(t.Context(), tc.metadata, twoGroupFixtureID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, manifest.GetPersistenceTimestamp(), binding.Timestamp)
			require.Equal(t, pb.Phase_PREPARE, binding.Phase)
			if tc.group == 1 {
				_, found, err = LookupTwoGroupHomeRecord(t.Context(), tc.metadata, twoGroupFixtureID)
				require.NoError(t, err)
				require.True(t, found)
			} else {
				proof, err := tc.metadata.ReadRaftControlMetadata(t.Context(), twoGroupSecondaryKey(twoGroupFixtureID))
				require.NoError(t, err)
				observedBinding, err := validateTwoGroupObservationProof(proof, tc.group)
				require.NoError(t, err)
				require.Equal(t, binding, observedBinding)
			}
			_, err = tc.fsm.store.GetAt(t.Context(), txnLockKey(key), ^uint64(0))
			require.NoError(t, err)
		})
	}

	homeCommit, err := marshalTwoGroupTerminalEntry(1, manifest, pb.Phase_COMMIT, TwoGroupDecisionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, home, 3, 1, homeCommit))
	decision, err := ObserveTwoGroupDecision(t.Context(), localTwoGroupDecisionLookup{store: homeMetadata, barrier: 3, term: 1}, manifest, pb.Phase_COMMIT)
	require.NoError(t, err)
	secondaryCommit, err := marshalTwoGroupTerminalEntry(2, manifest, pb.Phase_COMMIT, decision)
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, secondary, 3, 1, secondaryCommit))

	home, homeMetadata = restoreTwoGroupTestFSM(t, home, 1)
	secondary, secondaryMetadata = restoreTwoGroupTestFSM(t, secondary, 2)
	for _, tc := range []struct {
		name     string
		fsm      *kvFSM
		metadata store.RaftControlMetadataStore
		group    uint64
	}{
		{name: "home", fsm: home, metadata: homeMetadata, group: 1},
		{name: "secondary", fsm: secondary, metadata: secondaryMetadata, group: 2},
	} {
		t.Run(tc.name+" terminal", func(t *testing.T) {
			key := []byte("home-key")
			if tc.group == 2 {
				key = []byte("secondary-key")
			}
			binding, found, err := LookupTimestampBinding(t.Context(), tc.metadata, twoGroupFixtureID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, pb.Phase_PREPARE, binding.Phase)
			raw, err := tc.metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(twoGroupFixtureID))
			require.NoError(t, err)
			record, err := parseTwoGroupTerminalRecord(raw)
			require.NoError(t, err)
			require.Equal(t, pb.Phase_COMMIT, record.GetDecision())
			if tc.group == 2 {
				proof, err := tc.metadata.ReadRaftControlMetadata(t.Context(), twoGroupSecondaryKey(twoGroupFixtureID))
				require.NoError(t, err)
				observedBinding, err := validateTwoGroupObservationProof(proof, tc.group)
				require.NoError(t, err)
				require.Equal(t, binding, observedBinding)
			}
			if tc.group == 1 {
				_, err = tc.fsm.store.GetAt(t.Context(), txnCommitKey(key, 10), ^uint64(0))
				require.NoError(t, err)
			} else {
				value, err := tc.fsm.store.GetAt(t.Context(), key, manifest.GetPersistenceTimestamp())
				require.NoError(t, err)
				require.Equal(t, []byte("value"), value)
			}

			_, applier := testTimestampBindingStores(t, tc.fsm.store)
			for _, changed := range []TimestampBinding{
				func() TimestampBinding { b := binding; b.Timestamp++; return b }(),
				func() TimestampBinding { b := binding; b.OperationID[0]++; return b }(),
			} {
				err := applier.ApplyRaftEntry(t.Context(), 4, func(ctx context.Context) error {
					_, err := StageTimestampBinding(ctx, tc.metadata, changed)
					return err
				})
				require.ErrorIs(t, err, ErrTimestampBindingConflict)
			}
		})
	}
}

func twoGroupTestSnapshotImage(t *testing.T, source *kvFSM) []byte {
	t.Helper()
	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	var image bytes.Buffer
	_, err = snapshot.WriteTo(&image)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	return image.Bytes()
}

func restoreTwoGroupTestImage(t *testing.T, image []byte, group uint64) (*kvFSM, store.RaftControlMetadataStore) {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(nil, group)).(*kvFSM)
	require.True(t, ok)
	f.twoGroupPrepareEnabled.Store(true)
	require.NoError(t, f.Restore(bytes.NewReader(image)))
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	return f, metadata
}

func TestTwoGroupOldSnapshotReplaysPrepareAndTerminalSuffix(t *testing.T) {
	homeSource, homeSourceMetadata := twoGroupEntryFSM(t, 1)
	secondarySource, _ := twoGroupEntryFSM(t, 2)
	homeImage := twoGroupTestSnapshotImage(t, homeSource)
	secondaryImage := twoGroupTestSnapshotImage(t, secondarySource)
	manifest := twoGroupPrepareFixture()

	homePrepare, err := marshalTwoGroupPrepareEntry(1, manifest, TwoGroupAdmissionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, homeSource, 2, 1, homePrepare))
	prepared, err := ObserveTwoGroupHome(t.Context(), localTwoGroupHomeLookup{store: homeSourceMetadata, barrier: 2, term: 1}, manifest)
	require.NoError(t, err)
	secondaryPrepare, err := marshalTwoGroupPrepareEntry(2, manifest, prepared)
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, secondarySource, 2, 1, secondaryPrepare))
	homeCommit, err := marshalTwoGroupTerminalEntry(1, manifest, pb.Phase_COMMIT, TwoGroupDecisionObservation{})
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, homeSource, 3, 1, homeCommit))
	committed, err := ObserveTwoGroupDecision(t.Context(), localTwoGroupDecisionLookup{store: homeSourceMetadata, barrier: 3, term: 1}, manifest, pb.Phase_COMMIT)
	require.NoError(t, err)
	secondaryCommit, err := marshalTwoGroupTerminalEntry(2, manifest, pb.Phase_COMMIT, committed)
	require.NoError(t, err)
	require.Nil(t, applyTwoGroupTestEntry(t, secondarySource, 3, 1, secondaryCommit))

	home, homeMetadata := restoreTwoGroupTestImage(t, homeImage, 1)
	secondary, secondaryMetadata := restoreTwoGroupTestImage(t, secondaryImage, 2)
	for _, metadata := range []store.RaftControlMetadataStore{homeMetadata, secondaryMetadata} {
		_, found, err := LookupTimestampBinding(t.Context(), metadata, twoGroupFixtureID)
		require.NoError(t, err)
		require.False(t, found, "an old snapshot must not expose later binding evidence")
		_, err = metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(twoGroupFixtureID))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	_, err = homeMetadata.ReadRaftControlMetadata(t.Context(), twoGroupHomeKey(twoGroupFixtureID))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupSecondaryKey(twoGroupFixtureID))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, found, err := LookupTwoGroupHomeDecision(t.Context(), homeMetadata, twoGroupFixtureID)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	require.False(t, found)
	_, err = home.store.GetAt(t.Context(), txnLockKey([]byte("home-key")), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = secondary.store.GetAt(t.Context(), []byte("secondary-key"), manifest.GetPersistenceTimestamp())
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	staleHome, staleHomeMetadata := restoreTwoGroupTestImage(t, homeImage, 1)
	staleSecondary, staleSecondaryMetadata := restoreTwoGroupTestImage(t, secondaryImage, 2)
	for _, tc := range []struct {
		fsm      *kvFSM
		metadata store.RaftControlMetadataStore
		entry    []byte
	}{
		{fsm: staleHome, metadata: staleHomeMetadata, entry: homeCommit},
		{fsm: staleSecondary, metadata: staleSecondaryMetadata, entry: secondaryCommit},
	} {
		result := applyTwoGroupTestEntry(t, tc.fsm, 3, 1, tc.entry)
		applyErr, ok := result.(error)
		require.True(t, ok)
		require.ErrorIs(t, applyErr, ErrTwoGroupAdmission)
		_, err := tc.metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(twoGroupFixtureID))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}

	require.Nil(t, applyTwoGroupTestEntry(t, home, 2, 1, homePrepare))
	require.Nil(t, applyTwoGroupTestEntry(t, secondary, 2, 1, secondaryPrepare))
	require.Nil(t, applyTwoGroupTestEntry(t, home, 3, 1, homeCommit))
	require.Nil(t, applyTwoGroupTestEntry(t, secondary, 3, 1, secondaryCommit))

	for _, tc := range []struct {
		name     string
		fsm      *kvFSM
		metadata store.RaftControlMetadataStore
		group    uint64
	}{
		{name: "home", fsm: home, metadata: homeMetadata, group: 1},
		{name: "secondary", fsm: secondary, metadata: secondaryMetadata, group: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding, found, err := LookupTimestampBinding(t.Context(), tc.metadata, twoGroupFixtureID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, manifest.GetPersistenceTimestamp(), binding.Timestamp)
			require.Equal(t, pb.Phase_PREPARE, binding.Phase)
			if tc.group == 1 {
				_, found, err = LookupTwoGroupHomeRecord(t.Context(), tc.metadata, twoGroupFixtureID)
				require.NoError(t, err)
				require.True(t, found)
			} else {
				proof, err := tc.metadata.ReadRaftControlMetadata(t.Context(), twoGroupSecondaryKey(twoGroupFixtureID))
				require.NoError(t, err)
				observedBinding, err := validateTwoGroupObservationProof(proof, tc.group)
				require.NoError(t, err)
				require.Equal(t, binding, observedBinding)
			}
			raw, err := tc.metadata.ReadRaftControlMetadata(t.Context(), twoGroupTerminalKey(twoGroupFixtureID))
			require.NoError(t, err)
			terminal, err := parseTwoGroupTerminalRecord(raw)
			require.NoError(t, err)
			require.Equal(t, pb.Phase_COMMIT, terminal.GetDecision())
			if tc.group == 1 {
				_, err = tc.fsm.store.GetAt(t.Context(), txnCommitKey([]byte("home-key"), 10), ^uint64(0))
				require.NoError(t, err)
			} else {
				value, err := tc.fsm.store.GetAt(t.Context(), []byte("secondary-key"), manifest.GetPersistenceTimestamp())
				require.NoError(t, err)
				require.Equal(t, []byte("value"), value)
			}

			_, applier := testTimestampBindingStores(t, tc.fsm.store)
			for _, changed := range []TimestampBinding{
				func() TimestampBinding { b := binding; b.Timestamp++; return b }(),
				func() TimestampBinding { b := binding; b.OperationID[0]++; return b }(),
			} {
				err := applier.ApplyRaftEntry(t.Context(), 4, func(ctx context.Context) error {
					_, err := StageTimestampBinding(ctx, tc.metadata, changed)
					return err
				})
				require.ErrorIs(t, err, ErrTimestampBindingConflict)
			}
		})
	}
}
