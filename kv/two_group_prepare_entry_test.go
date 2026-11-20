package kv

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func twoGroupEntryFSM(t *testing.T, group uint64) (*kvFSM, store.RaftControlMetadataStore) {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(nil, group)).(*kvFSM)
	require.True(t, ok)
	f.twoGroupPrepareEnabled.Store(true)
	f.SetApplyIndex(1)
	require.NoError(t, f.applyPrepareEvidenceFence(1))
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	return f, metadata
}

func applyTwoGroupTestEntry(t *testing.T, f *kvFSM, index, term uint64, data []byte) any {
	t.Helper()
	f.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(index, term, 0, data))
	return f.Apply(data)
}

func TestTwoGroupNativePrepareRequiresAppliedHome(t *testing.T) {
	home, homeMetadata := twoGroupEntryFSM(t, 1)
	secondary, secondaryMetadata := twoGroupEntryFSM(t, 2)
	manifest := twoGroupPrepareFixture()
	_, err := ObserveTwoGroupHome(t.Context(), localTwoGroupHomeLookup{store: homeMetadata, barrier: 2, term: 1}, manifest)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	homeEntry, err := marshalTwoGroupPrepareEntry(1, manifest, TwoGroupAdmissionObservation{})
	require.NoError(t, err)
	homeResult := applyTwoGroupTestEntry(t, home, 2, 1, homeEntry)
	if halted, ok := homeResult.(*haltApplyResponse); ok {
		t.Fatalf("home apply halted: %+v", halted.HaltApply())
	}
	require.Nil(t, homeResult)
	observed, err := ObserveTwoGroupHome(t.Context(), localTwoGroupHomeLookup{store: homeMetadata, barrier: 2, term: 1}, manifest)
	require.NoError(t, err)
	secondaryEntry, err := marshalTwoGroupPrepareEntry(2, manifest, observed)
	require.NoError(t, err)
	secondaryResult := applyTwoGroupTestEntry(t, secondary, 2, 1, secondaryEntry)
	if halted, ok := secondaryResult.(*haltApplyResponse); ok {
		t.Fatalf("secondary apply halted: %+v", halted.HaltApply())
	}
	require.Nil(t, secondaryResult)
	_, found, err := LookupTimestampBinding(t.Context(), secondaryMetadata, twoGroupFixtureID)
	require.NoError(t, err)
	require.True(t, found)
}

func TestTwoGroupNativePrepareRejectsLegacyAndDisabledEntry(t *testing.T) {
	f, metadata := twoGroupEntryFSM(t, 1)
	manifest := twoGroupPrepareFixture()
	legacy, err := marshalRaftCommand([]*pb.Request{manifest.Participants[0].Prepare})
	require.NoError(t, err)
	f.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(2, 1, 0, legacy))
	legacyErr, ok := f.Apply(legacy).(error)
	require.True(t, ok)
	require.ErrorIs(t, legacyErr, ErrTwoGroupAdmission)
	_, found, err := LookupTimestampBinding(t.Context(), metadata, twoGroupFixtureID)
	require.NoError(t, err)
	require.False(t, found)
	entry, err := marshalTwoGroupPrepareEntry(1, manifest, TwoGroupAdmissionObservation{})
	require.NoError(t, err)
	f.twoGroupPrepareEnabled.Store(false)
	result := applyTwoGroupTestEntry(t, f, 2, 1, entry)
	halted, ok := result.(*haltApplyResponse)
	require.True(t, ok)
	require.ErrorIs(t, halted.HaltApply(), ErrTwoGroupAdmission)
}

func TestTwoGroupNativePrepareRejectsLegacyTerminalEntries(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			f, _ := twoGroupEntryFSM(t, 1)
			manifest := twoGroupPrepareFixture()
			entry, err := marshalTwoGroupPrepareEntry(1, manifest, TwoGroupAdmissionObservation{})
			require.NoError(t, err)
			require.Nil(t, applyTwoGroupTestEntry(t, f, 2, 1, entry))
			lockKey := txnLockKey([]byte("home-key"))
			before, err := f.store.GetAt(t.Context(), lockKey, ^uint64(0))
			require.NoError(t, err)
			require.NotEmpty(t, before)

			legacy, ok := proto.Clone(manifest.Participants[0].Prepare).(*pb.Request)
			require.True(t, ok)
			legacy.Phase = phase
			data, err := marshalRaftCommand([]*pb.Request{legacy})
			require.NoError(t, err)
			result := applyTwoGroupTestEntry(t, f, 3, 1, data)
			applyErr, ok := result.(error)
			require.True(t, ok)
			require.ErrorIs(t, applyErr, ErrTwoGroupAdmission)
			after, err := f.store.GetAt(t.Context(), lockKey, ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestTwoGroupNativeSecondaryRejectsTamperedEntry(t *testing.T) {
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
	for _, alter := range []func(*pb.TwoGroupPrepareEntry){
		func(e *pb.TwoGroupPrepareEntry) { e.HomeBarrierIndex = 1 },
		func(e *pb.TwoGroupPrepareEntry) { e.HomeLeaderTerm = 0 },
		func(e *pb.TwoGroupPrepareEntry) {
			e.Manifest.Participants[1].Prepare.Mutations[1].Value = []byte("wrong")
		},
		func(e *pb.TwoGroupPrepareEntry) { e.LocalGroupId = 1 },
	} {
		entry := new(pb.TwoGroupPrepareEntry)
		require.NoError(t, proto.Unmarshal(secondaryEntry[1:], entry))
		alter(entry)
		raw, err := proto.Marshal(entry)
		require.NoError(t, err)
		result := applyTwoGroupTestEntry(t, secondary, 2, 1, prependByte(raftEncodeTwoGroupPrepare, raw))
		applyErr, ok := result.(error)
		require.True(t, ok)
		require.ErrorIs(t, applyErr, ErrTwoGroupAdmission)
	}
	_, found, err := LookupTimestampBinding(t.Context(), secondaryMetadata, twoGroupFixtureID)
	require.NoError(t, err)
	require.False(t, found)
}

func TestTwoGroupNativeHomeRecordSurvivesPebbleRestart(t *testing.T) {
	path := t.TempDir()
	st, err := store.NewPebbleStore(path)
	require.NoError(t, err)
	f, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(nil, 1)).(*kvFSM)
	require.True(t, ok)
	f.twoGroupPrepareEnabled.Store(true)
	f.SetApplyIndex(1)
	require.NoError(t, f.applyPrepareEvidenceFence(1))
	manifest := twoGroupPrepareFixture()
	entry, err := marshalTwoGroupPrepareEntry(1, manifest, TwoGroupAdmissionObservation{})
	require.NoError(t, err)
	result := applyTwoGroupTestEntry(t, f, 2, 1, entry)
	if halted, ok := result.(*haltApplyResponse); ok {
		t.Fatalf("Pebble apply halted: %+v", halted.HaltApply())
	}
	require.Nil(t, result)
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	metadata, ok := reopened.(store.RaftControlMetadataStore)
	require.True(t, ok)
	_, found, err := LookupTwoGroupHomeRecord(t.Context(), metadata, twoGroupFixtureID)
	require.NoError(t, err)
	require.True(t, found)
}
