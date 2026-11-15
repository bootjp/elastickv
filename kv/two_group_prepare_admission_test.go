package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type localTwoGroupHomeLookup struct {
	store   store.RaftControlMetadataStore
	barrier uint64
	term    uint64
	err     error
}

func (l localTwoGroupHomeLookup) LookupTwoGroupHome(ctx context.Context, id [16]byte) ([]byte, uint64, uint64, error) {
	if l.err != nil {
		return nil, 0, 0, l.err
	}
	raw, found, err := LookupTwoGroupHomeRecord(ctx, l.store, id)
	if err != nil || !found {
		return nil, 0, 0, errors.Join(ErrTwoGroupAdmission, err)
	}
	return raw, l.barrier, l.term, nil
}

func twoGroupPrepareFixture() *pb.TwoGroupPrepareManifest {
	const commitTS = 42
	makePrepare := func(key string) *pb.Request {
		return &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 10, Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
				PrimaryKey: []byte("home-key"), CommitTS: commitTS, PrepareCommitTS: commitTS, LockTTLms: 1000,
			})},
			{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("value")},
		}}
	}
	return &pb.TwoGroupPrepareManifest{Version: twoGroupManifestVersion, OperationId: append([]byte{1}, make([]byte, 15)...),
		PersistenceTimestamp: commitTS, PrimaryGroupId: 1,
		Participants: []*pb.TwoGroupPrepareParticipant{
			{GroupId: 1, Prepare: makePrepare("home-key")},
			{GroupId: 2, Prepare: makePrepare("secondary-key")},
		}}
}

func controlStore(t *testing.T) (store.MVCCStore, store.RaftControlMetadataStore) {
	t.Helper()
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	return st, metadata
}

func stagePrepareMarker(t *testing.T, metadata store.RaftControlMetadataStore, key string) func(context.Context) error {
	t.Helper()
	return func(ctx context.Context) error {
		return metadata.StageRaftControlMetadata(ctx, []byte(key), []byte("prepared"))
	}
}

func TestTwoGroupPrepareAdmissionRequiresAppliedHome(t *testing.T) {
	home, homeMetadata := controlStore(t)
	secondary, secondaryMetadata := controlStore(t)
	manifest := twoGroupPrepareFixture()
	lookup := localTwoGroupHomeLookup{store: homeMetadata, barrier: 1, term: 1}
	_, err := ObserveTwoGroupHome(t.Context(), lookup, manifest)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	_, err = secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupSecondaryKey([16]byte{1}))
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	already, err := ApplyTwoGroupHomePrepare(t.Context(), home, 1, 1, manifest,
		stagePrepareMarker(t, homeMetadata, "test/home-prepared"))
	require.NoError(t, err)
	require.False(t, already)
	observed, err := ObserveTwoGroupHome(t.Context(), lookup, manifest)
	require.NoError(t, err)
	already, err = ApplyTwoGroupSecondaryPrepare(t.Context(), secondary, 1, 2, manifest, observed,
		stagePrepareMarker(t, secondaryMetadata, "test/secondary-prepared"))
	require.NoError(t, err)
	require.False(t, already)
	_, err = secondaryMetadata.ReadRaftControlMetadata(t.Context(), []byte("test/secondary-prepared"))
	require.NoError(t, err)
	proof, err := secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupSecondaryKey([16]byte{1}))
	require.NoError(t, err)
	_, err = validateTwoGroupObservationProof(proof, 2)
	require.NoError(t, err)
}

func TestTwoGroupPrepareAdmissionRejectsChangedIdentityAndManifest(t *testing.T) {
	home, metadata := controlStore(t)
	manifest := twoGroupPrepareFixture()
	_, err := ApplyTwoGroupHomePrepare(t.Context(), home, 1, 1, manifest,
		stagePrepareMarker(t, metadata, "test/home-prepared"))
	require.NoError(t, err)
	lookup := localTwoGroupHomeLookup{store: metadata, barrier: 1, term: 1}
	for _, change := range []func(*pb.TwoGroupPrepareManifest){
		func(m *pb.TwoGroupPrepareManifest) { m.OperationId[0] = 2 },
		func(m *pb.TwoGroupPrepareManifest) { m.PersistenceTimestamp++ },
		func(m *pb.TwoGroupPrepareManifest) { m.Participants[1].Prepare.Mutations[1].Value = []byte("changed") },
		func(m *pb.TwoGroupPrepareManifest) {
			m.Participants[0], m.Participants[1] = m.Participants[1], m.Participants[0]
		},
	} {
		changed := proto.CloneOf(manifest)
		change(changed)
		_, err := ObserveTwoGroupHome(t.Context(), lookup, changed)
		require.ErrorIs(t, err, ErrTwoGroupAdmission)
	}
}

func TestTwoGroupPrepareAdmissionRetriesAfterLostReplyAndTransfer(t *testing.T) {
	home, metadata := controlStore(t)
	secondary, secondaryMetadata := controlStore(t)
	manifest := twoGroupPrepareFixture()
	applyCount := 0
	apply := func(ctx context.Context) error {
		applyCount++
		return metadata.StageRaftControlMetadata(ctx, []byte("test/home-prepared"), []byte("prepared"))
	}
	_, err := ApplyTwoGroupHomePrepare(t.Context(), home, 1, 1, manifest, apply)
	require.NoError(t, err)
	// The first reply is lost; the next leader observes the immutable applied record.
	already, err := ApplyTwoGroupHomePrepare(t.Context(), home, 2, 2, manifest, apply)
	require.NoError(t, err)
	require.True(t, already)
	require.Equal(t, 1, applyCount)
	lookup := localTwoGroupHomeLookup{store: metadata, barrier: 2, term: 2}
	observed, err := ObserveTwoGroupHome(t.Context(), lookup, manifest)
	require.NoError(t, err)
	_, err = ApplyTwoGroupSecondaryPrepare(t.Context(), secondary, 1, 2, manifest, observed,
		stagePrepareMarker(t, secondaryMetadata, "test/secondary-prepared"))
	require.NoError(t, err)
	lookup.barrier, lookup.term = 3, 3
	newObservation, err := ObserveTwoGroupHome(t.Context(), lookup, manifest)
	require.NoError(t, err)
	already, err = ApplyTwoGroupSecondaryPrepare(t.Context(), secondary, 2, 2, manifest, newObservation,
		func(context.Context) error { t.Fatal("exact retry reapplied PREPARE"); return nil })
	require.NoError(t, err)
	require.True(t, already)
}

func TestTwoGroupPrepareAdmissionFailsClosedOnLookupOrApply(t *testing.T) {
	home, metadata := controlStore(t)
	manifest := twoGroupPrepareFixture()
	_, err := ApplyTwoGroupHomePrepare(t.Context(), home, 1, 1, manifest,
		func(context.Context) error { return errors.New("apply failed") })
	require.Error(t, err)
	_, found, err := LookupTwoGroupHomeRecord(t.Context(), metadata, [16]byte{1})
	require.NoError(t, err)
	require.False(t, found)
	_, err = ObserveTwoGroupHome(t.Context(), localTwoGroupHomeLookup{err: errors.New("leader transferred")}, manifest)
	require.ErrorContains(t, err, "leader transferred")
	_, err = ObserveTwoGroupHome(t.Context(), localTwoGroupHomeLookup{store: metadata, barrier: 0, term: 0}, manifest)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
}

func TestTwoGroupSecondaryRejectsChangedProjectionAndRollsBackFailedPrepare(t *testing.T) {
	home, homeMetadata := controlStore(t)
	secondary, secondaryMetadata := controlStore(t)
	manifest := twoGroupPrepareFixture()
	_, err := ApplyTwoGroupHomePrepare(t.Context(), home, 1, 1, manifest,
		stagePrepareMarker(t, homeMetadata, "test/home-prepared"))
	require.NoError(t, err)
	observed, err := ObserveTwoGroupHome(t.Context(), localTwoGroupHomeLookup{store: homeMetadata, barrier: 1, term: 1}, manifest)
	require.NoError(t, err)
	changed := proto.CloneOf(manifest)
	changed.Participants[1].Prepare.Mutations[1].Value = []byte("changed")
	_, err = ApplyTwoGroupSecondaryPrepare(t.Context(), secondary, 1, 2, changed, observed,
		stagePrepareMarker(t, secondaryMetadata, "test/secondary-prepared"))
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	_, err = ApplyTwoGroupSecondaryPrepare(t.Context(), secondary, 1, 2, manifest, observed,
		func(context.Context) error { return errors.New("prepare failed") })
	require.ErrorContains(t, err, "prepare failed")
	_, err = secondaryMetadata.ReadRaftControlMetadata(t.Context(), twoGroupSecondaryKey([16]byte{1}))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, found, err := LookupTimestampBinding(t.Context(), secondaryMetadata, [16]byte{1})
	require.NoError(t, err)
	require.False(t, found)
}

func TestTwoGroupPrepareAdmissionRejectsRecordWithoutBindingOnReplay(t *testing.T) {
	manifest := twoGroupPrepareFixture()
	raw, _, err := twoGroupRecord(manifest, 1, 1)
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{name: "home", key: twoGroupHomeKey([16]byte{1})},
		{name: "secondary", key: twoGroupSecondaryKey([16]byte{1})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, metadata := controlStore(t)
			applier, ok := st.(store.RaftEntryApplier)
			require.True(t, ok)
			value := raw
			if tc.name == "secondary" {
				value = make([]byte, twoGroupObservationHeaderSize+len(raw))
				binary.BigEndian.PutUint64(value[:8], 1)
				binary.BigEndian.PutUint64(value[8:16], 1)
				copy(value[twoGroupObservationHeaderSize:], raw)
			}
			require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
				return metadata.StageRaftControlMetadata(ctx, tc.key, value)
			}))
			apply := func(context.Context) error { t.Fatal("replay applied PREPARE"); return nil }
			if tc.name == "home" {
				_, err = ApplyTwoGroupHomePrepare(t.Context(), st, 2, 2, manifest, apply)
			} else {
				observed := TwoGroupAdmissionObservation{record: raw, barrierIndex: 1, leaderTerm: 1}
				_, err = ApplyTwoGroupSecondaryPrepare(t.Context(), st, 2, 2, manifest, observed, apply)
			}
			require.ErrorIs(t, err, ErrTimestampBindingConflict)
			_, found, lookupErr := LookupTimestampBinding(t.Context(), metadata, [16]byte{1})
			require.NoError(t, lookupErr)
			require.False(t, found)
		})
	}
}
