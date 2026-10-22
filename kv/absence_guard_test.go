package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func absenceRequest(key []byte, guarded bool) *pb.Request {
	return &pb.Request{IsTxn: true, Phase: pb.Phase_NONE, Ts: 100, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, CommitTS: 101})},
		{Op: pb.Op_PUT, Key: key, Value: []byte("migration"), RequireAbsent: guarded},
	}}
}

func TestAbsenceGuardRejectsBelowSnapshotUserWrite(t *testing.T) {
	t.Parallel()
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "value", true: "tombstone"}[deleted], func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := store.NewMVCCStore()
			f, ok := NewKvFSMWithHLC(s, NewHLC()).(*kvFSM)
			require.True(t, ok)
			// Fixture for a committed capability; runtime activation is separately gated.
			f.ttlInlineCapability.Store(true)
			key := []byte("metadata")
			reader, ok := s.(store.VersionPresenceReader)
			require.True(t, ok)
			exists, err := reader.VersionExistsAtOrBefore(ctx, key, 100)
			require.NoError(t, err)
			require.False(t, exists)
			// Another group's watermark gave readTS=100 while this write was parked.
			require.NoError(t, s.PutAt(ctx, key, []byte("user"), 5, 0))
			if deleted {
				require.NoError(t, s.DeleteAt(ctx, key, 6))
			}
			require.ErrorIs(t, applyFSMRequest(t, f, absenceRequest(key, true)), store.ErrWriteConflict)
			got, err := s.GetAt(ctx, key, 200)
			if deleted {
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, []byte("user"), got)
			}
		})
	}
}

func TestAbsenceGuardCapabilityAndPhaseAdmission(t *testing.T) {
	t.Parallel()
	s := store.NewMVCCStore()
	f, ok := NewKvFSMWithHLC(s, NewHLC()).(*kvFSM)
	require.True(t, ok)
	key := []byte("metadata")
	require.ErrorIs(t, applyFSMRequest(t, f, absenceRequest(key, true)), ErrTTLInlineCapabilityRequired)
	f.ttlInlineCapability.Store(true)
	for _, phase := range []pb.Phase{pb.Phase_PREPARE, pb.Phase_COMMIT, pb.Phase_ABORT} {
		r := absenceRequest(key, true)
		r.Phase = phase
		require.ErrorIs(t, f.handleRequest(context.Background(), r, 101), ErrInvalidRequest)
	}
	raw := absenceRequest(key, true)
	raw.IsTxn = false
	require.ErrorIs(t, f.handleRequest(context.Background(), raw, 101), ErrInvalidRequest)
	require.NoError(t, applyFSMRequest(t, f, absenceRequest(key, true)))
	got, err := s.GetAt(context.Background(), key, 200)
	require.NoError(t, err)
	require.Equal(t, []byte("migration"), got)
}

func TestAbsenceGuardRawBatchRefusalDoesNotHalt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = s.Close()
		}
	})
	f := g17FSM(t, s)
	guarded := g17RawPut(10, "guarded", "blocked")
	guarded.Mutations[0].RequireAbsent = true
	response := f.Apply(g17Entry(t, guarded, g17RawPut(11, "ordinary", "accepted")))
	result, ok := response.(*fsmApplyResponse)
	require.True(t, ok, "a deterministic capability refusal must not halt apply: %T", response)
	require.Len(t, result.results, 2)
	require.ErrorIs(t, result.results[0], ErrTTLInlineCapabilityRequired)
	require.NoError(t, result.results[1])
	require.False(t, g17Exists(t, s, "guarded"))
	require.True(t, g17Exists(t, s, "ordinary"))
	index, present := g17AppliedIndex(t, s)
	require.True(t, present)
	require.Equal(t, g17EntryIndex, index)
	require.NoError(t, s.Close())
	closed = true

	reopened := g17OpenStore(t, dir)
	index, present = g17AppliedIndex(t, reopened)
	require.True(t, present)
	require.Equal(t, g17EntryIndex, index, "restart must not replay a poison entry")
	require.True(t, g17Exists(t, reopened, "ordinary"))
	next := g17FSM(t, reopened)
	next.SetApplyIndex(index + 1)
	require.Nil(t, next.Apply(g17Entry(t, g17RawPut(12, "next", "accepted"))))
}

func TestAbsenceGuardLeavesLegacyRequestsUnchanged(t *testing.T) {
	t.Parallel()
	s := store.NewMVCCStore()
	f, ok := NewKvFSMWithHLC(s, NewHLC()).(*kvFSM)
	require.True(t, ok)
	key := []byte("metadata")
	require.NoError(t, applyFSMRequest(t, f, absenceRequest(key, false)))
	got, err := s.GetAt(context.Background(), key, 200)
	require.NoError(t, err)
	require.Equal(t, []byte("migration"), got)
}

func TestAbsenceGuardRejectsDuplicateThatChangesOperation(t *testing.T) {
	t.Parallel()
	s := store.NewMVCCStore()
	f, ok := NewKvFSMWithHLC(s, NewHLC()).(*kvFSM)
	require.True(t, ok)
	f.ttlInlineCapability.Store(true)
	key := []byte("metadata")
	r := absenceRequest(key, true)
	r.Mutations = append(r.Mutations, &pb.Mutation{Op: pb.Op_DEL, Key: key})
	require.ErrorIs(t, applyFSMRequest(t, f, r), ErrInvalidRequest)
	_, exists, err := s.LatestCommitTS(context.Background(), key)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestAbsenceGuardSurvivesWireAndDuplicateMutations(t *testing.T) {
	t.Parallel()
	first := elemToMutation(&Elem[OP]{Op: Put, Key: []byte("metadata"), Value: []byte("first"), RequireAbsent: true})
	encoded, err := proto.Marshal(first)
	require.NoError(t, err)
	var decoded pb.Mutation
	require.NoError(t, proto.Unmarshal(encoded, &decoded))
	require.True(t, decoded.RequireAbsent)
	last := &pb.Mutation{Op: pb.Op_PUT, Key: first.Key, Value: []byte("last")}
	mutations, err := uniqueMutations([]*pb.Mutation{&decoded, last})
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	require.True(t, mutations[0].RequireAbsent)
	require.Equal(t, []byte("last"), mutations[0].Value)
	require.False(t, last.RequireAbsent, "dedup must not mutate the original request")
	stored, err := toStoreMutations(mutations)
	require.NoError(t, err)
	require.True(t, stored[0].RequireAbsent)
}
