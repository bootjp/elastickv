package fstest_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type mutationCaptureStore struct {
	store.MVCCStore
	lastTS    uint64
	commitTS  uint64
	startTS   uint64
	mutations []*store.KVPairMutation
	readKeys  [][]byte
	applyErr  error
	called    bool
}

func (s *mutationCaptureStore) LastCommitTS() uint64 { return s.lastTS }

func (s *mutationCaptureStore) ApplyMutations(_ context.Context, mutations []*store.KVPairMutation, reads [][]byte, startTS, commitTS uint64) error {
	s.called = true
	s.mutations, s.readKeys = mutations, reads
	s.startTS, s.commitTS = startTS, commitTS
	return s.applyErr
}

func TestMemoryDispatcherTimestampPatchBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int
		offset uint64
		valid  bool
	}{
		{name: "empty unpatched", valid: true},
		{name: "short unpatched", size: 3, valid: true},
		{name: "zero disables patch", size: 8, valid: true},
		{name: "short patched", size: 7, offset: 1},
		{name: "exact width nonzero offset", size: 8, offset: 1},
		{name: "first offset", size: 9, offset: 1, valid: true},
		{name: "last offset", size: 12, offset: 4, valid: true},
		{name: "interior offset", size: 12, offset: 2, valid: true},
		{name: "one past last", size: 12, offset: 5},
		{name: "overflow offset", size: 12, offset: ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &mutationCaptureStore{lastTS: 40}
			value := bytes.Repeat([]byte{0xa5}, tc.size)
			original := bytes.Clone(value)
			req := &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Put, Key: []byte("key"), Value: value, CommitTSValueOffset: tc.offset},
			}}
			response, err := fstest.NewMemoryDispatcher(st).Dispatch(context.Background(), req)
			require.Equal(t, original, value)
			if !tc.valid {
				require.ErrorIs(t, err, filesystem.ErrInvalid)
				require.Nil(t, response)
				require.False(t, st.called)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(41), response.CommitIndex)
			require.Len(t, st.mutations, 1)
			want := append([]byte(nil), value...)
			if tc.offset != 0 {
				binary.BigEndian.PutUint64(want[tc.offset:], response.CommitIndex)
			}
			require.Equal(t, want, st.mutations[0].Value)
		})
	}
}

func TestMemoryDispatcherCopiesMutationInputs(t *testing.T) {
	st := &mutationCaptureStore{}
	putKey, delKey, value := []byte("put"), []byte("del"), []byte("value")
	_, err := fstest.NewMemoryDispatcher(st).Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: putKey, Value: value},
			{Op: kv.Del, Key: delKey, Value: []byte("ignored"), CommitTSValueOffset: 0},
		},
	})
	require.NoError(t, err)
	require.Len(t, st.mutations, 2)
	putKey[0], delKey[0], value[0] = 'X', 'X', 'X'
	require.Equal(t, []byte("put"), st.mutations[0].Key)
	require.Equal(t, []byte("value"), st.mutations[0].Value)
	require.Equal(t, store.OpTypePut, st.mutations[0].Op)
	require.Equal(t, []byte("del"), st.mutations[1].Key)
	require.Equal(t, store.OpTypeDelete, st.mutations[1].Op)
	require.Nil(t, st.mutations[1].Value)
	st.mutations[0].Key[1], st.mutations[0].Value[1] = 'Y', 'Y'
	require.Equal(t, []byte("Xut"), putKey)
	require.Equal(t, []byte("Xalue"), value)
}

func TestMemoryDispatcherTimestampSelectionAndOCC(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lastTS   uint64
		commitTS uint64
		startTS  uint64
		isTxn    bool
		want     uint64
	}{
		{name: "next applied", lastTS: 20, commitTS: 10, want: 21},
		{name: "requested", lastTS: 20, commitTS: 30, want: 30},
		{name: "transaction floor", lastTS: 20, commitTS: 30, startTS: 30, isTxn: true, want: 31},
		{name: "transaction already newer", lastTS: 20, startTS: 10, isTxn: true, want: 21},
		{name: "nontransaction ignores start", lastTS: 20, startTS: 30, want: 21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &mutationCaptureStore{lastTS: tc.lastTS, applyErr: store.ErrWriteConflict}
			reads := [][]byte{[]byte("first"), []byte("second")}
			response, err := fstest.NewMemoryDispatcher(st).Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{
				CommitTS: tc.commitTS, StartTS: tc.startTS, IsTxn: tc.isTxn, ReadKeys: reads,
			})
			require.ErrorIs(t, err, store.ErrWriteConflict)
			require.EqualError(t, err, "apply memory dispatcher mutations: write conflict")
			require.Nil(t, response)
			require.True(t, st.called)
			require.Equal(t, tc.want, st.commitTS)
			require.Equal(t, tc.startTS, st.startTS)
			require.Equal(t, reads, st.readKeys)
		})
	}
}

func TestMemoryDispatcherRejectsUnsupportedOperationsBeforeApply(t *testing.T) {
	for _, tc := range []struct {
		name   string
		op     kv.OP
		offset uint64
		want   error
	}{
		{name: "prefix delete", op: kv.DelPrefix, want: store.ErrUnknownOp},
		{name: "unknown operation", op: kv.OP(-1), want: store.ErrUnknownOp},
		{name: "patch checked before opcode", op: kv.DelPrefix, offset: 1, want: filesystem.ErrInvalid},
		{name: "delete patch checked", op: kv.Del, offset: 1, want: filesystem.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &mutationCaptureStore{}
			response, err := fstest.NewMemoryDispatcher(st).Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{
				Elems: []*kv.Elem[kv.OP]{
					{Op: kv.Put, Key: []byte("not applied"), Value: []byte("value")},
					{Op: tc.op, CommitTSValueOffset: tc.offset},
				},
			})
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, response)
			require.False(t, st.called)
		})
	}
}
