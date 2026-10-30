package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/bootjp/elastickv/store/storetest"
	"github.com/stretchr/testify/require"
)

func TestFSMTimestampBoundRawApplyAndRetry(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			fsm := g17FSM(t, st)
			fsm.timestampBindingEnabled.Store(true)
			request := g17RawPut(100, "bound", "first")
			request.TimestampBindingOperationId = make([]byte, len(testTimestampBinding().OperationID))
			request.TimestampBindingOperationId[0] = 1
			data := g17Entry(t, request)
			require.Nil(t, fsm.Apply(data))
			got, err := st.GetAt(t.Context(), []byte("bound"), 100)
			require.NoError(t, err)
			require.Equal(t, []byte("first"), got)

			fsm.SetApplyIndex(g17EntryIndex + 1)
			require.Nil(t, fsm.Apply(data), "exact retry must reuse the durable binding")
			changed := g17RawPut(100, "bound", "different")
			changed.TimestampBindingOperationId = request.TimestampBindingOperationId
			fsm.SetApplyIndex(g17EntryIndex + 2)
			response, ok := fsm.Apply(g17Entry(t, changed)).(error)
			require.True(t, ok)
			require.ErrorIs(t, response, ErrTimestampBindingConflict)
			other := g17RawPut(100, "other", "value")
			other.TimestampBindingOperationId = make([]byte, len(testTimestampBinding().OperationID))
			other.TimestampBindingOperationId[0] = 2
			response, ok = fsm.Apply(g17Entry(t, other)).(error)
			require.True(t, ok)
			require.ErrorIs(t, response, ErrTimestampBindingConflict)
			got, err = st.GetAt(t.Context(), []byte("bound"), 100)
			require.NoError(t, err)
			require.Equal(t, []byte("first"), got)
			_, err = st.GetAt(t.Context(), []byte("other"), 100)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestFSMTimestampBindingDisabledByDefault(t *testing.T) {
	st := store.NewMVCCStore()
	fsm := g17FSM(t, st)
	request := g17RawPut(100, "bound", "value")
	request.TimestampBindingOperationId = []byte{1}
	response, ok := fsm.Apply(g17Entry(t, request)).(error)
	require.True(t, ok)
	require.ErrorIs(t, response, ErrTimestampBindingInvalid)
	_, err := st.GetAt(t.Context(), []byte("bound"), 100)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.ErrorIs(t, StampTimestampAdmission(t.Context(), nil, st, []*pb.Request{request}), ErrTimestampBindingInvalid)
}

func TestFSMTimestampBoundTxnCreation(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
			t.Run(name+"/"+phase.String(), func(t *testing.T) {
				st := factory(t)
				fsm := g17FSM(t, st)
				fsm.timestampBindingEnabled.Store(true)
				if phase == pb.Phase_PREPARE {
					require.Nil(t, fsm.Apply(EncodePrepareEvidenceFence(0)))
				}
				request := preparedEvidenceRequest(phase, 20)
				request.TimestampBindingOperationId = make([]byte, len(testTimestampBinding().OperationID))
				request.TimestampBindingOperationId[0] = 1
				data := g17Entry(t, request)
				require.Nil(t, fsm.Apply(data))
				binding, err := timestampBindingForCreationRequest(request)
				require.NoError(t, err)
				require.Equal(t, uint64(20), binding.Timestamp)
				metadata, _ := testTimestampBindingStores(t, st)
				operationKey, timestampKey := binding.keys()
				_, err = metadata.ReadRaftControlMetadata(t.Context(), operationKey)
				require.NoError(t, err)
				_, err = metadata.ReadRaftControlMetadata(t.Context(), timestampKey)
				require.NoError(t, err)
				fsm.SetApplyIndex(g17EntryIndex + 1)
				require.Nil(t, fsm.Apply(data), "exact retry must not apply the transaction twice")
				if phase == pb.Phase_NONE {
					value, err := st.GetAt(t.Context(), []byte("primary"), 20)
					require.NoError(t, err)
					require.Equal(t, []byte("value"), value)
				} else {
					_, err := st.GetAt(t.Context(), txnLockKey([]byte("primary")), ^uint64(0))
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestFSMTimestampBoundTxnConflictRollsBackBinding(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore { return storetest.NewPebble(t) },
	} {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			fsm := g17FSM(t, st)
			fsm.timestampBindingEnabled.Store(true)
			first := preparedEvidenceRequest(pb.Phase_NONE, 20)
			first.TimestampBindingOperationId = append([]byte{1}, make([]byte, 15)...)
			require.Nil(t, fsm.Apply(g17Entry(t, first)))

			second := preparedEvidenceRequest(pb.Phase_NONE, 30)
			second.TimestampBindingOperationId = append([]byte{2}, make([]byte, 15)...)
			fsm.SetApplyIndex(g17EntryIndex + 1)
			response, ok := fsm.Apply(g17Entry(t, second)).(error)
			require.True(t, ok)
			require.ErrorIs(t, response, store.ErrWriteConflict)
			metadata, _ := testTimestampBindingStores(t, st)
			_, found, err := LookupTimestampBinding(t.Context(), metadata, [16]byte{2})
			require.NoError(t, err)
			require.False(t, found)
			value, err := st.GetAt(t.Context(), []byte("primary"), 30)
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
		})
	}
}
