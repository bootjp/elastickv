package kv

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestDynamoAppendDistinctRaftEntryCannotAcknowledgeTimestampCollision(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plaintext", true: "encrypted"}[encrypted], func(t *testing.T) {
			ctx := context.Background()
			var opts []store.PebbleStoreOption
			if encrypted {
				keys := encryption.NewKeystore()
				require.NoError(t, keys.Set(1, bytes.Repeat([]byte{0x37}, encryption.KeySize)))
				cipher, err := encryption.NewCipher(keys)
				require.NoError(t, err)
				opts = append(opts, store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(1, 1), func() (uint32, bool) { return 1, true }))
			}
			st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "fsm.db"), opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
			require.True(t, ok)
			key := []byte(DynamoItemPrefix + "amVwc2VuX2FwcGVuZA|2|16\x00\x01")
			// Minimized from the same committed WAL that produced the missing
			// acknowledged append37/key16: distinct entries371/372 in term5
			// carried different item values at this identical commit timestamp.
			const collisionTS uint64 = 117485124856709124
			first := dynamoCollisionItem(t, "43")
			lost := dynamoCollisionItem(t, "37")
			fsm.SetApplyIndex(371)
			require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(collisionTS-1, collisionTS, 0, key, first)))
			// An actual replay of entry371 remains idempotent.
			require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(collisionTS-1, collisionTS, 0, key, first)))
			fsm.SetApplyIndex(372)
			err = applyFSMRequest(t, fsm, onePhaseReq(collisionTS-2, collisionTS, 0, key, lost))
			require.ErrorIs(t, err, store.ErrWriteConflict, "another entry must not return success without writing its value")
			got, err := st.GetAt(ctx, key, collisionTS)
			require.NoError(t, err)
			require.Equal(t, first, got)
			// Re-read/recompute at a fresh timestamp preserves both appends.
			both := dynamoCollisionItem(t, "43", "37")
			fsm.SetApplyIndex(373)
			require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(collisionTS, collisionTS+1, 0, key, both)))
			got, err = st.GetAt(ctx, key, collisionTS+1)
			require.NoError(t, err)
			require.Equal(t, both, got)
		})
	}
}

func dynamoCollisionItem(t *testing.T, values ...string) []byte {
	t.Helper()
	list := &pb.DynamoAttributeValueList{}
	for _, value := range values {
		list.Values = append(list.Values, &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_N{N: value}})
	}
	item := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"pk":  {Value: &pb.DynamoAttributeValue_S{S: "16"}},
		"val": {Value: &pb.DynamoAttributeValue_L{L: list}},
	}}
	body, err := (proto.MarshalOptions{Deterministic: true}).Marshal(item)
	require.NoError(t, err)
	return append([]byte{0, 'D', 'I', 1}, body...)
}
