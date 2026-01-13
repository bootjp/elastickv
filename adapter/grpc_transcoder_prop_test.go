package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestGrpcTranscoder_Property_RawPut(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		tr := newGrpcGrpcTranscoder()

		putReq := &pb.RawPutRequest{Key: key, Value: value}
		gotPut, err := tr.RawPutToRequest(putReq)
		require.NoError(t, err)
		require.False(t, gotPut.IsTxn)
		require.Len(t, gotPut.Elems, 1)
		require.Equal(t, kv.Put, gotPut.Elems[0].Op)
		require.Equal(t, key, gotPut.Elems[0].Key)
		require.Equal(t, value, gotPut.Elems[0].Value)
	})
}

func TestGrpcTranscoder_Property_RawDelete(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		tr := newGrpcGrpcTranscoder()

		delReq := &pb.RawDeleteRequest{Key: key}
		gotDel, err := tr.RawDeleteToRequest(delReq)
		require.NoError(t, err)
		require.False(t, gotDel.IsTxn)
		require.Len(t, gotDel.Elems, 1)
		require.Equal(t, kv.Del, gotDel.Elems[0].Op)
		require.Equal(t, key, gotDel.Elems[0].Key)
	})
}

func TestGrpcTranscoder_Property_TxnOps(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		tr := newGrpcGrpcTranscoder()

		// TransactionalPut
		txPutReq := &pb.PutRequest{Key: key, Value: value}
		gotTxPut, err := tr.TransactionalPutToRequests(txPutReq)
		require.NoError(t, err)
		require.True(t, gotTxPut.IsTxn)
		require.Len(t, gotTxPut.Elems, 1)
		require.Equal(t, kv.Put, gotTxPut.Elems[0].Op)
		require.Equal(t, key, gotTxPut.Elems[0].Key)
		require.Equal(t, value, gotTxPut.Elems[0].Value)

		// TransactionalDelete
		txDelReq := &pb.DeleteRequest{Key: key}
		gotTxDel, err := tr.TransactionalDeleteToRequests(txDelReq)
		require.NoError(t, err)
		require.True(t, gotTxDel.IsTxn)
		require.Len(t, gotTxDel.Elems, 1)
		require.Equal(t, kv.Del, gotTxDel.Elems[0].Op)
		require.Equal(t, key, gotTxDel.Elems[0].Key)
	})
}
