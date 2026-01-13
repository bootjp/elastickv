package adapter

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func FuzzGrpcTranscoder(f *testing.F) {
	f.Add([]byte("key"), []byte("value"))
	f.Fuzz(func(t *testing.T, key, value []byte) {
		tr := newGrpcGrpcTranscoder()

		// RawPut
		putReq := &pb.RawPutRequest{Key: key, Value: value}
		gotPut, err := tr.RawPutToRequest(putReq)
		require.NoError(t, err)
		require.False(t, gotPut.IsTxn)
		require.Len(t, gotPut.Elems, 1)
		require.Equal(t, kv.Put, gotPut.Elems[0].Op)
		require.True(t, bytes.Equal(key, gotPut.Elems[0].Key))
		require.True(t, bytes.Equal(value, gotPut.Elems[0].Value))

		// RawDelete
		delReq := &pb.RawDeleteRequest{Key: key}
		gotDel, err := tr.RawDeleteToRequest(delReq)
		require.NoError(t, err)
		require.False(t, gotDel.IsTxn)
		require.Len(t, gotDel.Elems, 1)
		require.Equal(t, kv.Del, gotDel.Elems[0].Op)
		require.True(t, bytes.Equal(key, gotDel.Elems[0].Key))

		// TransactionalPut
		txPutReq := &pb.PutRequest{Key: key, Value: value}
		gotTxPut, err := tr.TransactionalPutToRequests(txPutReq)
		require.NoError(t, err)
		require.True(t, gotTxPut.IsTxn)
		require.Len(t, gotTxPut.Elems, 1)

		// TransactionalDelete
		txDelReq := &pb.DeleteRequest{Key: key}
		gotTxDel, err := tr.TransactionalDeleteToRequests(txDelReq)
		require.NoError(t, err)
		require.True(t, gotTxDel.IsTxn)
		require.Len(t, gotTxDel.Elems, 1)
	})
}
