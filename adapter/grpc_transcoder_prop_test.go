package adapter

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"pgregory.net/rapid"
)

func TestGrpcTranscoder_Property_RawPut(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		tr := newGrpcGrpcTranscoder()

		putReq := &pb.RawPutRequest{Key: key, Value: value}
		gotPut, err := tr.RawPutToRequest(putReq)
		if err != nil {
			t.Fatalf("RawPutToRequest failed: %v", err)
		}
		if gotPut.IsTxn || len(gotPut.Elems) != 1 || gotPut.Elems[0].Op != kv.Put ||
			!bytes.Equal(gotPut.Elems[0].Key, key) || !bytes.Equal(gotPut.Elems[0].Value, value) {
			t.Error("RawPutToRequest mapping incorrect")
		}
	})
}

func TestGrpcTranscoder_Property_RawDelete(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		tr := newGrpcGrpcTranscoder()

		delReq := &pb.RawDeleteRequest{Key: key}
		gotDel, err := tr.RawDeleteToRequest(delReq)
		if err != nil {
			t.Fatalf("RawDeleteToRequest failed: %v", err)
		}
		if gotDel.IsTxn || len(gotDel.Elems) != 1 || gotDel.Elems[0].Op != kv.Del ||
			!bytes.Equal(gotDel.Elems[0].Key, key) {
			t.Error("RawDeleteToRequest mapping incorrect")
		}
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
		if err != nil {
			t.Fatalf("TransactionalPutToRequests failed: %v", err)
		}
		if !gotTxPut.IsTxn || len(gotTxPut.Elems) != 1 {
			t.Error("TransactionalPutToRequests should be a transaction")
		}

		// TransactionalDelete
		txDelReq := &pb.DeleteRequest{Key: key}
		gotTxDel, err := tr.TransactionalDeleteToRequests(txDelReq)
		if err != nil {
			t.Fatalf("TransactionalDeleteToRequests failed: %v", err)
		}
		if !gotTxDel.IsTxn || len(gotTxDel.Elems) != 1 {
			t.Error("TransactionalDeleteToRequests should be a transaction")
		}
	})
}
