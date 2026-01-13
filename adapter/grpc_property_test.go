package adapter

import (
	"bytes"
	"testing"
	"testing/quick"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
)

func TestGrpcTranscoderProperty(t *testing.T) {
	transcoder := newGrpcGrpcTranscoder()

	rawPutProperty := func(key, value []byte) bool {
		group, err := transcoder.RawPutToRequest(&pb.RawPutRequest{Key: key, Value: value})
		if err != nil || group == nil {
			return false
		}
		if group.IsTxn || len(group.Elems) != 1 {
			return false
		}
		elem := group.Elems[0]
		return elem.Op == kv.Put && bytes.Equal(elem.Key, key) && bytes.Equal(elem.Value, value)
	}

	rawDeleteProperty := func(key []byte) bool {
		group, err := transcoder.RawDeleteToRequest(&pb.RawDeleteRequest{Key: key})
		if err != nil || group == nil {
			return false
		}
		if group.IsTxn || len(group.Elems) != 1 {
			return false
		}
		elem := group.Elems[0]
		return elem.Op == kv.Del && bytes.Equal(elem.Key, key)
	}

	txnPutProperty := func(key, value []byte) bool {
		group, err := transcoder.TransactionalPutToRequests(&pb.PutRequest{Key: key, Value: value})
		if err != nil || group == nil {
			return false
		}
		if !group.IsTxn || len(group.Elems) != 1 {
			return false
		}
		elem := group.Elems[0]
		return elem.Op == kv.Put && bytes.Equal(elem.Key, key) && bytes.Equal(elem.Value, value)
	}

	txnDeleteProperty := func(key []byte) bool {
		group, err := transcoder.TransactionalDeleteToRequests(&pb.DeleteRequest{Key: key})
		if err != nil || group == nil {
			return false
		}
		if !group.IsTxn || len(group.Elems) != 1 {
			return false
		}
		elem := group.Elems[0]
		return elem.Op == kv.Del && bytes.Equal(elem.Key, key)
	}

	if err := quick.Check(rawPutProperty, nil); err != nil {
		t.Fatal(err)
	}
	if err := quick.Check(rawDeleteProperty, nil); err != nil {
		t.Fatal(err)
	}
	if err := quick.Check(txnPutProperty, nil); err != nil {
		t.Fatal(err)
	}
	if err := quick.Check(txnDeleteProperty, nil); err != nil {
		t.Fatal(err)
	}
}
