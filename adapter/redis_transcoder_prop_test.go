package adapter

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"pgregory.net/rapid"
)

func TestRedisTranscoder_Property_Set(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		tr := newRedisTranscoder()

		req, err := tr.SetToRequest(key, value)
		if err != nil {
			t.Fatalf("SetToRequest failed: %v", err)
		}
		if req.IsTxn {
			t.Error("SetToRequest should not be a transaction")
		}
		if len(req.Elems) != 1 || req.Elems[0].Op != kv.Put || !bytes.Equal(req.Elems[0].Key, key) || !bytes.Equal(req.Elems[0].Value, value) {
			t.Errorf("SetToRequest mapping incorrect")
		}
	})
}

func TestRedisTranscoder_Property_Delete(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		tr := newRedisTranscoder()

		reqDel, err := tr.DeleteToRequest(key)
		if err != nil {
			t.Fatalf("DeleteToRequest failed: %v", err)
		}
		if reqDel.IsTxn {
			t.Error("DeleteToRequest should not be a transaction")
		}
		if len(reqDel.Elems) != 1 || reqDel.Elems[0].Op != kv.Del || !bytes.Equal(reqDel.Elems[0].Key, key) {
			t.Errorf("DeleteToRequest mapping incorrect")
		}
	})
}
