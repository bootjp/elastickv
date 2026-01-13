package adapter

import (
	"bytes"
	"testing"
	"testing/quick"

	"github.com/bootjp/elastickv/kv"
)

func TestRedisTranscoderProperty(t *testing.T) {
	transcoder := newRedisTranscoder()

	setProperty := func(key, value []byte) bool {
		group, err := transcoder.SetToRequest(key, value)
		if err != nil || group == nil {
			return false
		}
		if group.IsTxn || len(group.Elems) != 1 {
			return false
		}
		elem := group.Elems[0]
		return elem.Op == kv.Put && bytes.Equal(elem.Key, key) && bytes.Equal(elem.Value, value)
	}

	deleteProperty := func(key []byte) bool {
		group, err := transcoder.DeleteToRequest(key)
		if err != nil || group == nil {
			return false
		}
		if group.IsTxn || len(group.Elems) != 1 {
			return false
		}
		elem := group.Elems[0]
		return elem.Op == kv.Del && bytes.Equal(elem.Key, key)
	}

	if err := quick.Check(setProperty, nil); err != nil {
		t.Fatal(err)
	}
	if err := quick.Check(deleteProperty, nil); err != nil {
		t.Fatal(err)
	}
}
