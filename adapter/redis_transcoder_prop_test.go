package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestRedisTranscoder_Property_Set(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		tr := newRedisTranscoder()

		req, err := tr.SetToRequest(key, value)
		require.NoError(t, err)
		require.False(t, req.IsTxn)
		require.Len(t, req.Elems, 1)
		require.Equal(t, kv.Put, req.Elems[0].Op)
		require.Equal(t, key, req.Elems[0].Key)
		require.Equal(t, value, req.Elems[0].Value)
	})
}

func TestRedisTranscoder_Property_Delete(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		tr := newRedisTranscoder()

		reqDel, err := tr.DeleteToRequest(key)
		require.NoError(t, err)
		require.False(t, reqDel.IsTxn)
		require.Len(t, reqDel.Elems, 1)
		require.Equal(t, kv.Del, reqDel.Elems[0].Op)
		require.Equal(t, key, reqDel.Elems[0].Key)
	})
}
