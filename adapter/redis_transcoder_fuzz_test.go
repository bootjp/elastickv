package adapter

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func FuzzRedisTranscoder(f *testing.F) {
	f.Add([]byte("key1"), []byte("value1"))
	f.Add([]byte(""), []byte(""))

	f.Fuzz(func(t *testing.T, key []byte, value []byte) {
		tr := newRedisTranscoder()

		// Test SetToRequest
		req, err := tr.SetToRequest(key, value)
		require.NoError(t, err)
		require.NotNil(t, req)
		require.False(t, req.IsTxn)
		require.Len(t, req.Elems, 1)
		require.Equal(t, kv.Put, req.Elems[0].Op)
		require.True(t, bytes.Equal(key, req.Elems[0].Key))
		require.True(t, bytes.Equal(value, req.Elems[0].Value))

		// Test DeleteToRequest
		reqDel, err := tr.DeleteToRequest(key)
		require.NoError(t, err)
		require.NotNil(t, reqDel)
		require.False(t, reqDel.IsTxn)
		require.Len(t, reqDel.Elems, 1)
		require.Equal(t, kv.Del, reqDel.Elems[0].Op)
		require.True(t, bytes.Equal(key, reqDel.Elems[0].Key))
	})
}
