package adapter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLuaFlushTTLForDeletedKey_DirtyStateClearsBufferedTTL(t *testing.T) {
	t.Parallel()
	b := newTTLBufferWithMaxSize(8)
	expireAt := time.Now().Add(time.Minute)
	b.Set([]byte("k"), &expireAt)

	c := &luaScriptContext{
		server: &RedisServer{ttlBuffer: b},
		ttls: map[string]*luaTTLState{
			"k": {dirty: true},
		},
	}

	c.flushTTLForKeyToBuffer("k", redisTypeNone, false)

	got, found := b.Get([]byte("k"))
	require.True(t, found)
	require.Nil(t, got)
}

func TestLuaFlushTTLForDeletedKey_CleanStateDoesNotWriteBuffer(t *testing.T) {
	t.Parallel()
	b := newTTLBufferWithMaxSize(8)
	c := &luaScriptContext{
		server: &RedisServer{ttlBuffer: b},
		ttls: map[string]*luaTTLState{
			"k": {dirty: false},
		},
	}

	c.flushTTLForKeyToBuffer("k", redisTypeNone, false)

	_, found := b.Get([]byte("k"))
	require.False(t, found)
}
