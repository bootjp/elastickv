package kv

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	st := NewMemoryStore()
	for i := 0; i < 99999; i++ {
		go func(i int) {
			key := []byte(strconv.Itoa(i) + "foo")
			err := st.Put(ctx, key, []byte("bar"))
			assert.NoError(t, err)

			res, err := st.Get(ctx, key)
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, st.Delete(ctx, key))

			res, err = st.Get(ctx, key)
			assert.ErrorIs(t, ErrNotFound, err)
			assert.Nil(t, res)
			res, err = st.Get(ctx, []byte("aaaaaa"))
			assert.ErrorIs(t, ErrNotFound, err)
			assert.Nil(t, res)
		}(i)
	}
}
