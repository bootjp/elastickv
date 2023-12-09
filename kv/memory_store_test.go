package kv

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"

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
			assert.NoError(t, st.Delete(ctx, []byte("foo")))

			// bolt store does not support NotFound
			// todo migrate Store.SupportedNotFound()
			if st.Name() != "bolt" {
				res, err = st.Get(ctx, key)
				h, err := st.hash(key)
				assert.NoError(t, err)
				assert.ErrorIs(t, ErrNotFound, err, "key: %s", string(key), "hash: %d", h)
				if !errors.Is(err, ErrNotFound) {
					fmt.Print(err)
				}
				assert.Nil(t, res)
				res, err = st.Get(ctx, []byte("aaaaaa"))
				assert.ErrorIs(t, ErrNotFound, err, "key: %s", string(key), "hash: %d", h)
				assert.Nil(t, res)
			}
		}(i)
	}
}
