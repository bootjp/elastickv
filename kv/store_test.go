package kv

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var stores = []Store{
	NewMemoryStore(),
	mustStore(NewBoltStore(os.TempDir() + "/bolt.db")),
}

func mustStore(store Store, err error) Store {
	if err != nil {
		panic(err)
	}
	return store
}

func Test(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	for _, st := range stores {

		t.Run(st.Name(), func(t *testing.T) {

			err := st.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := st.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, st.Delete(ctx, []byte("foo")))

			// bolt store does not support NotFound
			// todo migrate Store.SupportedNotFound()
			if st.Name() != "bolt" {
				res, err = st.Get(ctx, []byte("foo"))
				assert.ErrorIs(t, ErrNotFound, err)
				assert.Nil(t, res)
				res, err = st.Get(ctx, []byte("aaaaaa"))
				assert.ErrorIs(t, ErrNotFound, err)
				assert.Nil(t, res)
			}
		})
	}
}
