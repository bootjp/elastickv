package kv

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mustStore(store Store, err error) Store {
	if err != nil {
		panic(err)
	}
	return store
}

func TestStore(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	st := mustStore(NewBoltStore(os.TempDir() + "/bolt.db"))

	t.Run(st.Name(), func(t *testing.T) {
		for i := 0; i < 99999; i++ {
			go func(i int) {
				key := []byte("foo" + strconv.Itoa(i))
				err := st.Put(ctx, key, []byte("bar"))
				assert.NoError(t, err)

				res, err := st.Get(ctx, key)
				assert.NoError(t, err)
				assert.Equal(t, []byte("bar"), res)

				assert.NoError(t, st.Delete(ctx, key))
				// bolt store does not support NotFound
				assert.Nil(t, res)
			}(i)
		}
	})
}
