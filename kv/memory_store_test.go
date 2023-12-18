package kv

import (
	"context"
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

func TestMemoryStore_Txn(t *testing.T) {
	t.Parallel()
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()

		st := NewMemoryStore()
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.ErrorIs(t, ErrNotFound, err)
			assert.Nil(t, res)
			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.ErrorIs(t, ErrNotFound, err)
			assert.Nil(t, res)
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("rollback case", func(t *testing.T) {
		var ErrAbort = errors.New("abort")
		st := NewMemoryStore()
		ctx := context.Background()
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {

			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.ErrorIs(t, ErrNotFound, err)
			assert.Nil(t, res)
			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.ErrorIs(t, ErrNotFound, err)
			assert.Nil(t, res)
			return ErrAbort
		})
		assert.ErrorContains(t, err, ErrAbort.Error())
		res, err := st.Get(ctx, []byte("foo"))
		assert.ErrorIs(t, ErrNotFound, err)
		assert.Nil(t, res)
		res, err = st.Get(ctx, []byte("aaaaaa"))
		assert.ErrorIs(t, ErrNotFound, err)
		assert.Nil(t, res)
	})
}
