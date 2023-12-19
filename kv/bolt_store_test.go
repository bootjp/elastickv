package kv

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func mustStore(store Store, err error) Store {
	if err != nil {
		panic(err)
	}
	return store
}

func TestBoltStore(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	d := t.TempDir()
	st := mustStore(NewBoltStore(d + "/bolt.db"))

	for i := 0; i < 999; i++ {
		key := []byte("foo" + strconv.Itoa(i))
		err := st.Put(ctx, key, []byte("bar"))
		assert.NoError(t, err)

		res, err := st.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, []byte("bar"), res)
		assert.NoError(t, st.Delete(ctx, key))
		// bolt store does not support NotFound
		res, err = st.Get(ctx, []byte("aaaaaa"))
		assert.NoError(t, err)
		assert.Nil(t, res)
	}
}

func TestBoltStore_Txn(t *testing.T) {
	t.Parallel()
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		d := t.TempDir()
		st := mustStore(NewBoltStore(d + "bolt.db"))
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			return nil
		})
		assert.NoError(t, err)
	})
	t.Run("error", func(t *testing.T) {
		ctx := context.Background()
		d := t.TempDir()
		st := mustStore(NewBoltStore(d + "bolt.db"))
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			return errors.New("error")
		})
		assert.Error(t, err)
	})

	t.Run("parallel", func(t *testing.T) {
		ctx := context.Background()
		d := t.TempDir()
		st := mustStore(NewBoltStore(d + "bolt.db"))
		wg := &sync.WaitGroup{}

		for i := 0; i < 9999; i++ {
			wg.Add(1)
			go func(i int) {
				key := []byte("foo" + strconv.Itoa(i))
				err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
					err := txn.Put(ctx, key, []byte("bar"))
					assert.NoError(t, err)

					res, err := txn.Get(ctx, key)
					assert.NoError(t, err)

					assert.Equal(t, []byte("bar"), res)
					assert.NoError(t, txn.Delete(ctx, key))

					res, err = txn.Get(ctx, key)
					assert.NoError(t, err)
					assert.Nil(t, res)
					res, err = txn.Get(ctx, []byte("aaaaaa"))
					assert.NoError(t, err)
					assert.Nil(t, res)
					return nil
				})
				assert.NoError(t, err)
				wg.Done()
			}(i)
		}
		wg.Wait()
	})
}
