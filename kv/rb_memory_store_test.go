package kv

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestRbMemoryStore(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	st := NewRbMemoryStore()
	wg := &sync.WaitGroup{}
	for i := 0; i < 9999; i++ {
		wg.Add(1)
		go func(i int) {
			key := []byte(strconv.Itoa(i) + "foo")
			err := st.Put(ctx, key, []byte("bar"))
			assert.NoError(t, err)

			res, err := st.Get(ctx, key)
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, st.Delete(ctx, key))

			res, err = st.Get(ctx, key)
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)
			res, err = st.Get(ctx, []byte("aaaaaa"))
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestRbMemoryStore_Txn(t *testing.T) {
	t.Parallel()
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		st := NewRbMemoryStore()
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)

			err = txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err = txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("bar"), res)

			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("rollback case", func(t *testing.T) {
		var ErrAbort = errors.New("abort")
		st := NewRbMemoryStore()
		ctx := context.Background()
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {

			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)
			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)
			return ErrAbort
		})
		assert.ErrorContains(t, err, ErrAbort.Error())
		res, err := st.Get(ctx, []byte("foo"))
		assert.ErrorIs(t, ErrKeyNotFound, err)
		assert.Nil(t, res)
		res, err = st.Get(ctx, []byte("aaaaaa"))
		assert.ErrorIs(t, ErrKeyNotFound, err)
		assert.Nil(t, res)
	})

	t.Run("parallel", func(t *testing.T) {
		ctx := context.Background()
		st := NewRbMemoryStore()
		wg := &sync.WaitGroup{}
		for i := 0; i < 9999; i++ {
			wg.Add(1)
			go func(i int) {
				err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
					key := []byte(strconv.Itoa(i) + "foo")
					err := txn.Put(ctx, key, []byte("bar"))
					assert.NoError(t, err)

					res, err := txn.Get(ctx, key)
					assert.NoError(t, err)

					assert.Equal(t, []byte("bar"), res)
					assert.NoError(t, txn.Delete(ctx, key))

					res, err = txn.Get(ctx, key)
					assert.ErrorIs(t, ErrKeyNotFound, err)
					assert.Nil(t, res)
					res, err = txn.Get(ctx, []byte("aaaaaa"))
					assert.ErrorIs(t, ErrKeyNotFound, err)
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

func TestRbMemoryStore_TTL(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	st := NewRbMemoryStoreWithExpire(time.Second)
	wg := &sync.WaitGroup{}
	for i := 0; i < 9999; i++ {
		wg.Add(1)
		go func(i int) {
			key := []byte(strconv.Itoa(i) + "foo")
			err := st.PutWithTTL(ctx, key, []byte("bar"), 1)
			assert.NoError(t, err)

			res, err := st.Get(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, []byte("bar"), res)

			time.Sleep(11 * time.Second)

			res, err = st.Get(ctx, key)
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)

			// ticker is called not only once, but also after the second time
			err = st.PutWithTTL(ctx, key, []byte("bar"), 1)
			assert.NoError(t, err)

			res, err = st.Get(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, []byte("bar"), res)

			time.Sleep(11 * time.Second)

			res, err = st.Get(ctx, key)
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestRbMemoryStore_TTL_Txn(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	st := NewRbMemoryStoreWithExpire(time.Second)
	wg := &sync.WaitGroup{}
	for i := 0; i < 9999; i++ {
		wg.Add(1)
		go func(i int) {
			key := []byte(strconv.Itoa(i) + "foo")
			err := st.TxnWithTTL(ctx, func(ctx context.Context, txn TTLTxn) error {
				err := txn.PutWithTTL(ctx, key, []byte("bar"), 1)
				assert.NoError(t, err)

				res, err := txn.Get(ctx, key)
				assert.NoError(t, err)
				assert.Equal(t, []byte("bar"), res)

				// wait for ttl
				go func(key []byte) {
					time.Sleep(11 * time.Second)

					res, err = st.Get(ctx, key)
					assert.ErrorIs(t, ErrKeyNotFound, err)
					assert.Nil(t, res)
					wg.Done()
				}(key)
				return nil
			})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}