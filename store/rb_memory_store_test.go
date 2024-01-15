package store

import (
	"context"
	"encoding/binary"
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

func TestRbMemoryStore_Scan(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	st := NewRbMemoryStore()

	for i := 0; i < 9999; i++ {
		keyStr := "prefix " + strconv.Itoa(i) + "foo"
		key := []byte(keyStr)
		b := make([]byte, 8)
		binary.PutVarint(b, int64(i))
		err := st.Put(ctx, key, b)
		assert.NoError(t, err)
	}

	res, err := st.Scan(ctx, []byte("prefix"), []byte("z"), 100)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(res))

	sortedKVPairs := make([]*KVPair, 9999)

	for _, re := range res {
		str := string(re.Key)
		i, err := strconv.Atoi(str[7 : len(str)-3])
		assert.NoError(t, err)
		sortedKVPairs[i] = re
	}

	cnt := 0
	for i, v := range sortedKVPairs {
		if v == nil {
			continue
		}
		cnt++
		n, _ := binary.Varint(v.Value)
		assert.NoError(t, err)

		assert.Equal(t, int64(i), n)
		assert.Equal(t, []byte("prefix "+strconv.Itoa(i)+"foo"), v.Key)
	}

	assert.Equal(t, 100, cnt)
}

func TestRbMemoryStore_Txn(t *testing.T) {
	t.Parallel()
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		st := NewRbMemoryStore()

		// put outside txn
		// read inside txn
		assert.NoError(t, st.Put(ctx, []byte("out_txn"), []byte("bar")))
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
			res, err := txn.Get(ctx, []byte("out_txn"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("bar"), res)

			err = txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err = txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)

			// overwrite exist key, return new value in txn
			assert.NoError(t, txn.Put(ctx, []byte("out_txn"), []byte("new")))
			res, err = txn.Get(ctx, []byte("out_txn"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("new"), res)

			// delete after put is returned
			err = txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err = txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("bar"), res)

			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Nil(t, res)
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
