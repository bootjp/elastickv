package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListStore_PutGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ls := NewListStore(NewRbMemoryStore())

	in := []string{"a", "b", "c"}
	assert.NoError(t, ls.PutList(ctx, []byte("k"), in))

	out, err := ls.GetList(ctx, []byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestListStore_GetList_NotFound(t *testing.T) {
	t.Parallel()

	_, err := NewListStore(NewRbMemoryStore()).GetList(context.Background(), []byte("missing"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestListStore_RPushAndRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ls := NewListStore(NewRbMemoryStore())

	n, err := ls.RPush(ctx, []byte("numbers"), "zero", "one", "two", "three", "four")
	assert.NoError(t, err)
	assert.Equal(t, 5, n)

	// Range with positive indexes.
	res, err := ls.Range(ctx, []byte("numbers"), 1, 3)
	assert.NoError(t, err)
	assert.Equal(t, []string{"one", "two", "three"}, res)

	// Range with negative end index.
	res, err = ls.Range(ctx, []byte("numbers"), 2, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "three", "four"}, res)
}

func TestListStore_Range_NotFound(t *testing.T) {
	t.Parallel()

	_, err := NewListStore(NewRbMemoryStore()).Range(context.Background(), []byte("nope"), 0, -1)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestListStore_RPushConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ls := NewListStore(NewRbMemoryStore())

	wg := &sync.WaitGroup{}
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := ls.RPush(ctx, []byte("k"), fmt.Sprintf("v-%d", i))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	list, err := ls.GetList(ctx, []byte("k"))
	assert.NoError(t, err)
	assert.Len(t, list, n)
}
