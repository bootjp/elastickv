package store

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
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

// failingScanStore simulates a transaction commit failure to verify atomicity.
type failingScanStore struct {
	inner ScanStore
	fail  bool
}

func newFailingScanStore(inner ScanStore, fail bool) *failingScanStore {
	return &failingScanStore{inner: inner, fail: fail}
}

func (s *failingScanStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s.inner.Get(ctx, key)
}

func (s *failingScanStore) Put(ctx context.Context, key []byte, value []byte) error {
	return s.inner.Put(ctx, key, value)
}

func (s *failingScanStore) Delete(ctx context.Context, key []byte) error {
	return s.inner.Delete(ctx, key)
}

func (s *failingScanStore) Exists(ctx context.Context, key []byte) (bool, error) {
	return s.inner.Exists(ctx, key)
}

func (s *failingScanStore) Snapshot() (io.ReadWriter, error) { return nil, ErrNotSupported }
func (s *failingScanStore) Restore(io.Reader) error          { return ErrNotSupported }
func (s *failingScanStore) Close() error                     { return nil }

func (s *failingScanStore) Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error) {
	return s.inner.Scan(ctx, start, end, limit)
}

// Txn executes the function; if fail is set, it aborts commit and returns an error.
func (s *failingScanStore) Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error {
	err := s.inner.Txn(ctx, func(ctx context.Context, txn Txn) error {
		if s.fail {
			return errors.New("injected commit failure")
		}
		return f(ctx, txn)
	})

	return err
}

func TestListStore_PutList_RollbackOnTxnFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rawBase := NewRbMemoryStore()
	ls := NewListStore(rawBase)

	initial := []string{"a", "b", "c"}
	assert.NoError(t, ls.PutList(ctx, []byte("k"), initial))

	failStore := newFailingScanStore(rawBase, true)
	lsFail := NewListStore(failStore)

	err := lsFail.PutList(ctx, []byte("k"), []string{"x", "y"})
	assert.Error(t, err, "expected injected failure")

	// Original list must remain intact because txn never committed.
	out, err := ls.GetList(ctx, []byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, initial, out)
}
