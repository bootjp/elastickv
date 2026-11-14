package kv

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type requestReadTestStore struct {
	store.MVCCStore
	floor    uint64
	fenceErr error
	reads    atomic.Int64
	entered  chan struct{}
	release  chan struct{}
}

func (s *requestReadTestStore) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return s.floor, s.fenceErr
}
func (s *requestReadTestStore) CommittedTimestampFloorForKeys(context.Context, [][]byte) (uint64, error) {
	return s.floor, s.fenceErr
}
func (s *requestReadTestStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.reads.Add(1)
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}
func (s *requestReadTestStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.reads.Add(1)
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func requestReadFixture(t *testing.T) (*requestReadTestStore, *ReadRequest) {
	t.Helper()
	base := store.NewMVCCStore()
	t.Cleanup(func() { _ = base.Close() })
	require.NoError(t, base.PutAt(t.Context(), []byte("key"), []byte("0"), 2, 0))
	require.NoError(t, base.PutAt(t.Context(), []byte("key"), []byte("1"), 42, 0))
	st := &requestReadTestStore{MVCCStore: base, floor: 42}
	r := NewReadRequest(t.Context(), nil, st, nil)
	t.Cleanup(r.Close)
	return st, r
}

func TestReadRequestLatestAndHistoricalAreExplicit(t *testing.T) {
	st, r := requestReadFixture(t)
	snapshot, err := r.Latest(r.Context(), "test")
	require.NoError(t, err)
	got, err := r.Get(r.Context(), snapshot, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, "1", string(got))
	rows, err := r.Scan(r.Context(), snapshot, nil, nil, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "1", string(rows[0].Value))
	old, err := r.Historical(r.Context(), 2)
	require.NoError(t, err)
	got, err = r.Get(r.Context(), old, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, "0", string(got))
	st.fenceErr = errors.New("quorum unavailable")
	_, err = r.Latest(r.Context(), "test")
	require.ErrorIs(t, err, st.fenceErr)
	_, err = r.Issued(r.Context(), 41)
	require.ErrorIs(t, err, ErrReadSnapshotUnissued)
}

func TestReadRequestRejectsForeignZeroAndRetiredTokensBeforeStorage(t *testing.T) {
	st, a := requestReadFixture(t)
	b := NewReadRequest(t.Context(), nil, st, nil)
	defer b.Close()
	otherStore := &requestReadTestStore{MVCCStore: st.MVCCStore, floor: 42}
	c := NewReadRequest(t.Context(), nil, otherStore, nil)
	defer c.Close()
	snapshot, err := a.Latest(a.Context(), "test")
	require.NoError(t, err)
	for _, foreign := range []*ReadRequest{b, c} {
		_, err = foreign.Get(foreign.Context(), snapshot, []byte("key"))
		require.ErrorIs(t, err, ErrReadSnapshotScope)
		_, err = foreign.Scan(foreign.Context(), snapshot, nil, nil, 10)
		require.ErrorIs(t, err, ErrReadSnapshotScope)
	}
	_, err = a.Get(b.Context(), snapshot, []byte("key"))
	require.ErrorIs(t, err, ErrReadSnapshotScope)
	_, err = a.Get(a.Context(), ReadSnapshot{}, []byte("key"))
	require.ErrorIs(t, err, ErrReadSnapshotScope)
	a.Close()
	_, err = a.Get(a.Context(), snapshot, []byte("key"))
	require.ErrorIs(t, err, ErrReadRequestClosed)
	_, err = a.Scan(a.Context(), snapshot, nil, nil, 10)
	require.ErrorIs(t, err, ErrReadRequestClosed)
	require.Zero(t, st.reads.Load())
	require.Zero(t, otherStore.reads.Load())
}

func TestReadRequestPointFenceCannotAuthorizeAnUnfencedKeyOrRange(t *testing.T) {
	st, r := requestReadFixture(t)
	r.coord = &requestReadGroupCoordinator{stubLeaderCoordinator: &stubLeaderCoordinator{isLeader: true}}
	snapshot, err := r.Latest(r.Context(), "test", []byte("key"))
	require.NoError(t, err)
	_, err = r.Get(r.Context(), snapshot, []byte("other"))
	require.ErrorIs(t, err, ErrReadSnapshotKeyScope)
	_, err = r.Scan(r.Context(), snapshot, nil, nil, 10)
	require.ErrorIs(t, err, ErrReadSnapshotKeyScope)
	require.Zero(t, st.reads.Load())
}

func TestReadRequestCloseJoinsAndSuppressesLateResults(t *testing.T) {
	st, r := requestReadFixture(t)
	st.entered = make(chan struct{})
	st.release = make(chan struct{})
	snapshot, err := r.Latest(r.Context(), "test")
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { _, readErr := r.Get(r.Context(), snapshot, []byte("key")); result <- readErr }()
	<-st.entered
	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	<-r.Context().Done()
	select {
	case <-done:
		t.Fatal("request closed before its storage read joined")
	case <-time.After(10 * time.Millisecond):
	}
	close(st.release)
	require.ErrorIs(t, <-result, context.Canceled)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not join")
	}
}

type requestReadGroupCoordinator struct{ *stubLeaderCoordinator }

func (*requestReadGroupCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	if string(key) == "key" {
		return 1
	}
	return 2
}

func TestReadRequestCannotReuseAnOldSnapshotAfterANewerWrite(t *testing.T) {
	st, first := requestReadFixture(t)
	st.floor = 2
	old, err := first.Latest(first.Context(), "first")
	require.NoError(t, err)
	first.Close()
	st.floor = 42
	next := NewReadRequest(t.Context(), nil, st, nil)
	defer next.Close()
	fresh, err := next.Latest(next.Context(), "next")
	require.NoError(t, err)
	_, err = next.Get(next.Context(), old, []byte("key"))
	require.ErrorIs(t, err, ErrReadSnapshotScope)
	_, err = next.Issued(next.Context(), old.Timestamp())
	require.ErrorIs(t, err, ErrReadSnapshotUnissued)
	value, err := next.Get(next.Context(), fresh, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, "1", string(value))
}

func TestReadRequestParentClosesAndJoinsAChildStoreRead(t *testing.T) {
	st, parent := requestReadFixture(t)
	childStore := &requestReadTestStore{MVCCStore: st.MVCCStore, floor: 42, entered: make(chan struct{}), release: make(chan struct{})}
	child := NewReadRequest(parent.Context(), nil, childStore, nil)
	snapshot, err := child.Latest(child.Context(), "child")
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { _, readErr := child.Get(child.Context(), snapshot, []byte("key")); result <- readErr }()
	<-childStore.entered
	done := make(chan struct{})
	go func() { parent.Close(); close(done) }()
	<-child.Context().Done()
	select {
	case <-done:
		t.Fatal("parent closed before its child joined")
	case <-time.After(10 * time.Millisecond):
	}
	close(childStore.release)
	require.ErrorIs(t, <-result, context.Canceled)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("parent did not join child")
	}
	_, err = child.Get(child.Context(), snapshot, []byte("key"))
	require.ErrorIs(t, err, ErrReadRequestClosed)
}

func TestReadRequestReleasesPinsAndClosedChildren(t *testing.T) {
	st, request := requestReadFixture(t)
	tracker := NewActiveTimestampTracker()
	request.tracker = tracker
	snapshot, err := request.Latest(request.Context(), "pinned read")
	require.NoError(t, err)
	require.Equal(t, snapshot.Timestamp(), tracker.Oldest())
	for range 100 {
		child := NewReadRequest(request.Context(), nil, st, tracker)
		_, err := child.Historical(child.Context(), 2)
		require.NoError(t, err)
		require.Equal(t, uint64(2), tracker.Oldest())
		child.Close()
		require.Equal(t, snapshot.Timestamp(), tracker.Oldest())
	}
	request.childrenMu.Lock()
	remaining := len(request.children)
	request.childrenMu.Unlock()
	require.Zero(t, remaining)
	request.Close()
	require.Zero(t, tracker.Oldest())
}
