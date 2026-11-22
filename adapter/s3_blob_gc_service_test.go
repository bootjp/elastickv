package adapter

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type s3GCTestLocalStores struct {
	store.MVCCStore
	locals []store.MVCCStore
}

func (s s3GCTestLocalStores) LocalStores() []store.MVCCStore { return s.locals }

type blockingS3BucketRecoveryStore struct {
	s3GCTestLocalStores
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingS3BucketRecoveryStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if bytes.Equal(start, []byte(s3keys.BucketCleanupPrefix)) {
		s.once.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestS3BlobGCServiceStartsForAllLocalStoresAndStops(t *testing.T) {
	first := store.NewMVCCStore()
	second := store.NewMVCCStore()
	st := s3GCTestLocalStores{MVCCStore: first, locals: []store.MVCCStore{first, second}}
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(first), nil, WithS3BlobOffloadEnabled(true))
	locals, err := server.chunkBlobGCLocalStores()
	require.NoError(t, err)
	require.Len(t, locals, 2)
	require.False(t, server.blobOffloadGCReady.Load())
	require.NoError(t, server.StartBlobGC(t.Context()))
	require.Eventually(t, server.blobOffloadGCReady.Load, time.Second, time.Millisecond)
	require.NoError(t, server.StartBlobGC(t.Context()))
	server.Stop()
	require.False(t, server.blobOffloadGCReady.Load())
}

func TestS3BlobGCServiceFailsClosedWithoutLocalStore(t *testing.T) {
	st := store.NewMVCCStore()
	wrapped := s3GCTestLocalStores{MVCCStore: st}
	server := NewS3Server(nil, "", wrapped, newLocalAdapterCoordinator(st), nil, WithS3BlobOffloadEnabled(true))
	require.Error(t, server.StartBlobGC(t.Context()))
	require.False(t, server.blobOffloadGCReady.Load())
	server.Stop()
}

func TestS3BlobGCServiceClosesReadinessOnContextCancellation(t *testing.T) {
	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil, WithS3BlobOffloadEnabled(true))
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, server.StartBlobGC(ctx))
	cancel()
	require.Eventually(t, func() bool { return !server.blobOffloadGCReady.Load() }, time.Second, time.Millisecond)
	server.Stop()
}

func TestS3BlobGCServiceRejectsCancelledStart(t *testing.T) {
	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil, WithS3BlobOffloadEnabled(true))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, server.StartBlobGC(ctx), context.Canceled)
	require.False(t, server.blobOffloadGCReady.Load())
	server.Stop()
}

func TestS3BlobGCReadinessWaitsForBucketRecovery(t *testing.T) {
	st := store.NewMVCCStore()
	blocking := &blockingS3BucketRecoveryStore{
		s3GCTestLocalStores: s3GCTestLocalStores{MVCCStore: st, locals: []store.MVCCStore{st}},
		started:             make(chan struct{}),
		release:             make(chan struct{}),
	}
	server := NewS3Server(nil, "", blocking, newLocalAdapterCoordinator(st), nil, WithS3BlobOffloadEnabled(true))
	require.NoError(t, server.StartBlobGC(t.Context()))
	require.Eventually(t, func() bool {
		select {
		case <-blocking.started:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.False(t, server.blobOffloadGCReady.Load())
	close(blocking.release)
	require.Eventually(t, server.blobOffloadGCReady.Load, time.Second, time.Millisecond)
	server.Stop()
}

type unavailableS3RecoveryStore struct {
	s3GCTestLocalStores
	recovered atomic.Bool
	attempted chan struct{}
	once      sync.Once
}

func (st *unavailableS3RecoveryStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if bytes.Equal(start, []byte(s3keys.BucketCleanupPrefix)) && !st.recovered.Load() {
		st.once.Do(func() { close(st.attempted) })
		return nil, kv.ErrTSOCommitFloorUnavailable
	}
	return st.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestS3BlobGCReadinessRecoversAfterLeaderElection(t *testing.T) {
	local := store.NewMVCCStore()
	st := &unavailableS3RecoveryStore{s3GCTestLocalStores: s3GCTestLocalStores{MVCCStore: local, locals: []store.MVCCStore{local}}, attempted: make(chan struct{})}
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil, WithS3BlobOffloadEnabled(true))
	require.NoError(t, server.StartBlobGC(t.Context()))
	t.Cleanup(server.Stop)
	select {
	case <-st.attempted:
	case <-time.After(time.Second):
		t.Fatal("missing initial bucket recovery scan")
	}
	require.False(t, server.blobOffloadGCReady.Load())
	st.recovered.Store(true)
	require.Eventually(t, server.blobOffloadGCReady.Load, 5*time.Second, 10*time.Millisecond)
}
