package adapter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type blockingDynamoScanStore struct {
	store.MVCCStore
	started chan uint64
	release chan struct{}
	once    sync.Once
}

func (s *blockingDynamoScanStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.once.Do(func() {
		s.started <- ts
		<-s.release
	})
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func requireReadTSPinned(t *testing.T, tracker *kv.ActiveTimestampTracker, want uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return tracker.Oldest() == want
	}, time.Second, 10*time.Millisecond)
}

func waitForScanStart(t *testing.T, started <-chan uint64) uint64 {
	t.Helper()
	select {
	case ts := <-started:
		return ts
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scan")
		return 0
	}
}

func TestDynamoDBScanAllByPrefixAtPinsReadTS(t *testing.T) {
	t.Parallel()

	tracker := kv.NewActiveTimestampTracker()
	backing := store.NewMVCCStore()
	require.NoError(t, backing.PutAt(context.Background(), []byte("prefix/item"), []byte("value"), 10, 0))

	scanStore := &blockingDynamoScanStore{
		MVCCStore: backing,
		started:   make(chan uint64, 1),
		release:   make(chan struct{}),
	}
	server := NewDynamoDBServer(nil, scanStore, &stubAdapterCoordinator{}, WithDynamoDBActiveTimestampTracker(tracker))

	const readTS uint64 = 42
	done := make(chan error, 1)
	go func() {
		_, err := server.scanAllByPrefixAt(context.Background(), []byte("prefix/"), readTS)
		done <- err
	}()

	require.Equal(t, readTS, waitForScanStart(t, scanStore.started))
	requireReadTSPinned(t, tracker, readTS)

	close(scanStore.release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scanAllByPrefixAt")
	}
	require.Zero(t, tracker.Oldest())
}

func TestDynamoDBMigrateLegacySourceItemsPinsReadTS(t *testing.T) {
	t.Parallel()

	tracker := kv.NewActiveTimestampTracker()
	scanStore := &blockingDynamoScanStore{
		MVCCStore: store.NewMVCCStore(),
		started:   make(chan uint64, 1),
		release:   make(chan struct{}),
	}
	server := NewDynamoDBServer(nil, scanStore, &stubAdapterCoordinator{}, WithDynamoDBActiveTimestampTracker(tracker))

	targetSchema := &dynamoTableSchema{TableName: "tbl", Generation: 2}
	sourceSchema := &dynamoTableSchema{TableName: "tbl", Generation: 1}

	const readTS uint64 = 55
	done := make(chan error, 1)
	go func() {
		done <- server.migrateLegacySourceItems(context.Background(), targetSchema, sourceSchema, readTS)
	}()

	require.Equal(t, readTS, waitForScanStart(t, scanStore.started))
	requireReadTSPinned(t, tracker, readTS)

	close(scanStore.release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for migrateLegacySourceItems")
	}
	require.Zero(t, tracker.Oldest())
}
