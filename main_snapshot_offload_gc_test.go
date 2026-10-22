package main

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func configureSnapshotGC(t *testing.T) {
	t.Helper()
	enabled, confirmed := *snapshotOffloadGCEnabled, *snapshotOffloadGCClaimsComplete
	interval, timeout := *snapshotOffloadGCInterval, *snapshotOffloadGCTimeout
	minimum, age := *snapshotOffloadGCMinGenerations, *snapshotOffloadGCMaxAge
	grace, mark := *snapshotOffloadGCPayloadGrace, *snapshotOffloadGCMinMarkAge
	prefix, cluster := *snapshotOffloadPrefix, *snapshotOffloadSourceCluster
	t.Cleanup(func() {
		*snapshotOffloadGCEnabled, *snapshotOffloadGCClaimsComplete = enabled, confirmed
		*snapshotOffloadGCInterval, *snapshotOffloadGCTimeout = interval, timeout
		*snapshotOffloadGCMinGenerations, *snapshotOffloadGCMaxAge = minimum, age
		*snapshotOffloadGCPayloadGrace, *snapshotOffloadGCMinMarkAge = grace, mark
		*snapshotOffloadPrefix, *snapshotOffloadSourceCluster = prefix, cluster
	})
	*snapshotOffloadGCEnabled, *snapshotOffloadGCClaimsComplete = true, true
	*snapshotOffloadGCInterval, *snapshotOffloadGCTimeout = 10*time.Millisecond, time.Second
	*snapshotOffloadGCMinGenerations, *snapshotOffloadGCMaxAge = 1, time.Nanosecond
	*snapshotOffloadGCPayloadGrace, *snapshotOffloadGCMinMarkAge = time.Nanosecond, time.Nanosecond
	*snapshotOffloadPrefix, *snapshotOffloadSourceCluster = "cluster-a", "cluster-a"
}

func TestSnapshotOffloadGCRuntimeRequiresExplicitRolloutAndDestination(t *testing.T) {
	configureSnapshotGC(t)
	withOffloadFlags(t, "", "")
	err := startSnapshotOffload(context.Background(), nil, nil, t.TempDir(), "n1", false, nil, testLogger(t))
	require.ErrorContains(t, err, "requires an offload destination")

	*snapshotOffloadLocalDir = t.TempDir()
	*snapshotOffloadGCClaimsComplete = false
	err = startSnapshotOffload(context.Background(), nil, nil, t.TempDir(), "n1", false, nil, testLogger(t))
	require.ErrorContains(t, err, "snapshotOffloadGCClaimsRolloutComplete")
	*snapshotOffloadGCEnabled = false
	store, err := buildSnapshotOffloadStore(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeSnapshotOffloadStore(store)) })
	retention, err := buildSnapshotOffloadRetention(context.Background(), store, testLogger(t))
	require.NoError(t, err)
	require.Nil(t, retention, "publication-only configuration must never enable reclamation")
	*snapshotOffloadGCEnabled, *snapshotOffloadGCClaimsComplete = true, true
	*snapshotOffloadGCInterval = 0
	_, err = buildSnapshotOffloadRetention(context.Background(), store, testLogger(t))
	require.ErrorIs(t, err, snapshotoffload.ErrInvalidOptions)
}

func seedRuntimeOffloadSnapshot(t *testing.T, store snapshotoffload.PublishStore, index uint64, payload string) *snapshotoffload.Manifest {
	t.Helper()
	root := t.TempDir()
	input := filepath.Join(root, "input.fsm")
	require.NoError(t, os.WriteFile(input, []byte(payload), 0o600))
	dataDir := filepath.Join(root, "raft")
	_, err := etcdraftengine.PreparePhysicalSnapshotRestore(etcdraftengine.PhysicalSnapshotRestoreOptions{
		InputFSMPath: input, DataDir: dataDir, Index: index, Term: 2,
		Peers: []etcdraftengine.Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.NoError(t, err)
	manifest, err := snapshotoffload.PublishPersistedSnapshot(context.Background(), snapshotoffload.PublishOptions{
		Store: store, DataDir: dataDir, Prefix: "cluster-a", GroupID: 1,
		SourceCluster: "cluster-a", CreatedAt: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	return manifest
}

func TestSnapshotOffloadRuntimeReclaimsAndRestoresRetainedSnapshot(t *testing.T) {
	configureSnapshotGC(t)
	root := t.TempDir()
	withOffloadFlags(t, "", root)
	objects, err := snapshotoffload.NewLocalStore(root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, objects.Close()) })
	stale := seedRuntimeOffloadSnapshot(t, objects, 10, "EKVTHLC1old-runtime-payload")
	latest := seedRuntimeOffloadSnapshot(t, objects, 20, "EKVTHLC1latest-runtime-payload")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	workers, workerCtx := errgroup.WithContext(ctx)
	require.NoError(t, startSnapshotOffload(workerCtx, workers, nil, t.TempDir(), "n1", false, nil, testLogger(t)))
	require.Eventually(t, func() bool {
		_, exists, headErr := objects.HeadObject(context.Background(), stale.Payload.Key)
		return headErr == nil && !exists
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, workers.Wait())
	_, exists, err := objects.HeadObject(context.Background(), stale.ManifestKey)
	require.NoError(t, err)
	require.False(t, exists)
	groupID := uint64(1)
	result, err := snapshotoffload.RestorePhysicalSnapshot(context.Background(), snapshotoffload.RestoreOptions{
		Store: objects, ManifestKey: latest.ManifestKey, DataDir: filepath.Join(t.TempDir(), "restored"),
		ExpectGroupID: &groupID, ExpectSourceCluster: "cluster-a",
		Peers: []etcdraftengine.Peer{{NodeID: 2, ID: "n2", Address: "127.0.0.1:12002"}},
	})
	require.NoError(t, err)
	require.Equal(t, latest.Payload.SHA256, result.PayloadSHA256)
}

type blockingRetentionStore struct {
	snapshotOffloadStore
	calls atomic.Int32
}

func (s *blockingRetentionStore) ValidateRetention(ctx context.Context) error {
	if s.calls.Add(1) == 1 {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.snapshotOffloadStore.ValidateRetention(ctx)
}

func TestSnapshotOffloadGCTimeoutRetriesAndShutdownClosesStore(t *testing.T) {
	store, err := snapshotoffload.NewLocalStore(t.TempDir())
	require.NoError(t, err)
	backend := &blockingRetentionStore{snapshotOffloadStore: store}
	gc, err := snapshotoffload.NewGC(snapshotoffload.GCOptions{
		Store: backend, Prefix: "cluster-a", VerifyPublishersUseClaims: func(ctx context.Context) error { return ctx.Err() },
	})
	require.NoError(t, err)
	retention := &snapshotOffloadRetention{gc: gc, interval: time.Millisecond, timeout: 10 * time.Millisecond, logger: testLogger(t)}
	scheduler, err := snapshotoffload.NewScheduler(store, nil, "cluster-a", "cluster-a", "test")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- runSnapshotOffloadWorkers(ctx, store, scheduler, retention) }()
	require.Eventually(t, func() bool { return backend.calls.Load() >= 2 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("snapshot workers did not stop after cancellation")
	}
	_, _, err = store.HeadObject(context.Background(), "closed")
	require.ErrorIs(t, err, snapshotoffload.ErrInvalidOptions)
}
