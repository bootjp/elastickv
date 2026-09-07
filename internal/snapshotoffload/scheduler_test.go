package snapshotoffload

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type recordingObserver struct {
	mu        sync.Mutex
	published []uint64
	skipped   []string
	failed    []error
}

func (o *recordingObserver) ObserveSnapshotOffloadPublished(groupID, _ uint64, _ int64, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.published = append(o.published, groupID)
}

func (o *recordingObserver) ObserveSnapshotOffloadSkipped(_ uint64, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.skipped = append(o.skipped, reason)
}

func (o *recordingObserver) ObserveSnapshotOffloadFailed(_ uint64, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failed = append(o.failed, err)
}

func (o *recordingObserver) snapshot() ([]uint64, []string, []error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]uint64(nil), o.published...), append([]string(nil), o.skipped...), append([]error(nil), o.failed...)
}

const schedulerTestIndex = 42

func seedSchedulerGroup(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	return seedPhysicalSnapshot(t, dir, []byte("EKVTHLC1scheduler-payload"), schedulerTestIndex, 3,
		[]etcdraftengine.Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:1"}})
}

// §4: only the current group leader may publish, and a follower must not even
// open the snapshot.
func TestSchedulerSkipsGroupsThisNodeDoesNotLead(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "g")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}

	s := newTestScheduler(t, store, []OffloadGroup{{
		GroupID:      7,
		DataDir:      dataDir,
		IsLeader:     func() bool { return false },
		VerifyLeader: func(context.Context) error { return nil },
	}}, WithSchedulerObserver(obs))

	s.SyncOnce(context.Background())

	published, skipped, failed := obs.snapshot()
	require.Empty(t, published, "a follower must publish nothing")
	require.Empty(t, failed)
	require.Equal(t, []string{"not_leader"}, skipped)
	require.Zero(t, s.LastPublishedIndex(7))
}

// §4: leadership is re-checked immediately before the manifest. Losing it in
// the window may strand a content-addressed payload, which GC reclaims, but
// must never commit a manifest.
func TestSchedulerDoesNotCommitManifestWhenLeadershipIsLostWhileSpooling(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "g")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}
	lost := errors.New("leadership lost")

	s := newTestScheduler(t, store, []OffloadGroup{{
		GroupID:      7,
		DataDir:      dataDir,
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return lost },
	}}, WithSchedulerObserver(obs))

	s.SyncOnce(context.Background())

	published, _, failed := obs.snapshot()
	require.Empty(t, published)
	require.Len(t, failed, 1)
	require.ErrorIs(t, failed[0], lost)

	manifestObjectKey, err := manifestKey("cluster-a", 7, 42, 3)
	require.NoError(t, err)
	_, ok, err := store.HeadObject(context.Background(), manifestObjectKey)
	require.NoError(t, err)
	require.False(t, ok, "no manifest may be committed after leadership is lost")
}

// A leader publishes, and re-running the scan is idempotent: the second pass
// reuses the committed manifest rather than producing a second one. Restart
// safety comes from the object store, so a fresh Scheduler behaves the same.
func TestSchedulerPublishesOnceAndIsIdempotentAcrossRestart(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "g")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}
	groups := []OffloadGroup{{
		GroupID:      7,
		DataDir:      dataDir,
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}}

	s := newTestScheduler(t, store, groups, WithSchedulerObserver(obs))
	s.SyncOnce(context.Background())
	require.Equal(t, uint64(42), s.LastPublishedIndex(7))

	// Same process, second scan.
	s.SyncOnce(context.Background())
	// A different process that has published nothing itself.
	restarted := newTestScheduler(t, store, groups, WithSchedulerObserver(obs))
	require.Zero(t, restarted.LastPublishedIndex(7), "a fresh process starts with no local record")
	restarted.SyncOnce(context.Background())

	_, _, failed := obs.snapshot()
	require.Empty(t, failed, "republishing the same index must reuse the manifest, not fail")
	require.Equal(t, uint64(42), restarted.LastPublishedIndex(7))
}

// §4 bounds uploads to one at a time per process by default, so a process
// hosting many groups cannot saturate its uplink.
func TestSchedulerBoundsConcurrentUploads(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	var inFlight, peak atomic.Int64
	groups := make([]OffloadGroup, 0, 4)
	for i := range 4 {
		groupID := uint64(i) + 1 //nolint:gosec // loop index over a 4-element fixture.
		dir := seedSchedulerGroup(t, root, "g"+string(rune('a'+i)))
		groups = append(groups, OffloadGroup{
			GroupID: groupID,
			DataDir: dir,
			IsLeader: func() bool {
				cur := inFlight.Add(1)
				for {
					old := peak.Load()
					if cur <= old || peak.CompareAndSwap(old, cur) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				inFlight.Add(-1)
				return true
			},
			VerifyLeader: func(context.Context) error { return nil },
		})
	}

	s := newTestScheduler(t, store, groups)
	s.SyncOnce(context.Background())

	require.Equal(t, int64(1), peak.Load(), "default concurrency is one upload per process")
}

// Cancellation must stop the scan rather than being reported as a publish
// failure: a cancelled context is a shutdown, not an object-store problem.
func TestSchedulerTreatsCancellationAsShutdown(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "g")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newTestScheduler(t, store, []OffloadGroup{{
		GroupID:      7,
		DataDir:      dataDir,
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}}, WithSchedulerObserver(obs))
	s.SyncOnce(ctx)

	published, _, failed := obs.snapshot()
	require.Empty(t, published)
	require.Empty(t, failed, "cancellation is shutdown, not a publish failure")
}

func TestSchedulerRunRequiresStoreAndSourceCluster(t *testing.T) {
	t.Parallel()

	_, err := NewScheduler(nil, nil, "p", "c", "v")
	require.ErrorIs(t, err, ErrInvalidOptions)
	store := newTestLocalStore(t, t.TempDir())
	_, err = NewScheduler(store, nil, "p", "", "v")
	require.ErrorIs(t, err, ErrInvalidOptions)
}

// newTestScheduler builds a valid scheduler and fails the test if the
// configuration is rejected.
func newTestScheduler(
	t *testing.T, store ObjectStore, groups []OffloadGroup, opts ...SchedulerOption,
) *Scheduler {
	t.Helper()
	s, err := NewScheduler(store, groups, "cluster-a", "cluster-a", "test", opts...)
	require.NoError(t, err)
	return s
}

// TestSchedulerRejectsGroupsMissingALeadershipCallback is the P1 guard:
// a nil callback previously meant "publishable", so a miswired
// scheduler would publish a manifest from a follower — silently, and
// exactly against the guarantee the scheduler exists to provide.
func TestSchedulerRejectsGroupsMissingALeadershipCallback(t *testing.T) {
	t.Parallel()

	store := newTestLocalStore(t, t.TempDir())
	valid := OffloadGroup{
		GroupID:      7,
		DataDir:      t.TempDir(),
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}

	missingIsLeader := valid
	missingIsLeader.IsLeader = nil
	_, err := NewScheduler(store, []OffloadGroup{missingIsLeader}, "p", "c", "v")
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.ErrorContains(t, err, "IsLeader")

	missingVerify := valid
	missingVerify.VerifyLeader = nil
	_, err = NewScheduler(store, []OffloadGroup{missingVerify}, "p", "c", "v")
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.ErrorContains(t, err, "VerifyLeader")

	// The fully-wired group is accepted.
	_, err = NewScheduler(store, []OffloadGroup{valid}, "p", "c", "v")
	require.NoError(t, err)
}

// TestSchedulerSkipsRepublishingAnUnchangedSnapshot is the P1
// efficiency guard: an unchanged snapshot must not be re-spooled and
// re-hashed on every tick. The skip has to happen before the payload
// is read, so it is observable as a skip rather than a publish.
func TestSchedulerSkipsRepublishingAnUnchangedSnapshot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "g")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}
	groups := []OffloadGroup{{
		GroupID:      7,
		DataDir:      dataDir,
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}}

	s := newTestScheduler(t, store, groups, WithSchedulerObserver(obs))
	s.SyncOnce(context.Background())
	s.SyncOnce(context.Background())
	s.SyncOnce(context.Background())

	published, skipped, failed := obs.snapshot()
	require.Empty(t, failed)
	require.Len(t, published, 1, "an unchanged snapshot must be published exactly once")
	require.Equal(t, []string{"already_published", "already_published"}, skipped)
}
