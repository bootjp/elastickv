package snapshotoffload

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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

// TestSchedulerSharesTheUploadLimitAcrossConcurrentScans pins that the
// limiter belongs to the scheduler, not to one scan. An operator-forced
// SyncOnce can overlap the Run loop's pass, and a per-scan semaphore
// would hand each its own full allowance — two uploads under a
// configured limit of one.
func TestSchedulerSharesTheUploadLimitAcrossConcurrentScans(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	var inFlight, peak atomic.Int64

	groups := make([]OffloadGroup, 0, 4)
	for i := range 4 {
		groupID := uint64(i) + 1 //nolint:gosec // loop index over a 4-element fixture.
		dir := seedSchedulerGroup(t, root, "shared"+string(rune('a'+i)))
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
				time.Sleep(2 * time.Millisecond)
				inFlight.Add(-1)
				return true
			},
			VerifyLeader: func(context.Context) error { return nil },
		})
	}

	s := newTestScheduler(t, store, groups)

	// Two overlapping scans on the same scheduler.
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.SyncOnce(context.Background())
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), peak.Load(),
		"overlapping scans must share the configured upload limit")
}

// TestSchedulerTreatsAbsentPersistedSnapshotAsASkip covers a young or
// lightly-used group: Raft has not produced a snapshot yet, which is a
// normal scan outcome. Reporting it as a failure would emit a warning
// and a failure metric every interval until Raft eventually snapshots.
func TestSchedulerTreatsAbsentPersistedSnapshotAsASkip(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}

	// A data dir with no persisted snapshot at all.
	empty := filepath.Join(root, "empty-group")
	require.NoError(t, os.MkdirAll(empty, 0o755))

	s := newTestScheduler(t, store, []OffloadGroup{{
		GroupID:      7,
		DataDir:      empty,
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}}, WithSchedulerObserver(obs))
	s.SyncOnce(context.Background())

	published, skipped, failed := obs.snapshot()
	require.Empty(t, published)
	require.Empty(t, failed, "a group with no snapshot yet is not an outage")
	require.Equal(t, []string{"no_persisted_snapshot"}, skipped)
}

// TestPublishReusesACommittedManifestAcrossABinaryUpgrade pins the
// upgrade retry path: a process that restarts on a new binary and
// republishes an index it has not published locally must reuse the
// committed manifest instead of conflicting with it. Otherwise every
// scan fails until Raft happens to produce a new snapshot.
func TestPublishReusesACommittedManifestAcrossABinaryUpgrade(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "upgrade")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	first, err := PublishPersistedSnapshot(context.Background(), PublishOptions{
		Store:         store,
		DataDir:       dataDir,
		Prefix:        "cluster-a",
		GroupID:       7,
		SourceCluster: "cluster-a",
		BinaryVersion: "v1.0.0",
	})
	require.NoError(t, err)

	// Same snapshot, newer binary.
	second, err := PublishPersistedSnapshot(context.Background(), PublishOptions{
		Store:         store,
		DataDir:       dataDir,
		Prefix:        "cluster-a",
		GroupID:       7,
		SourceCluster: "cluster-a",
		BinaryVersion: "v2.0.0",
	})
	require.NoError(t, err, "an upgraded binary must reuse the committed manifest")
	require.Equal(t, first.SnapshotIndex, second.SnapshotIndex)
	require.Equal(t, "v1.0.0", second.BinaryVersion,
		"the committed manifest keeps the publishing binary's version as the audit record")
}

// TestSchedulerRejectsInvalidGroupAndClusterConfiguration keeps static
// misconfiguration a startup error instead of a recurring per-interval
// failure metric.
func TestSchedulerRejectsInvalidGroupAndClusterConfiguration(t *testing.T) {
	t.Parallel()

	store := newTestLocalStore(t, t.TempDir())
	valid := OffloadGroup{
		GroupID:      7,
		DataDir:      t.TempDir(),
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}

	noDataDir := valid
	noDataDir.DataDir = "   "
	_, err := NewScheduler(store, []OffloadGroup{noDataDir}, "p", "cluster-a", "v")
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.ErrorContains(t, err, "data dir")

	// A whitespace-only cluster name passes a bare != "" test but
	// buildManifest trims it away, so artifacts would be published
	// without the source-cluster identity the scheduler requires.
	_, err = NewScheduler(store, []OffloadGroup{valid}, "p", "   ", "v")
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.ErrorContains(t, err, "source cluster")
}

// TestSchedulerReportsRemoteObjectLossAsAFailure separates the two
// not-found cases. A group with no local snapshot is a skip; an object
// disappearing from the store mid-publish is a real failure, and
// collapsing them would silence the second.
func TestSchedulerReportsRemoteObjectLossAsAFailure(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "remoteloss")
	obs := &recordingObserver{}
	store := &objectLosingStore{ObjectStore: newTestLocalStore(t, filepath.Join(root, "objects"))}

	s := newTestScheduler(t, store, []OffloadGroup{{
		GroupID:      7,
		DataDir:      dataDir,
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}}, WithSchedulerObserver(obs))
	s.SyncOnce(context.Background())

	published, skipped, failed := obs.snapshot()
	require.Empty(t, published)
	require.NotEmpty(t, failed, "a vanished remote object is an outage, not a quiet skip")
	require.NotContains(t, skipped, "no_persisted_snapshot")
}

// objectLosingStore makes every object read report not-found, standing
// in for an object deleted between the head and the get.
type objectLosingStore struct {
	ObjectStore
}

func (s *objectLosingStore) PutObject(
	ctx context.Context, key string, body io.Reader, opts PutOptions,
) (ObjectInfo, error) {
	return ObjectInfo{}, errors.Wrapf(ErrObjectNotFound, "object %s vanished", key)
}

// TestSchedulerSingleFlightsAGroupAcrossOverlappingScans pins that the
// aggregate upload limiter is not enough: with concurrency above one,
// two overlapping scans could each take a slot for the SAME group,
// read the same high-water mark, and both spool and upload the same
// snapshot.
func TestSchedulerSingleFlightsAGroupAcrossOverlappingScans(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "singleflight")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}

	var concurrentEntries, peak atomic.Int64
	groups := []OffloadGroup{{
		GroupID: 7,
		DataDir: dataDir,
		IsLeader: func() bool {
			cur := concurrentEntries.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			concurrentEntries.Add(-1)
			return true
		},
		VerifyLeader: func(context.Context) error { return nil },
	}}

	s := newTestScheduler(t, store, groups,
		WithSchedulerObserver(obs), WithSchedulerConcurrency(4))

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.SyncOnce(context.Background())
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), peak.Load(),
		"one group must never be published by two scans at once")
	published, _, failed := obs.snapshot()
	require.Empty(t, failed)
	require.Len(t, published, 1, "the same snapshot must be uploaded once, not once per scan")
}

// TestSchedulerBoundsTheLeadershipRecheck pins that the pre-commit
// leadership recheck gets its own deadline.
//
// The callback contract does not require callers to wrap their engine
// method, and a raw etcd Engine.VerifyLeader issues a ReadIndex that
// waits out its context during quorum loss. Handed the long-lived Run
// context — which expires only at shutdown — a scan would block
// forever and no later snapshot would ever be scheduled.
func TestSchedulerBoundsTheLeadershipRecheck(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "bounded")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	obs := &recordingObserver{}

	gotDeadline := make(chan bool, 1)
	s := newTestScheduler(t, store, []OffloadGroup{{
		GroupID:  7,
		DataDir:  dataDir,
		IsLeader: func() bool { return true },
		VerifyLeader: func(ctx context.Context) error {
			_, ok := ctx.Deadline()
			select {
			case gotDeadline <- ok:
			default:
			}
			return nil
		},
	}}, WithSchedulerObserver(obs))

	// A context with no deadline of its own, like the Run context.
	s.SyncOnce(context.Background())

	select {
	case ok := <-gotDeadline:
		require.True(t, ok,
			"the leadership recheck must run under its own deadline, not the caller's open-ended context")
	default:
		t.Fatal("VerifyLeader was never invoked")
	}
}

// TestSchedulerValidateDoesNotMutateSharedConfiguration guards the
// data race: Run calls validate too, and an operator SyncOnce launched
// right after Run reads sourceName to build PublishOptions.
func TestSchedulerValidateDoesNotMutateSharedConfiguration(t *testing.T) {
	t.Parallel()

	store := newTestLocalStore(t, t.TempDir())
	s, err := NewScheduler(store, nil, "p", "  cluster-a  ", "v")
	require.NoError(t, err)
	require.Equal(t, "cluster-a", s.sourceName, "the trim must happen once, at construction")

	// Plant an untrimmed value and re-validate. Asserting that the
	// post-construction value is already trimmed proves nothing —
	// it is trimmed either way. What must hold is that validate,
	// which Run also calls while a concurrent SyncOnce reads
	// sourceName, performs no write at all.
	s.sourceName = "  padded  "
	require.NoError(t, s.validate())
	require.Equal(t, "  padded  ", s.sourceName,
		"validate must not write shared configuration; Run calls it while SyncOnce reads")
}

// TestSchedulerRunAndSyncOnceAreRaceFree is the guard for mutating
// shared configuration during validation. Run calls validate too, and
// launching SyncOnce right after Run — the natural way to avoid
// waiting out the first interval — has publishGroup reading
// sourceName while validate would be writing it.
//
// Run validates once at startup, so the overlap window is narrow and
// this test is a smoke check rather than a deterministic reproduction;
// TestSchedulerValidateDoesNotMutateSharedConfiguration pins the
// property itself.
func TestSchedulerRunAndSyncOnceAreRaceFree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := seedSchedulerGroup(t, root, "racefree")
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	s := newTestScheduler(t, store, []OffloadGroup{{
		GroupID:      7,
		DataDir:      dataDir,
		IsLeader:     func() bool { return true },
		VerifyLeader: func(context.Context) error { return nil },
	}}, WithSchedulerInterval(time.Millisecond), WithSchedulerJitter(0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Run(ctx)
	}()
	// Overlap an operator-forced scan with the Run loop's validation.
	for range 20 {
		s.SyncOnce(ctx)
	}
	cancel()
	wg.Wait()
}

// TestSchedulerBoundsScanGoroutines pins that a scan does not stack one
// goroutine per group. A process hosting many groups would otherwise
// burst O(group-count) stacks — and, on a staggered scan, one timer
// each — every interval, before the upload semaphore ever applies.
func TestSchedulerBoundsScanGoroutines(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	const groupCount = 40
	var concurrent, peak, peakGoroutines atomic.Int64
	groups := make([]OffloadGroup, 0, groupCount)
	for i := range groupCount {
		groupID := uint64(i) + 1 //nolint:gosec // loop index over a fixed-size fixture.
		dir := seedSchedulerGroup(t, root, fmt.Sprintf("bounded-%d", i))
		groups = append(groups, OffloadGroup{
			GroupID: groupID,
			DataDir: dir,
			IsLeader: func() bool {
				cur := concurrent.Add(1)
				for {
					old := peak.Load()
					if cur <= old || peak.CompareAndSwap(old, cur) {
						break
					}
				}
				// Sample goroutines DURING the scan. With one
				// goroutine per group the surplus sit parked on the
				// upload semaphore and are invisible once SyncOnce
				// has returned.
				live := int64(runtime.NumGoroutine())
				for {
					old := peakGoroutines.Load()
					if live <= old || peakGoroutines.CompareAndSwap(old, live) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				concurrent.Add(-1)
				return true
			},
			VerifyLeader: func(context.Context) error { return nil },
		})
	}

	const concurrency = 3
	s := newTestScheduler(t, store, groups, WithSchedulerConcurrency(concurrency))

	before := int64(runtime.NumGoroutine())
	s.SyncOnce(context.Background())

	require.LessOrEqual(t, peak.Load(), int64(concurrency),
		"in-flight group work must stay within the configured concurrency")
	// A per-group goroutine scan would park groupCount-concurrency
	// goroutines on the semaphore; a pool adds only `workers`.
	require.Less(t, peakGoroutines.Load(), before+int64(groupCount)/2,
		"a scan must not stack one goroutine per group")
}

// TestSchedulerStaggerDoesNotAccumulateAcrossGroups pins that every
// group start lands inside ONE jitter window.
//
// Sleeping a fresh jitter slice before each group makes the delays
// compound: with the default single worker and a 3m45s jitter, 100
// groups would push the last upload hours out, and Run does not arm
// the next interval until the scan returns — so later groups could go
// unvisited indefinitely.
func TestSchedulerStaggerDoesNotAccumulateAcrossGroups(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	const groupCount = 12
	groups := make([]OffloadGroup, 0, groupCount)
	for i := range groupCount {
		groupID := uint64(i) + 1 //nolint:gosec // loop index over a fixed-size fixture.
		dir := seedSchedulerGroup(t, root, fmt.Sprintf("stagger-%d", i))
		groups = append(groups, OffloadGroup{
			GroupID:      groupID,
			DataDir:      dir,
			IsLeader:     func() bool { return true },
			VerifyLeader: func(context.Context) error { return nil },
		})
	}

	const jitter = 300 * time.Millisecond
	s := newTestScheduler(t, store, groups, WithSchedulerJitter(jitter))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Baseline: the same scan with no stagger, so the comparison is
	// against this fixture's real publish cost rather than a guess.
	baselineStart := time.Now()
	s.scan(ctx, false)
	baseline := time.Since(baselineStart)

	// A second scan republishes nothing (the high-water mark short-
	// circuits it), so this measures scheduling overhead almost alone.
	staggeredStart := time.Now()
	s.scan(ctx, true)
	staggered := time.Since(staggeredStart)

	// One shared window adds at most ~jitter over the baseline.
	// Sleeping a fresh slice per group would add groupCount*jitter/2
	// ≈ 1.8s here; allow 3x jitter of slack for scheduling noise and
	// the bound still separates the two by a wide margin.
	require.Less(t, staggered, baseline+3*jitter,
		"stagger must offset group starts from one scan start, not sleep a fresh slice per group")
}
