package snapshotoffload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const retentionPrefix = "cluster-a"

// gcFixture is a LocalStore-backed prefix plus a controllable clock.
type gcFixture struct {
	root  string
	store *LocalStore
	now   time.Time
}

func newGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	root := t.TempDir()
	store, err := NewLocalStore(root)
	require.NoError(t, err)
	return &gcFixture{
		root:  root,
		store: store,
		now:   time.Unix(1_700_000_000, 0).UTC(),
	}
}

func (f *gcFixture) gc(t *testing.T, policy RetentionPolicy) *GC {
	t.Helper()
	gc, err := NewGC(GCOptions{
		Store:  f.store,
		Prefix: retentionPrefix,
		Policy: policy,
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)
	return gc
}

// publishManifest writes a manifest object and (unless the payload is
// shared with an earlier manifest) its payload object, mirroring the
// real payload-first publish layout.
func (f *gcFixture) publishManifest(
	t *testing.T, groupID, index uint64, payload []byte, age time.Duration,
) Manifest {
	t.Helper()
	ctx := context.Background()
	// Term is fixed: retention orders by snapshot index and never
	// inspects the term beyond tie-breaking equal indexes.
	const term = uint64(2)

	payloadSHA := hexSHA256Bytes(payload)
	pKey, err := payloadKey(retentionPrefix, payloadSHA)
	require.NoError(t, err)
	_, err = f.store.PutObject(ctx, pKey, bytes.NewReader(payload), PutOptions{
		Size:   int64(len(payload)),
		SHA256: payloadSHA,
	})
	require.NoError(t, err)
	f.setMTime(t, pKey, f.now.Add(-age))

	mKey, err := manifestKey(retentionPrefix, groupID, index, term)
	require.NoError(t, err)
	manifest := Manifest{
		SchemaVersion: ManifestSchemaVersion,
		CreatedAt:     f.now.Add(-age),
		SourceCluster: retentionPrefix,
		GroupID:       groupID,
		SnapshotIndex: index,
		SnapshotTerm:  term,
		ConfState:     ManifestConfState{Voters: []uint64{1}},
		Payload: PayloadDescriptor{
			Key:    pKey,
			Bytes:  int64(len(payload)),
			SHA256: payloadSHA,
		},
		ManifestKey: mKey,
	}
	encoded, sum, err := manifest.MarshalCanonical()
	require.NoError(t, err)
	manifest.ManifestSHA256 = sum
	_, err = f.store.PutObject(ctx, mKey, bytes.NewReader(encoded), PutOptions{
		Size:   int64(len(encoded)),
		SHA256: hexSHA256Bytes(encoded),
	})
	require.NoError(t, err)
	f.setMTime(t, mKey, f.now.Add(-age))
	return manifest
}

func (f *gcFixture) writeManifest(t *testing.T, manifest Manifest, age time.Duration) {
	t.Helper()
	encoded, sum, err := manifest.MarshalCanonical()
	require.NoError(t, err)
	manifest.ManifestSHA256 = sum
	_, err = f.store.PutObject(context.Background(), manifest.ManifestKey, bytes.NewReader(encoded), PutOptions{
		Size:        int64(len(encoded)),
		SHA256:      hexSHA256Bytes(encoded),
		ContentType: "application/json",
	})
	require.NoError(t, err)
	f.setMTime(t, manifest.ManifestKey, f.now.Add(-age))
}

// writeMalformedManifest plants an object that lands in the manifest
// keyspace but does not decode.
func (f *gcFixture) writeMalformedManifest(t *testing.T, groupID, index, term uint64) string {
	t.Helper()
	key, err := manifestKey(retentionPrefix, groupID, index, term)
	require.NoError(t, err)
	body := []byte("{ not a manifest")
	_, err = f.store.PutObject(context.Background(), key, bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: hexSHA256Bytes(body),
	})
	require.NoError(t, err)
	return key
}

func (f *gcFixture) setMTime(t *testing.T, key string, at time.Time) {
	t.Helper()
	require.NoError(t, os.Chtimes(filepath.Join(f.root, filepath.FromSlash(key)), at, at))
}

func (f *gcFixture) exists(t *testing.T, key string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(f.root, filepath.FromSlash(key)))
	if err == nil {
		return true
	}
	require.True(t, os.IsNotExist(err), "unexpected stat error: %v", err)
	return false
}

// TestGCAlwaysRetainsNewestManifestPerGroup pins the §9 acceptance
// criterion: even with the most aggressive policy and an ancient
// snapshot, a group's newest manifest is never deleted. Losing it
// would leave the group with no restore point at all.
func TestGCAlwaysRetainsNewestManifestPerGroup(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	old := f.publishManifest(t, 1, 10, []byte("ancient"), 365*24*time.Hour)

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.Empty(t, result.ManifestsDeleted)
	require.True(t, f.exists(t, old.ManifestKey))
	require.True(t, f.exists(t, old.Payload.Key),
		"the payload of a retained manifest must never be reclaimed")
}

func TestGCRetainsMinGenerationsAndDeletesBeyondBothBounds(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	m1 := f.publishManifest(t, 1, 10, []byte("gen-1"), year)
	m2 := f.publishManifest(t, 1, 20, []byte("gen-2"), year)
	m3 := f.publishManifest(t, 1, 30, []byte("gen-3"), year)
	m4 := f.publishManifest(t, 1, 40, []byte("gen-4"), year)

	gc := f.gc(t, RetentionPolicy{MinGenerations: 2, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	// Newest two survive; the two older ones are beyond MinGenerations
	// and beyond MaxAge, so both go.
	require.True(t, f.exists(t, m4.ManifestKey))
	require.True(t, f.exists(t, m3.ManifestKey))
	require.False(t, f.exists(t, m2.ManifestKey))
	require.False(t, f.exists(t, m1.ManifestKey))
	require.Len(t, result.ManifestsDeleted, 2)

	// Their payloads are unreferenced and past grace, so phase 2 takes them.
	require.False(t, f.exists(t, m1.Payload.Key))
	require.False(t, f.exists(t, m2.Payload.Key))
	require.True(t, f.exists(t, m3.Payload.Key))
	require.True(t, f.exists(t, m4.Payload.Key))
	require.Len(t, result.PayloadsDeleted, 2)
}

func TestGCRetainsManifestsInsideMaxAgeBeyondMinGenerations(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	recent := 2 * time.Hour
	m1 := f.publishManifest(t, 1, 10, []byte("a"), recent)
	m2 := f.publishManifest(t, 1, 20, []byte("b"), recent)
	m3 := f.publishManifest(t, 1, 30, []byte("c"), recent)

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: 24 * time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.Empty(t, result.ManifestsDeleted)
	for _, m := range []Manifest{m1, m2, m3} {
		require.True(t, f.exists(t, m.ManifestKey))
	}
}

// TestGCNeverReclaimsPayloadSharedWithAnotherGroup is the test that
// matters most. Payloads are content-addressed, so two groups that
// snapshot identical bytes converge on ONE object. Trimming group 1's
// manifest must not delete a payload group 2 still references —
// rebuilding the live set per-group instead of prefix-wide would
// silently destroy group 2's only restore point.
func TestGCNeverReclaimsPayloadSharedWithAnotherGroup(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	shared := []byte("identical-snapshot-bytes")

	// Group 1: an old generation using the shared payload, plus a
	// newer one so the old is eligible for deletion.
	g1old := f.publishManifest(t, 1, 10, shared, year)
	g1new := f.publishManifest(t, 1, 20, []byte("group-1-newer"), year)
	// Group 2 has only one manifest, and it references the SAME payload.
	g2 := f.publishManifest(t, 2, 5, shared, year)

	require.Equal(t, g1old.Payload.Key, g2.Payload.Key, "fixture must exercise a shared payload")

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	// Group 1's old manifest is trimmed...
	require.False(t, f.exists(t, g1old.ManifestKey))
	require.True(t, f.exists(t, g1new.ManifestKey))
	require.True(t, f.exists(t, g2.ManifestKey))
	// ...but the shared payload survives because group 2 still names it.
	require.True(t, f.exists(t, g2.Payload.Key),
		"a payload referenced by another group's manifest must never be reclaimed")
	require.NotContains(t, result.PayloadsDeleted, g2.Payload.Key)
}

// TestGCSkipsPayloadPhaseWhenAManifestIsMalformed pins the §5
// fail-closed rule. An unparseable manifest means the live payload set
// cannot be proven complete, so phase 2 must not run at all — and the
// malformed object itself must not be deleted.
func TestGCSkipsPayloadPhaseWhenAManifestIsMalformed(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	live := f.publishManifest(t, 1, 30, []byte("live"), year)
	orphan := f.publishManifest(t, 1, 10, []byte("orphaned-payload"), year)
	badKey := f.writeMalformedManifest(t, 2, 7, 1)

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err, "a malformed manifest is reported, not an error")

	require.True(t, result.PayloadPhaseSkipped)
	require.Contains(t, result.SkipReason, "malformed")
	require.Equal(t, []string{badKey}, result.MalformedManifests)
	require.Empty(t, result.PayloadsDeleted)

	// The malformed manifest survives for operator inspection.
	require.True(t, f.exists(t, badKey))
	// The orphaned payload is NOT reclaimed despite being unreferenced.
	require.True(t, f.exists(t, orphan.Payload.Key),
		"phase 2 must not run while the live set is unprovable")
	require.True(t, f.exists(t, live.Payload.Key))
}

// TestGCDoesNotReclaimPayloadInsideGracePeriod covers the
// payload-first publish window: the payload lands before its manifest
// commits, so a freshly uploaded unreferenced payload is an in-flight
// publish, not garbage.
func TestGCDoesNotReclaimPayloadInsideGracePeriod(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	keep := f.publishManifest(t, 1, 30, []byte("referenced"), 365*24*time.Hour)

	// An unreferenced payload uploaded one minute ago.
	inflight := []byte("payload-of-an-in-flight-publish")
	sha := hexSHA256Bytes(inflight)
	key, err := payloadKey(retentionPrefix, sha)
	require.NoError(t, err)
	_, err = f.store.PutObject(context.Background(), key, bytes.NewReader(inflight), PutOptions{
		Size:   int64(len(inflight)),
		SHA256: sha,
	})
	require.NoError(t, err)
	f.setMTime(t, key, f.now.Add(-time.Minute))

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.Empty(t, result.PayloadsDeleted)
	require.True(t, f.exists(t, key), "a payload inside the grace window is an in-flight publish")
	require.True(t, f.exists(t, keep.Payload.Key))
}

func TestPutPayloadRefreshesReusedObjectMTime(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	ctx := context.Background()
	payload := []byte("payload-reused-by-a-new-manifest")
	sha := hexSHA256Bytes(payload)
	key, err := payloadKey(retentionPrefix, sha)
	require.NoError(t, err)
	_, err = f.store.PutObject(ctx, key, bytes.NewReader(payload), PutOptions{
		Size:   int64(len(payload)),
		SHA256: sha,
	})
	require.NoError(t, err)
	oldMTime := f.now.Add(-365 * 24 * time.Hour)
	f.setMTime(t, key, oldMTime)

	file, err := os.CreateTemp(t.TempDir(), "payload-*.fsm")
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	_, err = file.Write(payload)
	require.NoError(t, err)

	require.NoError(t, putPayload(ctx, f.store, key, file, int64(len(payload)), sha))
	info, ok, err := f.store.HeadObject(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, info.UpdatedAt.After(oldMTime),
		"reusing a content-addressed payload must refresh the store timestamp for GC grace")
}

func TestGCRevalidatesPayloadReferenceCommittedAfterInitialPayloadList(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	ctx := context.Background()
	payload := []byte("orphan-that-becomes-referenced")
	sha := hexSHA256Bytes(payload)
	key, err := payloadKey(retentionPrefix, sha)
	require.NoError(t, err)
	_, err = f.store.PutObject(ctx, key, bytes.NewReader(payload), PutOptions{
		Size:   int64(len(payload)),
		SHA256: sha,
	})
	require.NoError(t, err)
	f.setMTime(t, key, f.now.Add(-365*24*time.Hour))

	mKey, err := manifestKey(retentionPrefix, 1, 10, 2)
	require.NoError(t, err)
	injectedManifest := Manifest{
		SchemaVersion: ManifestSchemaVersion,
		CreatedAt:     f.now,
		SourceCluster: retentionPrefix,
		GroupID:       1,
		SnapshotIndex: 10,
		SnapshotTerm:  2,
		ConfState:     ManifestConfState{Voters: []uint64{1}},
		Payload: PayloadDescriptor{
			Key:    key,
			Bytes:  int64(len(payload)),
			SHA256: sha,
		},
		ManifestKey: mKey,
	}
	store := &manifestDuringPayloadListStore{
		RetentionStore: f.store,
		inject: func() {
			f.writeManifest(t, injectedManifest, 0)
		},
	}
	gc, err := NewGC(GCOptions{
		Store:  store,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	result, err := gc.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, result.PayloadsDeleted)
	require.True(t, f.exists(t, key),
		"a payload referenced by a manifest committed after the initial scan must survive")
}

func TestGCLiveSetUsesReferencedPayloadKeys(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	ctx := context.Background()
	keySHA := hexSHA256Bytes([]byte("key-hash"))
	body := []byte("payload-body-with-a-different-hash")
	bodySHA := hexSHA256Bytes(body)
	key, err := payloadKey(retentionPrefix, keySHA)
	require.NoError(t, err)
	_, err = f.store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: bodySHA,
	})
	require.NoError(t, err)
	f.setMTime(t, key, f.now.Add(-365*24*time.Hour))

	mKey, err := manifestKey(retentionPrefix, 1, 10, 2)
	require.NoError(t, err)
	f.writeManifest(t, Manifest{
		SchemaVersion: ManifestSchemaVersion,
		CreatedAt:     f.now.Add(-365 * 24 * time.Hour),
		SourceCluster: retentionPrefix,
		GroupID:       1,
		SnapshotIndex: 10,
		SnapshotTerm:  2,
		ConfState:     ManifestConfState{Voters: []uint64{1}},
		Payload: PayloadDescriptor{
			Key:    key,
			Bytes:  int64(len(body)),
			SHA256: bodySHA,
		},
		ManifestKey: mKey,
	}, 365*24*time.Hour)

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, result.PayloadsDeleted)
	require.True(t, f.exists(t, key),
		"retention must keep the exact object key a surviving manifest restores from")
}

// TestGCPerformsNoDeletesWhenListingFails pins §5's
// "listing failure performs no deletes".
func TestGCPerformsNoDeletesWhenListingFails(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	m1 := f.publishManifest(t, 1, 10, []byte("gen-1"), year)
	m2 := f.publishManifest(t, 1, 20, []byte("gen-2"), year)

	failing := &failingListStore{RetentionStore: f.store, failOn: "v1/groups"}
	gc, err := NewGC(GCOptions{
		Store:  failing,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	_, err = gc.RunOnce(context.Background())
	require.Error(t, err)
	require.Zero(t, failing.deletes, "a failed scan must delete nothing")
	require.True(t, f.exists(t, m1.ManifestKey))
	require.True(t, f.exists(t, m2.ManifestKey))
}

func TestGCPerformsNoDeletesWhenPayloadListingFails(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	m1 := f.publishManifest(t, 1, 10, []byte("gen-1"), year)
	m2 := f.publishManifest(t, 1, 20, []byte("gen-2"), year)

	failing := &failingListStore{RetentionStore: f.store, failOn: "v1/payloads"}
	gc, err := NewGC(GCOptions{
		Store:  failing,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	_, err = gc.RunOnce(context.Background())
	require.Error(t, err)
	require.Zero(t, failing.deletes, "payload listing must complete before any destructive delete")
	require.True(t, f.exists(t, m1.ManifestKey))
	require.True(t, f.exists(t, m2.ManifestKey))
}

func TestGCAbortsOnManifestTransportFailure(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	m1 := f.publishManifest(t, 1, 10, []byte("gen-1"), year)
	m2 := f.publishManifest(t, 1, 20, []byte("gen-2"), year)

	failing := &failingGetStore{RetentionStore: f.store, failKey: m1.ManifestKey}
	gc, err := NewGC(GCOptions{
		Store:  failing,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	_, err = gc.RunOnce(context.Background())
	require.Error(t, err)
	require.Zero(t, failing.deletes, "manifest transport failure must abort before deletes")
	require.True(t, f.exists(t, m1.ManifestKey))
	require.True(t, f.exists(t, m2.ManifestKey))
}

func TestGCTreatsManifestKeyMismatchAsMalformed(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	source := f.publishManifest(t, 1, 10, []byte("payload"), 365*24*time.Hour)
	body, _, err := f.store.GetObject(context.Background(), source.ManifestKey)
	require.NoError(t, err)
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())

	wrongKey, err := manifestKey(retentionPrefix, 2, 99, 2)
	require.NoError(t, err)
	_, err = f.store.PutObject(context.Background(), wrongKey, bytes.NewReader(raw), PutOptions{
		Size:        int64(len(raw)),
		SHA256:      hexSHA256Bytes(raw),
		ContentType: "application/json",
	})
	require.NoError(t, err)

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, result.PayloadPhaseSkipped)
	require.Equal(t, []string{wrongKey}, result.MalformedManifests)
	require.True(t, f.exists(t, wrongKey))
}

// TestGCLeavesUnrecognizedObjectsUnderPayloadPrefix guards against
// reclaiming an object this build does not understand — e.g. one
// written by a future layout version.
func TestGCLeavesUnrecognizedObjectsUnderPayloadPrefix(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	f.publishManifest(t, 1, 30, []byte("referenced"), 365*24*time.Hour)

	stray := "cluster-a/v1/payloads/sha256/ab/not-a-content-hash.fsm"
	body := []byte("from a future layout version")
	_, err := f.store.PutObject(context.Background(), stray, bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: hexSHA256Bytes(body),
	})
	require.NoError(t, err)
	f.setMTime(t, stray, f.now.Add(-365*24*time.Hour))

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.Empty(t, result.PayloadsDeleted)
	require.True(t, f.exists(t, stray))
}

// TestPayloadSHAFromKeyRejectsMismatchedShard stops a hand-placed
// object in the wrong shard directory from being treated as a payload.
func TestPayloadSHAFromKeyRejectsMismatchedShard(t *testing.T) {
	t.Parallel()

	sha := hexSHA256Bytes([]byte("payload"))
	good, err := payloadKey(retentionPrefix, sha)
	require.NoError(t, err)

	got, ok := payloadSHAFromKey(retentionPrefix, good)
	require.True(t, ok)
	require.Equal(t, sha, got)

	wrongShard := strings.Replace(good, "/"+sha[:2]+"/", "/zz/", 1)
	_, ok = payloadSHAFromKey(retentionPrefix, wrongShard)
	require.False(t, ok, "shard directory must agree with the content hash")

	_, ok = payloadSHAFromKey(retentionPrefix, "cluster-a/v1/payloads/sha256/ab/short.fsm")
	require.False(t, ok)

	_, ok = payloadSHAFromKey(retentionPrefix, "cluster-a/v1/payloads/archive/"+sha[:2]+"/"+sha+payloadObjectSuffix)
	require.False(t, ok, "only the canonical payloads/sha256 layout is reclaimable")

	_, ok = payloadSHAFromKey(retentionPrefix, "cluster-b/v1/payloads/sha256/"+sha[:2]+"/"+sha+payloadObjectSuffix)
	require.False(t, ok, "payload keys from another prefix must not be reclaimed")
}

func TestGCOverEmptyPrefixIsACleanNoOp(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	gc := f.gc(t, RetentionPolicy{})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)
	require.Zero(t, result.ManifestsScanned)
	require.Empty(t, result.ManifestsDeleted)
	require.Empty(t, result.PayloadsDeleted)
	require.False(t, result.PayloadPhaseSkipped)
}

func TestNewGCRequiresStore(t *testing.T) {
	t.Parallel()

	_, err := NewGC(GCOptions{Prefix: retentionPrefix})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

func TestRetentionPolicyDefaults(t *testing.T) {
	t.Parallel()

	got := RetentionPolicy{}.withDefaults()
	require.Equal(t, DefaultMinGenerations, got.MinGenerations)
	require.Equal(t, DefaultMaxAge, got.MaxAge)
	require.Equal(t, DefaultPayloadGrace, got.PayloadGrace)
}

// failingListStore fails ListObjects for a chosen prefix and counts
// deletes so a test can assert none happened.
type failingListStore struct {
	RetentionStore
	failOn  string
	deletes int
}

func (s *failingListStore) ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error) {
	if strings.Contains(prefix, s.failOn) {
		return nil, errors.New("object store listing unavailable")
	}
	return s.RetentionStore.ListObjects(ctx, prefix)
}

func (s *failingListStore) DeleteObject(ctx context.Context, key string) error {
	s.deletes++
	return s.RetentionStore.DeleteObject(ctx, key)
}

type failingGetStore struct {
	RetentionStore
	failKey string
	deletes int
}

func (s *failingGetStore) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	if normalizeObjectKey(key) == normalizeObjectKey(s.failKey) {
		return nil, ObjectInfo{}, errors.New("manifest transport unavailable")
	}
	return s.RetentionStore.GetObject(ctx, key)
}

func (s *failingGetStore) DeleteObject(ctx context.Context, key string) error {
	s.deletes++
	return s.RetentionStore.DeleteObject(ctx, key)
}

type manifestDuringPayloadListStore struct {
	RetentionStore
	inject   func()
	injected bool
}

func (s *manifestDuringPayloadListStore) ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error) {
	refs, err := s.RetentionStore.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if !s.injected && strings.Contains(prefix, "v1/payloads") {
		s.injected = true
		s.inject()
	}
	return refs, nil
}

// TestGCRetainsNewestEvenWhenPolicyWouldNotPins the §9 invariant
// independently of the MinGenerations floor. withDefaults clamps
// MinGenerations to >= 1 today, which makes the newest manifest
// survive as a side effect; this drives retains directly with a
// zero-generation, zero-age policy so the guarantee is pinned by the
// rule itself rather than by the clamp.
func TestGCRetainsNewestEvenWhenPolicyWouldNot(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	// Bypass withDefaults: an age-only policy that keeps no generations.
	gc.policy.MinGenerations = 0

	ancient := scannedManifest{
		key:       "cluster-a/v1/groups/1/snapshots/x.json",
		createdAt: f.now.Add(-365 * 24 * time.Hour),
	}
	cutoff := f.now.Add(-time.Hour)

	require.True(t, gc.retains(0, ancient, cutoff),
		"a group's newest manifest must survive any policy")
	require.False(t, gc.retains(1, ancient, cutoff),
		"an older manifest outside both bounds is still collectable")
}

// TestLocalStoreListObjectsSkipsInProgressPutTempFiles covers the
// crash-during-publish leftover: PutObject stages content as a
// ".put-*" temp file in the destination directory, and a process that
// died mid-put can leave one behind. Listing it as an object would
// hand GC a key that is not an object at all.
func TestLocalStoreListObjectsSkipsInProgressPutTempFiles(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	m := f.publishManifest(t, 1, 10, []byte("payload"), time.Hour)

	leftover := filepath.Join(f.root, filepath.FromSlash(path.Dir(m.Payload.Key)), ".put-abandoned")
	require.NoError(t, os.WriteFile(leftover, []byte("partial upload"), 0o600))

	refs, err := f.store.ListObjects(context.Background(), retentionPrefix)
	require.NoError(t, err)

	for _, ref := range refs {
		require.NotContains(t, ref.Key, ".put-",
			"an in-progress put temp file must not be listed as an object")
	}
	require.FileExists(t, leftover, "listing must not delete the leftover either")
}

// racingPublishStore simulates the concurrent-publish race the P1
// review identified: a publisher reuses a content-addressed payload
// and refreshes it in the window between GC validating the object and
// GC deleting it. The refresh happens inside the HeadObject call, so
// the state GC validated is already stale by the time it deletes.
type racingPublishStore struct {
	RetentionStore
	target      string
	refresh     func()
	refreshed   bool
	unconAppend *[]string
}

func (s *racingPublishStore) HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error) {
	info, ok, err := s.RetentionStore.HeadObject(ctx, key)
	if err == nil && ok && key == s.target && !s.refreshed {
		// GC has now validated the object. The publisher lands its
		// refresh right here, before GC issues the delete.
		s.refreshed = true
		s.refresh()
	}
	return info, ok, err
}

func (s *racingPublishStore) DeleteObject(ctx context.Context, key string) error {
	if s.unconAppend != nil {
		*s.unconAppend = append(*s.unconAppend, key)
	}
	return s.RetentionStore.DeleteObject(ctx, key)
}

// TestGCDoesNotDeletePayloadRefreshedByAConcurrentPublish is the
// regression test for the P1 concurrent-publication race.
//
// Sequence: GC decides a payload is unreferenced and past grace, and
// validates it. A publisher that is reusing the same content-addressed
// payload then refreshes the object (restarting its grace) and is
// about to commit a manifest naming it. If GC's delete is
// unconditional it lands anyway, and the publisher commits a manifest
// pointing at bytes that no longer exist.
//
// The compare-and-delete precondition turns that into a skip.
func TestGCDoesNotDeletePayloadRefreshedByAConcurrentPublish(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	// A newer manifest keeps the group alive; the older one is
	// trimmed, leaving its payload unreferenced and past grace.
	f.publishManifest(t, 1, 30, []byte("current"), year)
	orphan := f.publishManifest(t, 1, 10, []byte("reused-by-a-concurrent-publish"), year)

	racing := &racingPublishStore{
		RetentionStore: f.store,
		target:         orphan.Payload.Key,
		refresh: func() {
			// The publisher's refresh: same bytes, fresh mtime.
			f.setMTime(t, orphan.Payload.Key, f.now)
		},
	}

	gc, err := NewGC(GCOptions{
		Store:  racing,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.True(t, f.exists(t, orphan.Payload.Key),
		"a payload refreshed by a concurrent publish must survive GC")
	require.NotContains(t, result.PayloadsDeleted, orphan.Payload.Key)
	require.Equal(t, 1, result.PayloadsClaimedConcurrently,
		"the lost compare-and-delete must be reported, not silently dropped")
}

// TestLocalStoreConditionalDeleteRejectsChangedObject exercises the
// precondition directly.
func TestLocalStoreConditionalDeleteRejectsChangedObject(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	ctx := context.Background()
	key := "cluster-a/v1/payloads/sha256/ab/cond.fsm"
	body := []byte("payload bytes")
	_, err := f.store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: hexSHA256Bytes(body),
	})
	require.NoError(t, err)
	f.setMTime(t, key, f.now.Add(-time.Hour))

	stale := DeletePrecondition{Size: int64(len(body)), UpdatedAt: f.now.Add(-time.Hour)}

	// The object moves under the caller's feet.
	f.setMTime(t, key, f.now)
	err = f.store.DeleteObjectIfUnmodified(ctx, key, stale)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrObjectModified))
	require.True(t, f.exists(t, key), "a failed precondition must not delete")

	// With the current state it succeeds.
	current := DeletePrecondition{Size: int64(len(body)), UpdatedAt: f.now}
	require.NoError(t, f.store.DeleteObjectIfUnmodified(ctx, key, current))
	require.False(t, f.exists(t, key))

	// Already-absent stays a no-op so GC is retry-safe.
	require.NoError(t, f.store.DeleteObjectIfUnmodified(ctx, key, current))
}

// manifestRewritingStore rewrites an expired manifest between the scan
// that decided to delete it and the delete itself, modelling an
// idempotent publish retry that republishes the same index/term.
type manifestRewritingStore struct {
	RetentionStore
	target  string
	rewrite func()
	done    bool
}

func (s *manifestRewritingStore) DeleteObjectIfUnmodified(
	ctx context.Context, key string, cond DeletePrecondition,
) error {
	if key == s.target && !s.done {
		s.done = true
		s.rewrite()
	}
	return s.RetentionStore.DeleteObjectIfUnmodified(ctx, key, cond)
}

// TestGCDoesNotDeleteManifestRewrittenByAConcurrentPublish is the
// phase-1 sibling of the payload race. Manifest keys are deterministic
// in (group, index, term), so an idempotent republish rewrites the
// exact key retention is about to remove. Deleting it anyway would
// drop a manifest the publisher believes it just committed.
func TestGCDoesNotDeleteManifestRewrittenByAConcurrentPublish(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	f.publishManifest(t, 1, 30, []byte("current"), year)
	stale := f.publishManifest(t, 1, 10, []byte("republished"), year)

	rewriting := &manifestRewritingStore{
		RetentionStore: f.store,
		target:         stale.ManifestKey,
		rewrite: func() {
			// The publisher's idempotent rewrite: same key, fresh mtime.
			f.setMTime(t, stale.ManifestKey, f.now)
		},
	}

	gc, err := NewGC(GCOptions{
		Store:  rewriting,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.True(t, f.exists(t, stale.ManifestKey),
		"a manifest rewritten by a concurrent publish must survive GC")
	require.NotContains(t, result.ManifestsDeleted, stale.ManifestKey)
	require.Equal(t, 1, result.ManifestsClaimedConcurrently)
}

// countingListStore counts ListObjects calls so a test can assert the
// revalidation pass is per-phase, not per-payload.
type countingListStore struct {
	RetentionStore
	mu    sync.Mutex
	lists int
}

func (s *countingListStore) ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error) {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	return s.RetentionStore.ListObjects(ctx, prefix)
}

// TestGCRevalidatesReferencesOncePerPassNotPerPayload pins the cost
// shape. Revalidating inside the per-payload loop turns a stale-payload
// backlog into N listings and O(N×M) object reads, so the first cleanup
// of a realistically accumulated backlog never finishes.
func TestGCRevalidatesReferencesOncePerPassNotPerPayload(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	year := 365 * 24 * time.Hour
	f.publishManifest(t, 1, 100, []byte("current"), year)
	// Eight orphaned payloads, all eligible for reclamation.
	for i := range uint64(8) {
		f.publishManifest(t, 1, 10+i, []byte(fmt.Sprintf("orphan-%d", i)), year)
	}

	counting := &countingListStore{RetentionStore: f.store}
	gc, err := NewGC(GCOptions{
		Store:  counting,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, result.PayloadsDeleted, "the fixture must actually reclaim something")

	counting.mu.Lock()
	defer counting.mu.Unlock()
	// One manifest listing (phase 1) + one payload listing + one
	// revalidation listing. The bound is what matters: it must not
	// grow with the number of reclaimable payloads.
	require.LessOrEqual(t, counting.lists, 4,
		"listings must be bounded per pass, not proportional to the payload backlog")
}

// TestGCRejectsAManifestStoredOffItsCanonicalPath closes a retention
// hijack: retention groups and orders by the manifest BODY, so a
// high-index body claiming group 2 parked under a group-1 path would
// consume group 2's retained-generation slots and get group 2's real
// newest manifests deleted.
func TestGCRejectsAManifestStoredOffItsCanonicalPath(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	ctx := context.Background()
	year := 365 * 24 * time.Hour
	victim := f.publishManifest(t, 2, 5, []byte("group-2-only-restore-point"), year)

	// A manifest whose body claims group 2 at a very high index, but
	// which is stored under group 1's path. Its self-hash is valid and
	// its ManifestKey matches where it lives.
	hijackKey, err := manifestKey(retentionPrefix, 1, 9000, 2)
	require.NoError(t, err)
	payloadSHA := hexSHA256Bytes([]byte("hijack"))
	pKey, err := payloadKey(retentionPrefix, payloadSHA)
	require.NoError(t, err)
	hijack := Manifest{
		SchemaVersion: ManifestSchemaVersion,
		CreatedAt:     f.now,
		SourceCluster: retentionPrefix,
		GroupID:       2, // body claims group 2 ...
		SnapshotIndex: 9000,
		SnapshotTerm:  2,
		ConfState:     ManifestConfState{Voters: []uint64{1}},
		Payload:       PayloadDescriptor{Key: pKey, Bytes: 6, SHA256: payloadSHA},
		ManifestKey:   hijackKey, // ... but lives under group 1's path
	}
	encoded, _, err := hijack.MarshalCanonical()
	require.NoError(t, err)
	_, err = f.store.PutObject(ctx, hijackKey, bytes.NewReader(encoded), PutOptions{
		Size:   int64(len(encoded)),
		SHA256: hexSHA256Bytes(encoded),
	})
	require.NoError(t, err)

	gc := f.gc(t, RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour})
	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.Contains(t, result.MalformedManifests, hijackKey,
		"a manifest off its canonical path must be classified malformed")
	require.True(t, f.exists(t, victim.ManifestKey),
		"group 2's real manifest must not be displaced by the hijack")
	require.True(t, result.PayloadPhaseSkipped,
		"a malformed manifest blocks reclamation")
}

// duplicateListingStore returns one manifest key twice, modelling an
// S3-compatible endpoint producing overlapping pages while objects
// change underneath the scan.
type duplicateListingStore struct {
	RetentionStore
	target string
}

func (s *duplicateListingStore) ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error) {
	refs, err := s.RetentionStore.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if ref.Key == s.target {
			refs = append(refs, ref)
			break
		}
	}
	return refs, nil
}

// TestGCDeduplicatesListedManifestKeys is the duplicate-page guard.
// Two copies of the same key would be counted as two generations: with
// MinGenerations 1 one lands in survivors and the other in expired, so
// phase 1 would delete the exact key chosen as the group's newest
// restore point.
func TestGCDeduplicatesListedManifestKeys(t *testing.T) {
	t.Parallel()

	f := newGCFixture(t)
	only := f.publishManifest(t, 1, 10, []byte("sole-restore-point"), 365*24*time.Hour)

	dup := &duplicateListingStore{RetentionStore: f.store, target: only.ManifestKey}
	gc, err := NewGC(GCOptions{
		Store:  dup,
		Prefix: retentionPrefix,
		Policy: RetentionPolicy{MinGenerations: 1, MaxAge: time.Hour, PayloadGrace: time.Hour},
		Now:    func() time.Time { return f.now },
	})
	require.NoError(t, err)

	result, err := gc.RunOnce(context.Background())
	require.NoError(t, err)

	require.Empty(t, result.ManifestsDeleted)
	require.True(t, f.exists(t, only.ManifestKey),
		"a duplicated listing entry must not make a group's newest manifest deletable")
	require.Equal(t, 1, result.ManifestsScanned, "duplicates must be collapsed")
}
