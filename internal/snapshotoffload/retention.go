package snapshotoffload

import (
	"context"
	"log/slog"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

// Design §5 retention/GC.
//
// GC is deliberately two-phase and asymmetric in what it trusts:
//
//	Phase 1 deletes MANIFEST objects that fall outside the policy.
//	Phase 2 deletes PAYLOAD objects that no surviving manifest names.
//
// The asymmetry exists because payloads are content-addressed and
// therefore SHARED: two groups (or two generations within a group)
// that snapshot identical bytes converge on one payload object. A
// payload may only be reclaimed once the live set has been rebuilt
// from EVERY surviving manifest in the whole prefix — not just the
// group being trimmed. Getting this wrong deletes live data, so every
// ambiguity below resolves toward keeping the object.
const (
	// DefaultMinGenerations is the floor on manifests kept per group,
	// independent of age. Restores are rare and usually urgent, so the
	// default keeps a fallback generation when the newest snapshot
	// turns out to be unusable.
	DefaultMinGenerations = 3

	// DefaultMaxAge keeps every manifest published inside this window
	// even when that exceeds MinGenerations.
	DefaultMaxAge = 14 * 24 * time.Hour

	// DefaultMinMarkAge is the default two-pass sweep delay. It
	// matches DefaultPayloadGrace because both bound the same thing:
	// the longest plausible time between a publisher touching a
	// payload and its manifest committing.
	DefaultMinMarkAge = 24 * time.Hour

	// DefaultPayloadGrace is the §5 phase-2 delay before an
	// unreferenced payload may be reclaimed. It covers the window in
	// which a payload has been uploaded (payload-first publish) but
	// its manifest has not yet committed: during that window the
	// payload is legitimately unreferenced and must NOT be collected.
	// The grace period must therefore exceed the longest plausible
	// publish duration.
	DefaultPayloadGrace = 24 * time.Hour

	payloadKeyRelativeParts = 2
)

// RetentionPolicy is the per-prefix retention rule.
//
// A manifest survives phase 1 if it is within MinGenerations newest
// for its group OR younger than MaxAge. The newest valid manifest for
// a group is always retained regardless of both, per §9's acceptance
// criterion that retention never deletes a group's newest manifest.
type RetentionPolicy struct {
	MinGenerations int
	MaxAge         time.Duration
	PayloadGrace   time.Duration
	// MinMarkAge is the §5 two-pass sweep delay: a payload must have
	// been marked for at least this long, and must not have changed
	// since the mark, before it may be reclaimed.
	//
	// It exists because no conditional-delete primitive on a
	// general-purpose S3 bucket can detect a content-preserving
	// refresh: `If-Match` compares a content-derived ETag, which a
	// republish of identical bytes leaves untouched, and
	// `IfMatchLastModifiedTime` is directory-buckets only. Comparing
	// the object's observed state ACROSS passes is what detects that
	// refresh, so it must exceed the longest plausible publish.
	MinMarkAge time.Duration
}

func (p RetentionPolicy) withDefaults() RetentionPolicy {
	if p.MinGenerations <= 0 {
		p.MinGenerations = DefaultMinGenerations
	}
	if p.MaxAge <= 0 {
		p.MaxAge = DefaultMaxAge
	}
	if p.PayloadGrace <= 0 {
		p.PayloadGrace = DefaultPayloadGrace
	}
	if p.MinMarkAge <= 0 {
		p.MinMarkAge = DefaultMinMarkAge
	}
	return p
}

// GCResult reports what one RunOnce did and, just as importantly, what
// it refused to do. PayloadPhaseSkipped with a non-empty
// MalformedManifests is the expected shape when the bucket contains a
// manifest this build cannot parse.
type GCResult struct {
	GroupsScanned       int
	ManifestsScanned    int
	ManifestsDeleted    []string
	PayloadsDeleted     []string
	MalformedManifests  []string
	PayloadPhaseSkipped bool
	// SkipReason explains PayloadPhaseSkipped in operator-facing
	// terms. Empty when the phase ran.
	SkipReason string
	// ManifestsClaimedConcurrently counts expired manifests whose
	// compare-and-delete lost to a concurrent publish rewriting the
	// same key (an idempotent republish of the same index/term).
	ManifestsClaimedConcurrently int
	// PayloadsAwaitingSweep counts payloads that are eligible for
	// reclamation but are still serving the §5 two-pass sweep delay.
	// A steady non-zero value on a busy prefix is normal.
	PayloadsAwaitingSweep int
	// PayloadsClaimedConcurrently counts payloads that were eligible
	// for reclamation but whose compare-and-delete lost to a
	// concurrent publish reusing them. A steady non-zero value is
	// healthy dedup traffic, not an error.
	PayloadsClaimedConcurrently int
}

// GC implements the §5 retention and payload-reclamation passes over
// one object-store prefix.
type GC struct {
	store  RetentionStore
	prefix string
	policy RetentionPolicy
	now    func() time.Time
	log    *slog.Logger

	// marks is the §5 two-pass sweep state: the first pass that finds
	// a payload unreferenced and past grace records what it saw, and
	// only a LATER pass that finds the same object unchanged may
	// delete it. A publisher that refreshes the payload anywhere
	// between the two passes changes its mtime, which invalidates the
	// mark and spares the object — the coordination that a
	// content-derived ETag precondition cannot provide.
	//
	// The state is in-memory and per-process. Losing it on restart is
	// safe in the only direction that matters: reclamation is delayed
	// by one more pass, never advanced.
	marksMu sync.Mutex
	marks   map[string]payloadMark
}

// payloadMark records the state of a payload when it was first seen
// eligible for reclamation.
type payloadMark struct {
	at        time.Time
	size      int64
	updatedAt time.Time
}

// matches reports whether ref is the same object state that was
// marked. Any difference means the object was rewritten since — the
// signal that a publisher reused it.
func (m payloadMark) matches(ref ObjectRef) bool {
	return m.size == ref.Size && m.updatedAt.Equal(ref.UpdatedAt)
}

// GCOptions configures NewGC.
type GCOptions struct {
	Store  RetentionStore
	Prefix string
	Policy RetentionPolicy
	// Now defaults to time.Now. Injected so tests can drive the age
	// windows deterministically rather than by sleeping.
	Now    func() time.Time
	Logger *slog.Logger
}

func NewGC(opts GCOptions) (*GC, error) {
	if opts.Store == nil {
		return nil, errors.Wrap(ErrInvalidOptions, "retention requires an object store")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	return &GC{
		store:  opts.Store,
		prefix: cleanObjectPrefix(opts.Prefix),
		policy: opts.Policy.withDefaults(),
		now:    now,
		log:    log,
		marks:  make(map[string]payloadMark),
	}, nil
}

// RunOnce executes both phases. It returns an error only when the scan
// itself failed; a scan that completes but declines to reclaim
// payloads returns a nil error and a GCResult with
// PayloadPhaseSkipped set.
func (g *GC) RunOnce(ctx context.Context) (GCResult, error) {
	if g == nil {
		return GCResult{}, errors.Wrap(ErrInvalidOptions, "gc is required")
	}

	scan, err := g.scanManifests(ctx)
	if err != nil {
		// §5: listing or pagination failure performs no deletes.
		return GCResult{}, err
	}

	result := GCResult{
		GroupsScanned:      len(scan.byGroup),
		ManifestsScanned:   scan.scanned,
		MalformedManifests: scan.malformed,
	}

	survivors, expired := g.partition(scan)
	payloadSkipReason := g.payloadPhaseBlockedBy(scan)
	var payloadRefs []ObjectRef
	if payloadSkipReason == "" {
		payloadRefs, err = g.listPayloadObjects(ctx)
		if err != nil {
			return result, err
		}
	}

	for _, entry := range expired {
		deleted, err := g.compareAndDeleteManifest(ctx, entry)
		if err != nil {
			return result, err
		}
		if !deleted {
			result.ManifestsClaimedConcurrently++
			continue
		}
		result.ManifestsDeleted = append(result.ManifestsDeleted, entry.key)
	}

	if payloadSkipReason != "" {
		result.PayloadPhaseSkipped = true
		result.SkipReason = payloadSkipReason
		g.log.Warn("snapshot offload payload reclamation skipped",
			"reason", payloadSkipReason,
			"malformed_manifests", len(scan.malformed))
		return result, nil
	}

	deleted, claimed, marked, err := g.reclaimPayloads(ctx, survivors, payloadRefs)
	result.PayloadsDeleted = deleted
	result.PayloadsClaimedConcurrently = claimed
	result.PayloadsAwaitingSweep = marked
	if err != nil {
		return result, err
	}
	return result, nil
}

// manifestScan is the phase-1 view of the prefix.
type manifestScan struct {
	byGroup   map[uint64][]scannedManifest
	malformed []string
	scanned   int
}

type scannedManifest struct {
	key string
	// ref is the object state observed when the manifest was listed.
	// It is the compare-and-delete precondition for phase 1, for the
	// same reason phase 2 needs one: an idempotent publish retry can
	// rewrite this exact key between the retention decision and the
	// delete, and an unconditional delete would then remove a
	// manifest the publisher believes it just committed.
	ref      ObjectRef
	manifest Manifest
	// createdAt is the manifest's own CreatedAt. The object store's
	// mtime is deliberately NOT used here: a bucket copy or a
	// lifecycle transition rewrites mtime and would silently reset
	// every manifest's apparent age.
	createdAt time.Time
}

type malformedManifestError struct {
	err error
}

func (e *malformedManifestError) Error() string {
	return e.err.Error()
}

func (e *malformedManifestError) Unwrap() error {
	return e.err
}

func malformedManifest(err error) error {
	return &malformedManifestError{err: err}
}

func isMalformedManifest(err error) bool {
	var target *malformedManifestError
	return errors.As(err, &target)
}

func (g *GC) scanManifests(ctx context.Context) (manifestScan, error) {
	groupsPrefix := path.Join(g.prefix, "v1", "groups")
	refs, err := g.store.ListObjects(ctx, groupsPrefix)
	if err != nil {
		return manifestScan{}, errors.Wrap(err, "retention: list manifests")
	}

	scan := manifestScan{byGroup: make(map[uint64][]scannedManifest)}
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if !strings.HasSuffix(ref.Key, manifestObjectSuffix) {
			continue
		}
		// A store that returns the same key twice — overlapping pages
		// from an S3-compatible endpoint while objects change — would
		// otherwise be counted as two generations of the same
		// manifest. With MinGenerations 1 one copy lands in survivors
		// and the other in expired, so phase 1 would delete the very
		// key chosen as the group's newest restore point.
		if _, dup := seen[ref.Key]; dup {
			continue
		}
		seen[ref.Key] = struct{}{}
		scan.scanned++
		manifest, err := g.loadManifest(ctx, ref)
		if err != nil {
			if !isMalformedManifest(err) {
				return manifestScan{}, errors.Wrapf(err, "retention: load manifest %s", ref.Key)
			}
			// §5: malformed manifests are reported and excluded from
			// deletion. They also block payload reclamation entirely
			// (see payloadPhaseBlockedBy) because an unparseable
			// manifest may reference a payload we cannot enumerate.
			scan.malformed = append(scan.malformed, ref.Key)
			g.log.Error("snapshot offload retention found malformed manifest",
				"manifest_key", ref.Key, "error", err)
			continue
		}
		scan.byGroup[manifest.GroupID] = append(scan.byGroup[manifest.GroupID], scannedManifest{
			key:       ref.Key,
			ref:       ref,
			manifest:  manifest,
			createdAt: manifest.CreatedAt,
		})
	}
	return scan, nil
}

func (g *GC) loadManifest(ctx context.Context, ref ObjectRef) (Manifest, error) {
	if ref.Size > maxManifestBytes {
		return Manifest{}, malformedManifest(errors.Wrapf(ErrInvalidOptions,
			"manifest %s exceeds %d bytes", ref.Key, maxManifestBytes))
	}
	body, _, err := g.store.GetObject(ctx, ref.Key)
	if err != nil {
		return Manifest{}, errors.Wrapf(err, "get manifest %s", ref.Key)
	}
	defer func() { _ = body.Close() }()
	data, err := readLimitedManifest(ctx, body)
	if err != nil {
		if errors.Is(err, ErrInvalidOptions) {
			return Manifest{}, malformedManifest(errors.Wrapf(err, "read manifest %s", ref.Key))
		}
		return Manifest{}, errors.Wrapf(err, "read manifest %s", ref.Key)
	}
	manifest, err := DecodeManifest(data)
	if err != nil {
		return Manifest{}, malformedManifest(errors.Wrapf(err, "decode manifest %s", ref.Key))
	}
	if normalizeObjectKey(ref.Key) != normalizeObjectKey(manifest.ManifestKey) {
		return Manifest{}, malformedManifest(errors.Wrapf(ErrIntegrity,
			"manifest key mismatch: listed %s, body says %s", ref.Key, manifest.ManifestKey))
	}
	// Self-consistency is not enough: a body may agree with its own
	// ManifestKey while its group/index/term disagree with the path it
	// is stored under. Retention groups and orders by the BODY, so a
	// high-index body claiming group 2 parked under a group-1 path
	// would consume group 2's retained-generation slots and get its
	// real newest manifests deleted. Re-derive the canonical key and
	// treat any disagreement as malformed.
	canonical, err := manifestKey(g.prefix, manifest.GroupID, manifest.SnapshotIndex, manifest.SnapshotTerm)
	if err != nil {
		return Manifest{}, malformedManifest(errors.Wrapf(err, "derive canonical key for %s", ref.Key))
	}
	if normalizeObjectKey(ref.Key) != normalizeObjectKey(canonical) {
		return Manifest{}, malformedManifest(errors.Wrapf(ErrIntegrity,
			"manifest %s is stored off its canonical path %s (group=%d index=%d term=%d)",
			ref.Key, canonical, manifest.GroupID, manifest.SnapshotIndex, manifest.SnapshotTerm))
	}
	return manifest, nil
}

// partition splits every scanned manifest into survivors and the keys
// phase 1 may delete.
func (g *GC) partition(scan manifestScan) ([]scannedManifest, []scannedManifest) {
	var (
		survivors []scannedManifest
		expired   []scannedManifest
	)
	cutoff := g.now().Add(-g.policy.MaxAge)

	for _, manifests := range scan.byGroup {
		// Newest first, so index < MinGenerations is "recent".
		sorted := append([]scannedManifest(nil), manifests...)
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].manifest.SnapshotIndex != sorted[j].manifest.SnapshotIndex {
				return sorted[i].manifest.SnapshotIndex > sorted[j].manifest.SnapshotIndex
			}
			return sorted[i].manifest.SnapshotTerm > sorted[j].manifest.SnapshotTerm
		})
		for i, entry := range sorted {
			if g.retains(i, entry, cutoff) {
				survivors = append(survivors, entry)
				continue
			}
			expired = append(expired, entry)
		}
	}
	sort.Slice(expired, func(i, j int) bool { return expired[i].key < expired[j].key })
	return survivors, expired
}

// retains decides one manifest's fate. Index 0 is the group's newest
// valid manifest and is retained unconditionally.
//
// That first branch is deliberately NOT folded into the
// MinGenerations check even though withDefaults currently clamps
// MinGenerations to >= 1, which makes the two overlap today. §9 makes
// "never delete a group's newest manifest" a standalone safety
// property, and resting it on a policy floor means a later change
// that permits MinGenerations == 0 (say, an age-only policy) would
// silently make the last restore point deletable. Stating the
// invariant here keeps it true regardless of the policy.
func (g *GC) retains(index int, entry scannedManifest, cutoff time.Time) bool {
	switch {
	case index == 0:
		return true
	case index < g.policy.MinGenerations:
		return true
	case entry.createdAt.After(cutoff):
		return true
	default:
		return false
	}
}

// payloadPhaseBlockedBy returns a non-empty reason when phase 2 must
// not run. Every branch here is a case where the live payload set
// cannot be proven complete, and deleting on an incomplete live set
// destroys referenced data.
func (g *GC) payloadPhaseBlockedBy(scan manifestScan) string {
	if len(scan.malformed) > 0 {
		return "malformed manifests present; live payload set cannot be proven complete"
	}
	return ""
}

func (g *GC) listPayloadObjects(ctx context.Context) ([]ObjectRef, error) {
	payloadsPrefix := path.Join(g.prefix, "v1", "payloads")
	refs, err := g.store.ListObjects(ctx, payloadsPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "retention: list payloads")
	}
	return refs, nil
}

// reclaimPayloads is §5 phase 2: rebuild the live object-key set from
// every surviving manifest across every group, then delete only
// payload objects that are both unreferenced and older than the grace
// period.
func (g *GC) reclaimPayloads(ctx context.Context, survivors []scannedManifest, refs []ObjectRef) ([]string, int, int, error) {
	live := make(map[string]struct{}, len(survivors))
	for _, entry := range survivors {
		live[normalizeObjectKey(entry.manifest.Payload.Key)] = struct{}{}
	}

	// One revalidation pass for the whole phase, taken AFTER the
	// payload listing so a manifest committed between the two is
	// visible. Doing this per payload turns a stale-payload backlog
	// into N listings and O(N×M) reads.
	fresh, safe, err := g.revalidateLiveKeys(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	if !safe {
		// A malformed manifest appeared since phase 1; the live set
		// can no longer be proven complete, so reclaim nothing.
		return nil, 0, 0, nil
	}
	for key := range fresh {
		live[key] = struct{}{}
	}

	graceCutoff := g.now().Add(-g.policy.PayloadGrace)
	var (
		deleted []string
		claimed int
		marked  int
	)
	for _, ref := range refs {
		key, ok, err := g.reclaimPayload(ctx, live, graceCutoff, ref)
		if err != nil {
			// A payload claimed by a concurrent publish is a normal
			// outcome, not a failure: skip it and keep going.
			if errors.Is(err, errObjectClaimed) {
				claimed++
				continue
			}
			if errors.Is(err, errPayloadMarked) {
				marked++
				continue
			}
			return deleted, claimed, marked, err
		}
		if !ok {
			continue
		}
		deleted = append(deleted, key)
	}
	sort.Strings(deleted)
	// Drop marks for payloads no longer in the listing. Without this,
	// a payload removed by another GC process or a bucket lifecycle
	// rule never passes through reclaimPayload again, so no dropMark
	// call can reach it and its mark leaks for the process's lifetime.
	g.pruneMarks(refs)
	return deleted, claimed, marked, nil
}

// pruneMarks discards marks whose object was absent from the listing
// this pass. refs is the COMPLETE payload listing (ListObjects is
// all-or-error), so absence is authoritative.
func (g *GC) pruneMarks(refs []ObjectRef) {
	present := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		present[ref.Key] = struct{}{}
	}

	g.marksMu.Lock()
	defer g.marksMu.Unlock()
	for key := range g.marks {
		if _, ok := present[key]; !ok {
			delete(g.marks, key)
		}
	}
}

func (g *GC) reclaimPayload(
	ctx context.Context,
	live map[string]struct{},
	graceCutoff time.Time,
	ref ObjectRef,
) (string, bool, error) {
	sha, ok := payloadSHAFromKey(g.prefix, ref.Key)
	if !ok {
		g.log.Warn("snapshot offload retention skipped unrecognized payload object",
			"object_key", ref.Key)
		return "", false, nil
	}
	if payloadKeyIsLive(live, ref.Key) || !beforeGraceCutoff(ref.UpdatedAt, graceCutoff) {
		// Referenced again, or freshly touched: forget any mark so a
		// later eligibility has to serve its own full sweep delay.
		g.dropMark(ref.Key)
		return "", false, nil
	}
	if !g.sweepable(ref) {
		return "", false, errPayloadMarked
	}
	info, exists, err := g.store.HeadObject(ctx, ref.Key)
	if err != nil {
		return "", false, errors.Wrapf(err, "retention: head payload %s", ref.Key)
	}
	if !exists || !beforeGraceCutoff(info.UpdatedAt, graceCutoff) {
		g.dropMark(ref.Key)
		return "", false, nil
	}
	if err := g.compareAndDeletePayload(ctx, refreshed(ref, info), sha); err != nil {
		return "", false, err
	}
	g.dropMark(ref.Key)
	g.log.Info("snapshot offload retention reclaimed payload",
		"object_key", ref.Key, "sha256", sha)
	return ref.Key, true, nil
}

// sweepable implements the §5 two-pass rule. It reports true only when
// this payload was marked on an earlier pass, has not changed since,
// and the mark has aged past MinMarkAge. Otherwise it (re-)marks the
// object and reports false.
//
// The DELAY is the protection: a publisher that refreshes a reused
// payload and then commits its manifest completes well inside one
// inter-pass interval, so the sweep pass sees the refreshed mtime (via
// the grace check) or the new manifest (via the live set) and spares
// the object. A single-pass GC had no such window.
//
// The state comparison is a secondary consistency check rather than
// the primary mechanism: because a refresh sets mtime to now, the
// grace check already rejects a refreshed object on its own. Keeping
// the comparison makes a mark mean "this exact object state has been
// quiet", so a mark can never be honoured for bytes that changed
// underneath it — including a size change that left mtime untouched,
// which the grace check alone would miss.
func (g *GC) sweepable(ref ObjectRef) bool {
	now := g.now()

	g.marksMu.Lock()
	defer g.marksMu.Unlock()

	mark, marked := g.marks[ref.Key]
	if !marked || !mark.matches(ref) {
		if marked {
			g.log.Info("snapshot offload retention re-marked a payload that changed since the last pass",
				"object_key", ref.Key)
		}
		g.marks[ref.Key] = payloadMark{at: now, size: ref.Size, updatedAt: ref.UpdatedAt}
		return false
	}
	return now.Sub(mark.at) >= g.policy.MinMarkAge
}

func (g *GC) dropMark(key string) {
	g.marksMu.Lock()
	defer g.marksMu.Unlock()
	delete(g.marks, key)
}

// MarkedPayloads returns the payload keys currently held under a sweep
// mark. Exposed for operator tooling and tests; the set is per-process
// and advisory.
func (g *GC) MarkedPayloads() []string {
	g.marksMu.Lock()
	defer g.marksMu.Unlock()
	keys := make([]string, 0, len(g.marks))
	for key := range g.marks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// errPayloadMarked reports that a payload is waiting out its sweep
// delay. It never escapes reclaimPayloads.
var errPayloadMarked = errors.New("snapshot offload: payload marked, awaiting the sweep delay")

// compareAndDeleteManifest deletes an expired manifest only if it
// still matches the state the scan observed. It reports deleted=false
// (not an error) when a concurrent publish rewrote the key, since
// leaving a just-republished manifest in place is the correct outcome.
func (g *GC) compareAndDeleteManifest(ctx context.Context, entry scannedManifest) (bool, error) {
	err := g.store.DeleteObjectIfUnmodified(ctx, entry.key, PreconditionFor(entry.ref))
	switch {
	case err == nil:
		g.log.Info("snapshot offload retention deleted manifest",
			"manifest_key", entry.key)
		return true, nil
	case errors.Is(err, ErrObjectModified):
		g.log.Info("snapshot offload retention skipped manifest rewritten by a concurrent publish",
			"manifest_key", entry.key)
		return false, nil
	default:
		return false, errors.Wrapf(err, "retention: delete manifest %s", entry.key)
	}
}

// compareAndDeletePayload deletes the payload only if it still matches
// the state reclaimPayload validated.
//
// An unconditional delete here would still lose the race with a
// publisher that reuses this payload: it can refresh the object and
// commit a manifest between the validating head and this call, leaving
// a committed manifest pointing at deleted bytes.
func (g *GC) compareAndDeletePayload(ctx context.Context, ref ObjectRef, sha string) error {
	err := g.store.DeleteObjectIfUnmodified(ctx, ref.Key, PreconditionFor(ref))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrObjectModified):
		// A concurrent publish claimed the payload. Not an error:
		// leaving it alone is the correct outcome, and the next pass
		// reclaims it if it really is garbage.
		g.log.Info("snapshot offload retention skipped payload claimed by a concurrent publish",
			"object_key", ref.Key, "sha256", sha)
		return errObjectClaimed
	default:
		return errors.Wrapf(err, "retention: delete payload %s", ref.Key)
	}
}

// errObjectClaimed marks the benign "a publisher took this payload
// back" outcome so the caller can count it without treating it as a
// failure. It never escapes reclaimPayloads.
var errObjectClaimed = errors.New("snapshot offload: payload claimed by a concurrent publish")

// refreshed merges the freshly-headed state into the listed ref so the
// delete precondition describes what was actually validated, not the
// possibly-staler listing.
func refreshed(ref ObjectRef, info ObjectInfo) ObjectRef {
	ref.Size = info.Size
	if !info.UpdatedAt.IsZero() {
		ref.UpdatedAt = info.UpdatedAt
	}
	if info.ETag != "" {
		ref.ETag = info.ETag
	}
	return ref
}

func payloadKeyIsLive(live map[string]struct{}, key string) bool {
	_, ok := live[normalizeObjectKey(key)]
	return ok
}

func beforeGraceCutoff(updatedAt, graceCutoff time.Time) bool {
	return !updatedAt.IsZero() && updatedAt.Before(graceCutoff)
}

// revalidateLiveKeys re-reads the manifest tree ONCE, after the payload
// listing, and returns the set of payload keys it still references.
//
// The freshness matters: the live set used for the delete decision must
// be at least as new as the payload listing, or a manifest committed
// between the two would look absent. But it must be rebuilt once per
// pass, not once per payload — a prefix with a large stale-payload
// backlog would otherwise issue N listings and O(N×M) object reads and
// never finish its first cleanup.
//
// A second return of false means the re-scan itself found the prefix
// unsafe to reclaim from (a malformed manifest appeared), in which case
// the caller must skip the phase entirely.
func (g *GC) revalidateLiveKeys(ctx context.Context) (map[string]struct{}, bool, error) {
	scan, err := g.scanManifests(ctx)
	if err != nil {
		return nil, false, err
	}
	if g.payloadPhaseBlockedBy(scan) != "" {
		return nil, false, nil
	}
	live := make(map[string]struct{}, scan.scanned)
	for _, manifests := range scan.byGroup {
		for _, entry := range manifests {
			live[normalizeObjectKey(entry.manifest.Payload.Key)] = struct{}{}
		}
	}
	return live, true, nil
}

// payloadSHAFromKey recovers the content hash from a payload object
// key laid out as <prefix>/v1/payloads/sha256/<xx>/<sha><suffix>.
// It verifies the two-character shard directory matches the hash so a
// hand-placed object cannot masquerade as a payload.
func payloadSHAFromKey(prefix, key string) (string, bool) {
	normalized := normalizeObjectKey(key)
	if normalized != key {
		return "", false
	}
	payloadsPrefix := path.Join(cleanObjectPrefix(prefix), "v1", "payloads", "sha256")
	rel, ok := strings.CutPrefix(normalized, payloadsPrefix+"/")
	if !ok {
		return "", false
	}
	parts := strings.Split(rel, "/")
	if len(parts) != payloadKeyRelativeParts {
		return "", false
	}
	base := parts[1]
	if !strings.HasSuffix(base, payloadObjectSuffix) {
		return "", false
	}
	sha := strings.TrimSuffix(base, payloadObjectSuffix)
	if !isSHA256Hex(sha) {
		return "", false
	}
	if shard := parts[0]; shard != sha[:2] {
		return "", false
	}
	return sha, true
}
