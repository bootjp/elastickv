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
func (g *GC) RunOnce(ctx context.Context) (result GCResult, retErr error) {
	if g == nil {
		return GCResult{}, errors.Wrap(ErrInvalidOptions, "gc is required")
	}

	scan, err := g.scanManifests(ctx)
	if err != nil {
		// §5: listing or pagination failure performs no deletes.
		return GCResult{}, err
	}

	result = GCResult{
		GroupsScanned:      len(scan.byGroup),
		ManifestsScanned:   scan.scanned,
		MalformedManifests: scan.malformed,
	}

	var plan retentionPlan
	defer func() {
		retErr = errors.CombineErrors(retErr, releaseRetentionClaims(ctx, plan))
	}()
	plan, err = g.prepareRetentionPlan(ctx, scan)
	if err != nil {
		return result, err
	}
	scan = plan.scan
	result.GroupsScanned = len(scan.byGroup)
	result.ManifestsScanned = scan.scanned
	result.MalformedManifests = scan.malformed
	result.PayloadsClaimedConcurrently = plan.payloadsClaimedConcurrently
	result.PayloadsAwaitingSweep = plan.awaitingSweep
	survivors, expired := g.partition(scan)
	live := livePayloadKeys(survivors)
	result.ManifestsDeleted, result.ManifestsClaimedConcurrently, err =
		g.deleteExpiredManifests(ctx, expired, plan.manifestClaims, live)
	if err != nil {
		return result, err
	}

	if plan.skipReason != "" {
		result.PayloadPhaseSkipped = true
		result.SkipReason = plan.skipReason
		g.log.Warn("snapshot offload payload reclamation skipped",
			"reason", plan.skipReason,
			"malformed_manifests", len(scan.malformed))
		return result, nil
	}

	deleted, claimed, err := g.reclaimPayloads(ctx, live, plan.payloadClaims, plan.refs)
	result.PayloadsDeleted = deleted
	result.PayloadsClaimedConcurrently += claimed
	if err != nil {
		return result, err
	}
	return result, nil
}

type retentionPlan struct {
	scan                        manifestScan
	refs                        []ObjectRef
	payloadClaims               []claimedPayload
	manifestClaims              map[string]claimedManifest
	payloadsClaimedConcurrently int
	awaitingSweep               int
	skipReason                  string
}

type claimedManifest struct {
	ref            ObjectRef
	manifestSHA256 string
	claim          ObjectClaim
}

func (g *GC) prepareRetentionPlan(ctx context.Context, scan manifestScan) (retentionPlan, error) {
	initialMalformed := append([]string(nil), scan.malformed...)
	plan := retentionPlan{
		scan:           scan,
		skipReason:     g.payloadPhaseBlockedBy(scan),
		manifestClaims: make(map[string]claimedManifest),
	}
	if plan.skipReason == "" {
		survivors, _ := g.partition(scan)
		live := livePayloadKeys(survivors)
		refs, err := g.listPayloadObjects(ctx)
		if err != nil {
			return plan, err
		}
		plan.refs = refs
		plan.payloadClaims, plan.payloadsClaimedConcurrently, plan.awaitingSweep, err =
			g.claimSweepablePayloads(ctx, live, refs)
		if err != nil {
			return plan, err
		}
	}

	_, expired := g.partition(scan)
	manifestClaims, err := g.claimExpiredManifests(ctx, expired)
	plan.manifestClaims = manifestClaims
	if err != nil {
		return plan, err
	}

	// This is the final fallible manifest scan for the pass. It must
	// complete BEFORE phase-one deletes. Payload and manifest claims remain
	// held through the scan and deletion, closing both publish-versus-sweep
	// races. A manifest that changed before its claim was acquired is also
	// skipped by comparing this scan with the initial ref.
	plan.scan, err = g.scanManifests(ctx)
	if err != nil {
		return plan, err
	}
	plan.scan.malformed = unionSortedStrings(initialMalformed, plan.scan.malformed)
	if finalSkipReason := g.payloadPhaseBlockedBy(plan.scan); finalSkipReason != "" {
		plan.skipReason = finalSkipReason
	}
	return plan, nil
}

func (g *GC) claimExpiredManifests(
	ctx context.Context, expired []scannedManifest,
) (map[string]claimedManifest, error) {
	claims := make(map[string]claimedManifest, len(expired))
	for _, entry := range expired {
		claim, err := g.store.AcquireObjectClaim(ctx, entry.key)
		if err != nil {
			if errors.Is(err, ErrObjectClaimed) {
				continue
			}
			return claims, errors.Wrapf(err, "retention: claim manifest %s", entry.key)
		}
		claims[entry.key] = claimedManifest{
			ref:            entry.ref,
			manifestSHA256: entry.manifest.ManifestSHA256,
			claim:          claim,
		}
	}
	return claims, nil
}

func protectManifestPayload(live map[string]struct{}, entry scannedManifest) {
	live[normalizeObjectKey(entry.manifest.Payload.Key)] = struct{}{}
}

func (g *GC) deleteExpiredManifests(
	ctx context.Context,
	expired []scannedManifest,
	claims map[string]claimedManifest,
	live map[string]struct{},
) ([]string, int, error) {
	var deletedKeys []string
	claimedConcurrently := 0
	for _, entry := range expired {
		claimed, ok := claims[entry.key]
		if !ok || !sameObjectState(claimed.ref, entry.ref) ||
			claimed.manifestSHA256 != entry.manifest.ManifestSHA256 {
			protectManifestPayload(live, entry)
			claimedConcurrently++
			continue
		}
		deleted, err := g.deleteClaimedManifest(ctx, entry)
		if err != nil {
			return deletedKeys, claimedConcurrently, err
		}
		if !deleted {
			protectManifestPayload(live, entry)
			claimedConcurrently++
			continue
		}
		deletedKeys = append(deletedKeys, entry.key)
	}
	return deletedKeys, claimedConcurrently, nil
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

func unionSortedStrings(left, right []string) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	for _, value := range left {
		seen[value] = struct{}{}
	}
	for _, value := range right {
		seen[value] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
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
	if err := verifyManifestPaths(g.prefix, ref, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// verifyManifestPaths rejects a manifest whose recorded keys disagree with the
// canonical paths its own body implies.
//
// Split out of loadManifest so the three checks read as one rule rather than
// accumulating inline branches.
func verifyManifestPaths(prefix string, ref ObjectRef, manifest Manifest) error {
	if normalizeObjectKey(ref.Key) != normalizeObjectKey(manifest.ManifestKey) {
		return malformedManifest(errors.Wrapf(ErrIntegrity,
			"manifest key mismatch: listed %s, body says %s", ref.Key, manifest.ManifestKey))
	}
	// Self-consistency is not enough: a body may agree with its own
	// ManifestKey while its group/index/term disagree with the path it
	// is stored under. Retention groups and orders by the BODY, so a
	// high-index body claiming group 2 parked under a group-1 path
	// would consume group 2's retained-generation slots and get its
	// real newest manifests deleted. Re-derive the canonical key and
	// treat any disagreement as malformed.
	canonical, err := manifestKey(prefix, manifest.GroupID, manifest.SnapshotIndex, manifest.SnapshotTerm)
	if err != nil {
		return malformedManifest(errors.Wrapf(err, "derive canonical key for %s", ref.Key))
	}
	if normalizeObjectKey(ref.Key) != normalizeObjectKey(canonical) {
		return malformedManifest(errors.Wrapf(ErrIntegrity,
			"manifest %s is stored off its canonical path %s (group=%d index=%d term=%d)",
			ref.Key, canonical, manifest.GroupID, manifest.SnapshotIndex, manifest.SnapshotTerm))
	}
	// The payload reference has to be canonical for THIS prefix too. A
	// manifest under prefix A naming a payload under prefix B records the
	// exact B key in A's live set, but A's scan never lists B -- and B's
	// scan cannot see A's manifest, so B eventually reclaims a payload A
	// still needs and A's restore breaks. Re-deriving the key makes a
	// cross-prefix reference malformed instead of a latent data loss.
	canonicalPayload, err := payloadKey(prefix, manifest.Payload.SHA256)
	if err != nil {
		return malformedManifest(errors.Wrapf(err, "derive canonical payload key for %s", ref.Key))
	}
	if normalizeObjectKey(manifest.Payload.Key) != normalizeObjectKey(canonicalPayload) {
		return malformedManifest(errors.Wrapf(ErrIntegrity,
			"manifest %s references payload %s outside its own prefix (canonical %s)",
			ref.Key, manifest.Payload.Key, canonicalPayload))
	}
	return nil
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

func livePayloadKeys(survivors []scannedManifest) map[string]struct{} {
	live := make(map[string]struct{}, len(survivors))
	for _, entry := range survivors {
		live[normalizeObjectKey(entry.manifest.Payload.Key)] = struct{}{}
	}
	return live
}

type claimedPayload struct {
	ref   ObjectRef
	sha   string
	claim ObjectClaim
}

// claimSweepablePayloads takes claims for every payload this pass may delete.
// Claims are acquired before the final manifest scan and held through delete,
// so a publisher cannot complete inside the scan-to-delete window.
func (g *GC) claimSweepablePayloads(
	ctx context.Context,
	live map[string]struct{},
	refs []ObjectRef,
) ([]claimedPayload, int, int, error) {
	graceCutoff := g.now().Add(-g.policy.PayloadGrace)
	var (
		claims  []claimedPayload
		claimed int
		marked  int
	)
	for _, ref := range refs {
		sha, ok := payloadSHAFromKey(g.prefix, ref.Key)
		if !ok {
			g.log.Warn("snapshot offload retention skipped unrecognized payload object",
				"object_key", ref.Key)
			continue
		}
		if payloadKeyIsLive(live, ref.Key) || !beforeGraceCutoff(ref.UpdatedAt, graceCutoff) {
			g.dropMark(ref.Key)
			continue
		}
		if !g.sweepable(ref) {
			marked++
			continue
		}
		claim, err := g.store.AcquireObjectClaim(ctx, ref.Key)
		if err != nil {
			if errors.Is(err, ErrObjectClaimed) {
				claimed++
				continue
			}
			return claims, claimed, marked, errors.Wrapf(err, "retention: claim payload %s", ref.Key)
		}
		claims = append(claims, claimedPayload{ref: ref, sha: sha, claim: claim})
	}
	return claims, claimed, marked, nil
}

// reclaimPayloads is §5 phase 2. Every entry in claims has been locked since
// before the authoritative manifest scan used to build live.
func (g *GC) reclaimPayloads(
	ctx context.Context,
	live map[string]struct{},
	claims []claimedPayload,
	refs []ObjectRef,
) ([]string, int, error) {
	graceCutoff := g.now().Add(-g.policy.PayloadGrace)
	var (
		deleted []string
		claimed int
	)
	for _, candidate := range claims {
		if payloadKeyIsLive(live, candidate.ref.Key) {
			g.dropMark(candidate.ref.Key)
			continue
		}
		key, ok, err := g.reclaimClaimedPayload(ctx, candidate.ref, candidate.sha, graceCutoff)
		if err != nil {
			if errors.Is(err, errObjectClaimed) {
				claimed++
				continue
			}
			return deleted, claimed, err
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
	return deleted, claimed, nil
}

func releaseRetentionClaims(ctx context.Context, plan retentionPlan) error {
	return releaseRetentionClaimsWithin(ctx, plan, claimReleaseTimeout)
}

func releaseRetentionClaimsWithin(ctx context.Context, plan retentionPlan, maxWait time.Duration) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), maxWait)
	defer cancel()

	claims := make([]ObjectClaim, 0, len(plan.manifestClaims)+len(plan.payloadClaims))
	for _, candidate := range plan.manifestClaims {
		claims = append(claims, candidate.claim)
	}
	for _, candidate := range plan.payloadClaims {
		claims = append(claims, candidate.claim)
	}
	if len(claims) == 0 {
		return nil
	}

	jobs := make(chan ObjectClaim, len(claims))
	errs := make(chan error, len(claims))
	for _, claim := range claims {
		jobs <- claim
	}
	close(jobs)

	var workers sync.WaitGroup
	for range min(len(claims), claimReleaseConcurrency) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for claim := range jobs {
				errs <- releaseObjectClaimWithContext(releaseCtx, claim)
			}
		}()
	}
	workers.Wait()
	close(errs)

	var releaseErr error
	for err := range errs {
		releaseErr = errors.CombineErrors(releaseErr, err)
	}
	if releaseErr != nil {
		return errors.Wrap(releaseErr, "release retention claims")
	}
	return nil
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

func (g *GC) reclaimClaimedPayload(
	ctx context.Context,
	ref ObjectRef,
	sha string,
	graceCutoff time.Time,
) (string, bool, error) {
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

func (g *GC) deleteClaimedManifest(ctx context.Context, entry scannedManifest) (bool, error) {
	info, exists, err := g.store.HeadObject(ctx, entry.key)
	if err != nil {
		return false, errors.Wrapf(err, "retention: head manifest %s", entry.key)
	}
	if !exists {
		return false, nil
	}
	current := refreshed(entry.ref, info)
	if !sameObjectState(entry.ref, current) {
		g.log.Info("snapshot offload retention skipped manifest rewritten by a concurrent publish",
			"manifest_key", entry.key)
		return false, nil
	}
	err = g.store.DeleteObjectIfUnmodified(ctx, entry.key, PreconditionFor(current))
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

func sameObjectState(want, got ObjectRef) bool {
	if want.Size != got.Size {
		return false
	}
	if !want.UpdatedAt.IsZero() && !got.UpdatedAt.Equal(want.UpdatedAt) {
		return false
	}
	if want.ETag != "" && got.ETag != want.ETag {
		return false
	}
	return true
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
var errObjectClaimed = ErrObjectClaimed

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
