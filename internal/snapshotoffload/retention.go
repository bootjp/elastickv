package snapshotoffload

import (
	"context"
	"log/slog"
	"path"
	"sort"
	"strings"
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

	deleted, claimed, err := g.reclaimPayloads(ctx, survivors, payloadRefs)
	result.PayloadsDeleted = deleted
	result.PayloadsClaimedConcurrently = claimed
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
	for _, ref := range refs {
		if !strings.HasSuffix(ref.Key, manifestObjectSuffix) {
			continue
		}
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
func (g *GC) reclaimPayloads(ctx context.Context, survivors []scannedManifest, refs []ObjectRef) ([]string, int, error) {
	live := make(map[string]struct{}, len(survivors))
	for _, entry := range survivors {
		live[normalizeObjectKey(entry.manifest.Payload.Key)] = struct{}{}
	}

	graceCutoff := g.now().Add(-g.policy.PayloadGrace)
	var (
		deleted []string
		claimed int
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
			return deleted, claimed, err
		}
		if !ok {
			continue
		}
		deleted = append(deleted, key)
	}
	sort.Strings(deleted)
	return deleted, claimed, nil
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
		return "", false, nil
	}
	if referenced, err := g.payloadCurrentlyReferenced(ctx, ref.Key); err != nil {
		return "", false, errors.Wrapf(err, "retention: revalidate payload %s", ref.Key)
	} else if referenced {
		return "", false, nil
	}
	info, exists, err := g.store.HeadObject(ctx, ref.Key)
	if err != nil {
		return "", false, errors.Wrapf(err, "retention: head payload %s", ref.Key)
	}
	if !exists || !beforeGraceCutoff(info.UpdatedAt, graceCutoff) {
		return "", false, nil
	}
	if err := g.compareAndDeletePayload(ctx, refreshed(ref, info), sha); err != nil {
		return "", false, err
	}
	g.log.Info("snapshot offload retention reclaimed payload",
		"object_key", ref.Key, "sha256", sha)
	return ref.Key, true, nil
}

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

func (g *GC) payloadCurrentlyReferenced(ctx context.Context, key string) (bool, error) {
	scan, err := g.scanManifests(ctx)
	if err != nil {
		return false, err
	}
	if g.payloadPhaseBlockedBy(scan) != "" {
		return true, nil
	}
	target := normalizeObjectKey(key)
	for _, manifests := range scan.byGroup {
		for _, entry := range manifests {
			if normalizeObjectKey(entry.manifest.Payload.Key) == target {
				return true, nil
			}
		}
	}
	return false, nil
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
