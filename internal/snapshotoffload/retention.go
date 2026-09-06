package snapshotoffload

import (
	"context"
	"io"
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

	for _, key := range expired {
		if err := g.store.DeleteObject(ctx, key); err != nil {
			// Report what was already deleted alongside the error so
			// the caller can see the pass was partial.
			result.ManifestsDeleted = append(result.ManifestsDeleted, key)
			return result, errors.Wrapf(err, "retention: delete manifest %s", key)
		}
		result.ManifestsDeleted = append(result.ManifestsDeleted, key)
		g.log.Info("snapshot offload retention deleted manifest",
			"manifest_key", key)
	}

	if reason := g.payloadPhaseBlockedBy(scan); reason != "" {
		result.PayloadPhaseSkipped = true
		result.SkipReason = reason
		g.log.Warn("snapshot offload payload reclamation skipped",
			"reason", reason,
			"malformed_manifests", len(scan.malformed))
		return result, nil
	}

	deleted, err := g.reclaimPayloads(ctx, survivors)
	result.PayloadsDeleted = deleted
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
	key      string
	manifest Manifest
	// createdAt is the manifest's own CreatedAt. The object store's
	// mtime is deliberately NOT used here: a bucket copy or a
	// lifecycle transition rewrites mtime and would silently reset
	// every manifest's apparent age.
	createdAt time.Time
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
		manifest, err := g.loadManifest(ctx, ref.Key)
		if err != nil {
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
			manifest:  manifest,
			createdAt: manifest.CreatedAt,
		})
	}
	return scan, nil
}

func (g *GC) loadManifest(ctx context.Context, key string) (Manifest, error) {
	body, _, err := g.store.GetObject(ctx, key)
	if err != nil {
		return Manifest{}, errors.Wrapf(err, "get manifest %s", key)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		return Manifest{}, errors.Wrapf(err, "read manifest %s", key)
	}
	manifest, err := DecodeManifest(data)
	if err != nil {
		return Manifest{}, errors.Wrapf(err, "decode manifest %s", key)
	}
	return manifest, nil
}

// partition splits every scanned manifest into survivors and the keys
// phase 1 may delete.
func (g *GC) partition(scan manifestScan) ([]scannedManifest, []string) {
	var (
		survivors []scannedManifest
		expired   []string
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
			expired = append(expired, entry.key)
		}
	}
	sort.Strings(expired)
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

// reclaimPayloads is §5 phase 2: rebuild the live SHA set from every
// surviving manifest across every group, then delete only payload
// objects that are both unreferenced and older than the grace period.
func (g *GC) reclaimPayloads(ctx context.Context, survivors []scannedManifest) ([]string, error) {
	live := make(map[string]struct{}, len(survivors))
	for _, entry := range survivors {
		live[entry.manifest.Payload.SHA256] = struct{}{}
	}

	payloadsPrefix := path.Join(g.prefix, "v1", "payloads")
	refs, err := g.store.ListObjects(ctx, payloadsPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "retention: list payloads")
	}

	graceCutoff := g.now().Add(-g.policy.PayloadGrace)
	var deleted []string
	for _, ref := range refs {
		sha, ok := payloadSHAFromKey(ref.Key)
		if !ok {
			// An object under the payload prefix that does not parse
			// as a payload key is left alone: it is not ours to
			// reclaim and may belong to a future layout version.
			g.log.Warn("snapshot offload retention skipped unrecognized payload object",
				"object_key", ref.Key)
			continue
		}
		if _, referenced := live[sha]; referenced {
			continue
		}
		if !ref.UpdatedAt.Before(graceCutoff) {
			// Inside the grace window: this is very likely a
			// payload-first upload whose manifest has not committed
			// yet. Deleting it would break an in-flight publish.
			continue
		}
		if err := g.store.DeleteObject(ctx, ref.Key); err != nil {
			return deleted, errors.Wrapf(err, "retention: delete payload %s", ref.Key)
		}
		deleted = append(deleted, ref.Key)
		g.log.Info("snapshot offload retention reclaimed payload",
			"object_key", ref.Key, "sha256", sha)
	}
	sort.Strings(deleted)
	return deleted, nil
}

// payloadSHAFromKey recovers the content hash from a payload object
// key laid out as <prefix>/v1/payloads/sha256/<xx>/<sha><suffix>.
// It verifies the two-character shard directory matches the hash so a
// hand-placed object cannot masquerade as a payload.
func payloadSHAFromKey(key string) (string, bool) {
	base := path.Base(key)
	if !strings.HasSuffix(base, payloadObjectSuffix) {
		return "", false
	}
	sha := strings.TrimSuffix(base, payloadObjectSuffix)
	if !isSHA256Hex(sha) {
		return "", false
	}
	if shard := path.Base(path.Dir(key)); shard != sha[:2] {
		return "", false
	}
	return sha, true
}
