package encryption

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	pkgerrors "github.com/cockroachdb/errors"
)

// StartupConfig is the input to CheckStartupGuards. Each field is
// derived from the operator-facing flags at process startup; the
// helper exists so the §9.1 refusal logic can be unit-tested
// without reaching back into main.go's flag plumbing.
//
// All paths are operator-supplied (--kekFile, --encryptionSidecarPath);
// empty values mean "operator did not provide one". The presence of a
// sidecar on disk is detected by os.Stat on SidecarPath, not by an
// explicit boolean — operators do not pass "sidecar exists" as a
// flag, the on-disk state is the source of truth.
type StartupConfig struct {
	// EncryptionEnabled mirrors --encryption-enabled. When true the
	// cluster has opted in to the §7.1 rollout; when false the node
	// must refuse to start if encrypted on-disk state already exists
	// (downgrade prevention).
	EncryptionEnabled bool

	// KEKConfigured is true iff --kekFile is non-empty. The KEK
	// itself is supplied via KEK below; KEKConfigured exists
	// independently so the helper can distinguish "operator did
	// not supply a KEK source" from "supplied but failed to load"
	// (the latter is handled at main.go's loadKEKWrapperFromFlag()
	// call site, before this guard runs).
	KEKConfigured bool

	// KEK is the loaded KEK wrapper. May be nil iff KEKConfigured
	// is false. When EncryptionEnabled and KEKConfigured are both
	// true the guard uses KEK.Unwrap to verify each wrapped DEK
	// in the sidecar decrypts under the configured KEK.
	KEK KEKUnwrapper

	// SidecarPath is the absolute path to the §5.1 keys.json file.
	// May be empty; an empty path skips every sidecar-dependent
	// guard. Stage 6B-2's triple-gate readback (sidecarPath !=
	// "") covers the mutator-RPC side, this field covers the
	// startup-refusal side.
	SidecarPath string
}

// CheckStartupGuards runs the §9.1 startup-refusal guards covered
// by Stage 6C-1 + 6C-2 and returns the first guard that fires.
// nil means every guard passed and the process is safe to proceed
// past startup into buildShardGroups / Raft engine wiring.
//
// Scope (Stage 6C-1, PR #778):
//
//   - ErrSidecarPresentWithoutFlag: sidecar on disk but flag off
//     (downgrade prevention)
//   - ErrKEKRequiredWithFlag: flag on but no KEK source (fail-fast)
//   - ErrKEKMismatch: flag on, KEK loaded, sidecar present, at least
//     one wrapped DEK fails to unwrap under the configured KEK
//     (operator-error catch)
//
// Scope (Stage 6C-2, this PR):
//
//   - ErrLocalEpochExhausted: any active DEK in the sidecar has
//     reached local_epoch == 0xFFFF (would-be GCM nonce-reuse on
//     the next encrypted write). Refuse to start until rotated.
//   - ErrUnsupportedFilesystem: ProbeSidecarFilesystem exercises
//     the §5.1 write+rename+dir.Sync sequence at startup so the
//     filesystem incompatibility surfaces BEFORE the first
//     encryption-relevant Raft entry, not on the first WriteSidecar
//     call (which on a fresh node may be hours into operation).
//
// Out of scope (deferred to later sub-milestones / Stage 6D / 6E):
//
//   - ErrNodeIDCollision — requires the cluster-wide Voters ∪
//     Learners membership view that Stage 6D's capability fan-out
//     provides; node-local hashing cannot detect cross-node
//     collisions on its own. Bundled with Stage 6C-3 / 6D.
//   - ErrLocalEpochRollback — needs the writer-registry record
//     (Stage 7); the exhaustion peer ships here, the rollback peer
//     ships once the registry is available. Bundled with Stage 6C-3.
//   - ErrSidecarBehindRaftLog — requires raftengine integration
//     (read the persisted applied index) which is the same data
//     path Stage 6D / 6E touch for the cutover gate; bundled
//     there. Deferred from 6C-2 to keep this PR focused on
//     guards that need only the encryption package's own state.
//   - Snapshot cutover divergence, raft-envelope-without-bootstrap,
//     RPC local_epoch range — Stage 6E (raft cutover) / 6C-4.
//
// The function reads the sidecar AT MOST ONCE (cached across every
// sidecar-reading guard) and is safe to call before any other
// encryption package state is constructed; it does NOT mutate the
// on-disk sidecar. ProbeSidecarFilesystem creates a sentinel file
// in the sidecar's parent directory and removes it before returning,
// leaving the on-disk state indistinguishable from before the call.
//
// The single-read invariant matters as new sidecar-reading guards
// land in 6C-2b / 6C-3 / 6C-4: shipping each new guard with its
// own ReadSidecar call would silently grow the startup IO from
// O(1) to O(N guards), and a partial-write race between the
// guards' reads is harder to reason about than a single snapshot
// shared across all of them (claude r1 medium on PR #781).
func CheckStartupGuards(cfg StartupConfig) error {
	sidecarPresent, err := sidecarOnDisk(cfg.SidecarPath)
	if err != nil {
		return err
	}

	// Load the sidecar once for every guard that needs it. nil
	// means "no sidecar to consult" — either the path is empty,
	// the file is absent, or the guards that consume sidecar
	// state were short-circuited before this point.
	sc, err := loadSidecarForGuards(cfg, sidecarPresent)
	if err != nil {
		return err
	}

	if err := guardSidecarWithoutFlag(cfg, sidecarPresent); err != nil {
		return err
	}
	if err := guardKEKRequired(cfg); err != nil {
		return err
	}
	if err := guardKEKMatchesSidecar(cfg, sc); err != nil {
		return err
	}
	if err := guardLocalEpochExhausted(cfg, sc); err != nil {
		return err
	}
	if err := guardFilesystemDurability(cfg); err != nil {
		return err
	}
	return nil
}

// loadSidecarForGuards parses the sidecar exactly once iff at least
// one downstream guard needs it. Returns (nil, nil) when no guard
// will consume it (sidecar absent, encryption off, or KEK missing
// — guardKEKMatchesSidecar would short-circuit anyway), avoiding
// the unnecessary ReadSidecar I/O.
func loadSidecarForGuards(cfg StartupConfig, sidecarPresent bool) (*Sidecar, error) {
	if !sidecarPresent {
		return nil, nil
	}
	if !cfg.EncryptionEnabled {
		// guardSidecarWithoutFlag will fire first; the sidecar
		// contents are irrelevant to the refusal.
		return nil, nil
	}
	sc, err := ReadSidecar(cfg.SidecarPath)
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "encryption: read sidecar %q for startup guards", cfg.SidecarPath)
	}
	return sc, nil
}

// sidecarOnDisk reports whether SidecarPath names an existing file.
// An empty path returns (false, nil): no sidecar configured →
// nothing to refuse on. A genuine I/O error (permission denied,
// path traversal failure) is propagated; the operator must resolve
// it before the process can decide.
func sidecarOnDisk(path string) (bool, error) {
	if path == "" {
		return false, nil
	}
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, pkgerrors.Wrapf(err, "encryption: stat sidecar %q", path)
}

// guardSidecarWithoutFlag fires the §9.1 downgrade-prevention check.
// On-disk encryption state (sidecar present) with --encryption-enabled
// off would silently route new writes to cleartext, which is the
// classic "downgrade attack via misconfiguration" footgun — better
// to refuse to start than to half-honor a prior bootstrap.
func guardSidecarWithoutFlag(cfg StartupConfig, sidecarPresent bool) error {
	if cfg.EncryptionEnabled || !sidecarPresent {
		return nil
	}
	return pkgerrors.Wrapf(ErrSidecarPresentWithoutFlag,
		"sidecar=%q", cfg.SidecarPath)
}

// guardKEKRequired fires when the operator turned on
// --encryption-enabled without supplying --kekFile. A flag-on /
// KEK-off node would refuse every mutator at the Stage 6B-2 RPC
// gate AND HaltApply if a mutator ever did commit, neither of
// which matches the operator's stated intent. Fail fast at startup.
func guardKEKRequired(cfg StartupConfig) error {
	if !cfg.EncryptionEnabled {
		return nil
	}
	if cfg.KEKConfigured {
		return nil
	}
	return ErrKEKRequiredWithFlag
}

// guardKEKMatchesSidecar attempts to KEK-unwrap every wrapped DEK
// in the sidecar. A single failure fires ErrKEKMismatch with the
// offending key_id annotated — the classic operator error here is
// "wrong --kekFile points at a key from a different cluster" and
// the key_id identifies which DEK could not be unwrapped, which is
// almost always enough to root-cause.
//
// Sidecar keys are visited in ascending key_id order so that when
// more than one wrapped DEK fails the reported key_id / purpose is
// REPRODUCIBLE across process restarts. Map-order iteration would
// pick a different DEK each restart, breaking runbook log
// correlation (claude r1 MEDIUM on PR #778).
//
// Returns nil (does not fire) when:
//   - encryption is not enabled (other guards cover that path),
//   - the sidecar is absent (nothing to unwrap),
//   - the KEK is nil (the guardKEKRequired check already fired and
//     was either acted on or, in tests, deliberately suppressed),
//   - the sidecar exists but has no wrapped DEKs (e.g., a freshly
//     created empty sidecar — bootstrap not yet committed).
func guardKEKMatchesSidecar(cfg StartupConfig, sc *Sidecar) error {
	if !cfg.EncryptionEnabled || sc == nil || cfg.KEK == nil {
		return nil
	}
	keyIDs := sortedSidecarKeyIDs(sc.Keys)
	for _, idStr := range keyIDs {
		k := sc.Keys[idStr]
		if len(k.Wrapped) == 0 {
			continue
		}
		if _, err := cfg.KEK.Unwrap(k.Wrapped); err != nil {
			id, parseErr := strconv.ParseUint(idStr, 10, 32)
			if parseErr != nil {
				// validateSidecar would have caught a non-decimal
				// key_id, but be defensive in case ReadSidecar's
				// invariant changes. Include parseErr so an
				// operator triaging a manual-edit sidecar can see
				// BOTH the unwrap failure and the malformed key_id
				// (gemini medium on PR #778; deferred to 6C-2).
				return pkgerrors.Wrapf(ErrKEKMismatch,
					"sidecar=%q key_id=%q (parse error: %v): %v",
					cfg.SidecarPath, idStr, parseErr, err)
			}
			return pkgerrors.Wrapf(ErrKEKMismatch,
				"sidecar=%q key_id=%d purpose=%q: %v",
				cfg.SidecarPath, id, k.Purpose, err)
		}
	}
	return nil
}

// guardLocalEpochExhausted fires when any active DEK in the sidecar
// has reached the uint16 saturation value (math.MaxUint16). §4.1
// reserves 16 bits for local_epoch in the nonce; emitting one more
// nonce under the same DEK would either roll the counter back to 0
// (re-issuing a nonce already in use → GCM catastrophic) or saturate
// at 0xFFFF (the same outcome on the next bump). The right recovery
// is a deliberate DEK rotation (§5.2), which retires the exhausted
// DEK and lets the next process startup proceed with a fresh
// local_epoch=0 DEK.
//
// Why both Active.Storage and Active.Raft are checked: §4.1 nonce
// construction is per-DEK; an exhausted storage DEK and a fresh raft
// DEK can coexist in the sidecar after a rotation that only rotated
// one purpose. The guard fires on either, identifying which purpose's
// rotation the operator must run.
//
// Why only ACTIVE keys are checked: a retired DEK's local_epoch is
// frozen at the value it had when retired; reading old envelopes
// wrapped under a retired DEK does not consume a new nonce, so an
// exhausted retired DEK is harmless. Only a DEK that the next write
// would actually wrap a new nonce under matters here.
func guardLocalEpochExhausted(cfg StartupConfig, sc *Sidecar) error {
	if !cfg.EncryptionEnabled || sc == nil {
		return nil
	}
	if err := checkActiveDEKEpochSaturated(sc, SidecarPurposeStorage, sc.Active.Storage, cfg.SidecarPath); err != nil {
		return err
	}
	if err := checkActiveDEKEpochSaturated(sc, SidecarPurposeRaft, sc.Active.Raft, cfg.SidecarPath); err != nil {
		return err
	}
	return nil
}

// checkActiveDEKEpochSaturated reports ErrLocalEpochExhausted iff
// the active DEK for the given purpose has local_epoch ==
// math.MaxUint16. A zero active id means "not bootstrapped for this
// purpose" and is silently passed through.
func checkActiveDEKEpochSaturated(sc *Sidecar, purpose string, activeID uint32, sidecarPath string) error {
	if activeID == ReservedKeyID {
		return nil
	}
	idStr := strconv.FormatUint(uint64(activeID), 10)
	k, ok := sc.Keys[idStr]
	if !ok {
		// validateSidecar would have caught this in ReadSidecar
		// via ErrSidecarActiveKeyMissing; defensive bail-out.
		return nil
	}
	if k.LocalEpoch == math.MaxUint16 {
		return pkgerrors.Wrapf(ErrLocalEpochExhausted,
			"sidecar=%q purpose=%q active_key_id=%d local_epoch=0x%X (max=0x%X)",
			sidecarPath, purpose, activeID, k.LocalEpoch, math.MaxUint16)
	}
	return nil
}

// guardFilesystemDurability runs the §5.1 write+rename+dir.Sync
// probe on the sidecar's parent directory. It only fires when the
// operator has supplied --encryptionSidecarPath (otherwise there
// is no encryption directory whose durability we need to verify).
// The check skips silently if encryption is off — a pre-rollout
// cluster on NFS is no worse off than today (cleartext storage
// has its own durability surface; this guard's scope is the
// §5.1 sidecar specifically).
func guardFilesystemDurability(cfg StartupConfig) error {
	if !cfg.EncryptionEnabled || cfg.SidecarPath == "" {
		return nil
	}
	return ProbeSidecarFilesystem(cfg.SidecarPath)
}

// ProbeSidecarFilesystem exercises the §5.1 crash-durable write
// protocol against the parent directory of sidecarPath WITHOUT
// disturbing an existing sidecar file. The probe writes a small
// sentinel file, fsyncs it, renames it, dir.Syncs the parent
// directory, and removes the sentinel. Any step that fails returns
// the failure wrapped with ErrUnsupportedFilesystem so callers can
// errors.Is-match it identically to the WriteSidecar path.
//
// The probe runs before any encryption-relevant Raft entry can
// commit (CheckStartupGuards is called from main.go before
// buildShardGroups). Detecting NFS or a FUSE mount whose dir.Sync
// is a no-op at startup is dramatically cheaper than discovering
// it days later when the first bootstrap proposal commits and the
// sidecar write fails halfway through, leaving the cluster's
// Raft-committed bootstrap entry without a durable on-disk DEK.
//
// Implementation notes:
//   - The parent directory must exist; we do not mkdir it because
//     the operator-supplied --encryptionSidecarPath is meant to
//     name an EXISTING data directory. Missing parent is propagated
//     as the underlying os.MkdirTemp error (NOT wrapped with
//     ErrUnsupportedFilesystem — a missing dir is a config error,
//     not a filesystem-capability problem).
//   - The sentinel filename is prefixed with .encryption-probe- so
//     a stale file left behind by an interrupted probe is easy to
//     identify, and so it sorts away from the real keys.json in
//     ls output.
//   - We do not need to test atomicity of os.Rename (POSIX
//     guarantees it on the same filesystem); the failure mode this
//     probe specifically catches is dir.Sync being a no-op or
//     erroring out.
func ProbeSidecarFilesystem(sidecarPath string) (retErr error) {
	dir := filepath.Dir(sidecarPath)
	tmp, err := os.CreateTemp(dir, ".encryption-probe-*.tmp")
	if err != nil {
		// Missing dir / permission denied here is a config error,
		// not a filesystem-capability problem. Propagate without
		// the ErrUnsupportedFilesystem mark so operators see the
		// real cause.
		return pkgerrors.Wrapf(err, "encryption: probe sidecar dir %q for fsync support", dir)
	}
	tmpPath := tmp.Name()
	defer func() {
		// Removes whichever file `tmpPath` currently names: either
		// the original CreateTemp output (if we returned before the
		// rename below) or the renamed .final sentinel (if we got
		// past the rename). NOTE: stale .encryption-probe-*.final
		// files left behind by a prior probe that crashed between
		// the rename and the dir.Sync are NOT cleaned up here — they
		// are operator-removable by the .encryption-probe- prefix
		// (claude r1 nit on PR #781).
		_ = os.Remove(tmpPath)
	}()

	if _, err := tmp.Write([]byte("encryption-probe\n")); err != nil {
		_ = tmp.Close()
		return pkgerrors.Wrap(err, "encryption: probe write")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return pkgerrors.Wrapf(
			pkgerrors.WithSecondaryError(ErrUnsupportedFilesystem, err),
			"encryption: probe fsync %q", tmpPath)
	}
	if err := tmp.Close(); err != nil {
		return pkgerrors.Wrapf(err, "encryption: probe close %q", tmpPath)
	}

	// Rename to a sibling and dir.Sync the parent — the same
	// sequence WriteSidecar does. We rename to a distinct
	// final-name so an existing .tmp-named keys.json.tmp on
	// disk (from a prior crash) is not affected.
	finalPath := filepath.Join(dir, filepath.Base(tmpPath)+".final")
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return pkgerrors.Wrapf(err, "encryption: probe rename %q -> %q", tmpPath, finalPath)
	}
	// Re-point the deferred cleanup at the renamed file so we
	// don't leave a sentinel on disk when the probe succeeds.
	tmpPath = finalPath

	parent, err := os.Open(dir) //nolint:gosec // dir comes from operator config
	if err != nil {
		return pkgerrors.Wrapf(err, "encryption: probe open dir %q", dir)
	}
	if err := parent.Sync(); err != nil {
		_ = parent.Close()
		return pkgerrors.Wrapf(
			pkgerrors.WithSecondaryError(ErrUnsupportedFilesystem, err),
			"encryption: probe dir.Sync %q", dir)
	}
	if err := parent.Close(); err != nil {
		return pkgerrors.Wrapf(err, "encryption: probe dir.Close %q", dir)
	}
	return nil
}

// sortedSidecarKeyIDs returns the keys of m in ascending numeric
// key_id order. Sidecar map keys are decimal uint32 strings (per
// §5.1, enforced by validateSidecar), so a lexicographic sort
// would put "10" before "2"; we parse and numeric-sort instead.
// Any malformed key_id (which validateSidecar would have caught
// upstream) is sorted to the end via the math.MaxUint32 sentinel
// so the iteration order stays defined even if invariant drift
// lets a bad entry through.
func sortedSidecarKeyIDs(m map[string]SidecarKey) []string {
	const sortLast = ^uint32(0)
	ids := make([]string, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		ai, aerr := strconv.ParseUint(ids[i], 10, 32)
		bi, berr := strconv.ParseUint(ids[j], 10, 32)
		if aerr != nil {
			ai = uint64(sortLast)
		}
		if berr != nil {
			bi = uint64(sortLast)
		}
		return ai < bi
	})
	return ids
}
