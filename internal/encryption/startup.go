package encryption

import (
	"errors"
	"os"
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
// by Stage 6C-1 and returns the first guard that fires. nil means
// every guard passed and the process is safe to proceed past startup
// into buildShardGroups / Raft engine wiring.
//
// Scope (Stage 6C-1, this PR):
//
//   - ErrSidecarPresentWithoutFlag: sidecar on disk but flag off
//     (downgrade prevention)
//   - ErrKEKRequiredWithFlag: flag on but no KEK source (fail-fast)
//   - ErrKEKMismatch: flag on, KEK loaded, sidecar present, at least
//     one wrapped DEK fails to unwrap under the configured KEK
//     (operator-error catch)
//
// Out of scope (deferred to later 6C sub-milestones / Stage 6D / 6E):
//
//   - ErrUnsupportedFilesystem startup probe (WriteSidecar already
//     surfaces this on the first write; a dedicated startup probe
//     can ship without re-architecting this helper).
//   - ErrLocalEpochExhausted (active DEK local_epoch == 0xFFFF) —
//     coupled with the writer-registry record (Stage 7); shipping
//     the exhaustion check without the rollback peer would create
//     an asymmetric posture that future readers would have to
//     reason around.
//   - ErrNodeIDCollision — requires the cluster-wide Voters ∪
//     Learners membership view that Stage 6D's capability fan-out
//     provides; node-local hashing cannot detect cross-node
//     collisions on its own.
//   - ErrSidecarBehindRaftLog — requires raftengine integration
//     (read the persisted applied index) which is the same data
//     path Stage 6D / 6E touch for the cutover gate; bundled
//     there.
//   - Snapshot cutover divergence, raft-envelope-without-bootstrap,
//     RPC local_epoch range — Stage 6E (raft cutover).
//
// The function reads the sidecar at most once and is safe to call
// before any other encryption package state is constructed; it
// does NOT mutate the on-disk sidecar.
func CheckStartupGuards(cfg StartupConfig) error {
	sidecarPresent, err := sidecarOnDisk(cfg.SidecarPath)
	if err != nil {
		return err
	}

	if err := guardSidecarWithoutFlag(cfg, sidecarPresent); err != nil {
		return err
	}
	if err := guardKEKRequired(cfg); err != nil {
		return err
	}
	if err := guardKEKMatchesSidecar(cfg, sidecarPresent); err != nil {
		return err
	}
	return nil
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
// Returns nil (does not fire) when:
//   - encryption is not enabled (other guards cover that path),
//   - the sidecar is absent (nothing to unwrap),
//   - the KEK is nil (the guardKEKRequired check already fired and
//     was either acted on or, in tests, deliberately suppressed),
//   - the sidecar exists but has no wrapped DEKs (e.g., a freshly
//     created empty sidecar — bootstrap not yet committed).
func guardKEKMatchesSidecar(cfg StartupConfig, sidecarPresent bool) error {
	if !cfg.EncryptionEnabled || !sidecarPresent || cfg.KEK == nil {
		return nil
	}
	sc, err := ReadSidecar(cfg.SidecarPath)
	if err != nil {
		return pkgerrors.Wrapf(err, "encryption: read sidecar %q for KEK-mismatch guard", cfg.SidecarPath)
	}
	for idStr, k := range sc.Keys {
		if len(k.Wrapped) == 0 {
			continue
		}
		if _, err := cfg.KEK.Unwrap(k.Wrapped); err != nil {
			id, parseErr := strconv.ParseUint(idStr, 10, 32)
			if parseErr != nil {
				// validateSidecar would have caught a non-decimal
				// key_id, but be defensive in case ReadSidecar's
				// invariant changes.
				return pkgerrors.Wrapf(ErrKEKMismatch,
					"sidecar=%q key_id=%q: %v", cfg.SidecarPath, idStr, err)
			}
			return pkgerrors.Wrapf(ErrKEKMismatch,
				"sidecar=%q key_id=%d purpose=%q: %v",
				cfg.SidecarPath, id, k.Purpose, err)
		}
	}
	return nil
}
