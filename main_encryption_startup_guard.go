package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/bootjp/elastickv/internal/encryption"
	pkgerrors "github.com/cockroachdb/errors"
)

// encryptionGapEngine is the subset of an opened Raft engine that
// the §9.1 ErrSidecarBehindRaftLog startup guard needs. The
// production etcd-raft engine (internal/raftengine/etcd.Engine)
// satisfies this interface structurally via its AppliedIndex()
// and EncryptionScanner() methods.
//
// Defining a local interface (rather than depending on the
// concrete engine type) follows the same pattern as
// encryptionAdminEngine in main_encryption_admin.go: it keeps
// main.go's startup-guard wiring decoupled from the engine
// implementation and lets tests substitute a stub without
// pulling in the full engine surface.
type encryptionGapEngine interface {
	AppliedIndex() uint64
	EncryptionScanner() encryption.EncryptionRelevantScanner
}

// checkSidecarBehindRaftLog implements the §9.1 ErrSidecarBehindRaftLog
// startup-guard phase (Stage 6C-2d). It runs AFTER buildShardGroups
// has opened each shard's engine and BEFORE any gRPC server starts
// serving, in a different lifecycle phase from the §9.1 6C-1 / 6C-2
// guards (which run BEFORE engine startup via CheckStartupGuards).
//
// For each runtime whose spec.id matches defaultGroup (the group
// that processes encryption FSM applies and advances the sidecar's
// RaftAppliedIndex), the guard reads the sidecar, fetches the
// engine's AppliedIndex(), and asks
// encryption.GuardSidecarBehindRaftLog whether the gap covers any
// §5.5 sidecar-mutating entry. A non-nil return aborts startup with
// a typed error wrapping the cause.
//
// Skipped conditions (return nil without consulting the engine):
//
//   - encryptionEnabled is false: no encryption opt-in, no gap to
//     refuse on. The sidecar's raft_applied_index is irrelevant
//     because nothing reads it.
//
//   - sidecarPath is empty: operator hasn't configured a sidecar
//     location, same posture as a non-encrypted cluster.
//
//   - sidecar file does NOT exist on disk: no bootstrap has
//     committed yet, so there are no wrapped DEKs to be stale.
//     The first ApplyBootstrap will create the sidecar with a
//     RaftAppliedIndex caught up to the engine.
//
//   - default-group runtime is nil or its engine is nil: the
//     engine isn't open for this shard, so there's nothing to
//     scan. The earlier startup-guard layer (6C-1/6C-2) would
//     have caught a missing engine before this point.
//
// Why only the default group: the §5.1 sidecar tracks a SINGLE
// raft_applied_index, advanced by WriteSidecar calls in
// ApplyBootstrap / ApplyRotation on the default group's FSM. Other
// shards' Raft logs don't carry encryption FSM entries, so their
// applied index has no relationship to the sidecar's. Running the
// guard against a non-default shard would always report
// caught-up (gap == 0 because the default group's WriteSidecar
// doesn't bump the sidecar's index relative to OTHER shards).
func checkSidecarBehindRaftLog(
	runtimes []*raftGroupRuntime,
	defaultGroup uint64,
	sidecarPath string,
	encryptionEnabled bool,
) error {
	if !encryptionEnabled {
		return nil
	}
	if sidecarPath == "" {
		return nil
	}
	if _, err := os.Stat(sidecarPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return pkgerrors.Wrapf(err, "encryption startup guard: stat sidecar %q", sidecarPath)
	}

	rt := findDefaultGroupRuntime(runtimes, defaultGroup)
	if rt == nil {
		return nil
	}
	engine := rt.snapshotEngine()
	if engine == nil {
		// A nil engine at the §9.1 phase means buildShardGroups
		// reported success but did not actually populate this
		// runtime's engine — the lifecycle should have failed
		// fast upstream and never reached here. Treat as a
		// startup-init failure rather than silently returning
		// "guard passed", which would let the cluster boot
		// without ever inspecting the sidecar gap.
		return pkgerrors.Errorf(
			"encryption startup guard: engine for default group %d is nil (buildShardGroups returned without an opened engine)",
			defaultGroup)
	}

	gapEngine, ok := engine.(encryptionGapEngine)
	if !ok {
		return pkgerrors.Errorf("encryption startup guard: engine for default group %d does not implement encryptionGapEngine (missing AppliedIndex+EncryptionScanner)", defaultGroup)
	}

	return runSidecarBehindRaftLogGuard(gapEngine, sidecarPath, defaultGroup)
}

// runSidecarBehindRaftLogGuard is the inner per-engine half of
// checkSidecarBehindRaftLog: it reads the sidecar, invokes
// encryption.GuardSidecarBehindRaftLog with the engine's applied
// index + scanner, and wraps the result with the context the
// operator will see in their log line. Split out so tests can
// exercise the guard logic without constructing a full
// raftengine.Engine — they pass an encryptionGapEngine stub
// directly.
func runSidecarBehindRaftLogGuard(
	gapEngine encryptionGapEngine,
	sidecarPath string,
	defaultGroup uint64,
) error {
	sidecar, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		return pkgerrors.Wrapf(err, "encryption startup guard: read sidecar %q", sidecarPath)
	}
	// Stage 6C-2d transitional gate: the §6.3 applier
	// (internal/encryption/applier.go) does not yet advance
	// `sidecar.raft_applied_index` on ApplyBootstrap / ApplyRotation
	// — `writeBootstrapSidecar` and `writeRotationSidecar` only
	// mutate Active.{Storage,Raft} and the wrapped DEK row. Until
	// that applier-side advancement ships (Stage 6C-2e or equivalent
	// follow-up), an encrypted cluster that committed bootstrap /
	// rotation entries will persist `raft_applied_index=0` on disk.
	// Firing the guard against (0, engine.applied] would falsely
	// classify those committed-and-applied historical entries as
	// "sidecar behind" and refuse startup on every restart — turning
	// the guard into a brick-the-cluster regression instead of a
	// safety net.
	//
	// Skip-when-zero is fail-OPEN by design for this transitional
	// window. The fail-OPEN posture matches the pre-6C-2d behaviour
	// (no guard at all) so this PR does not regress any production
	// cluster. Once the applier advances raft_applied_index, the
	// skip condition is naturally a no-op (RaftAppliedIndex > 0)
	// and the guard becomes active. A WARN-level log line surfaces
	// the transitional state so an operator can see the dependency
	// hasn't shipped.
	if sidecar.RaftAppliedIndex == 0 {
		slog.Warn("encryption startup guard skipped: sidecar.raft_applied_index=0 (applier-side advancement is a Stage 6C-2e follow-up; guard is wired but dormant until then)",
			slog.String("sidecar_path", sidecarPath),
			slog.Uint64("default_group", defaultGroup),
			slog.Uint64("engine_applied_index", gapEngine.AppliedIndex()))
		return nil
	}
	if err := encryption.GuardSidecarBehindRaftLog(
		sidecar.RaftAppliedIndex,
		gapEngine.AppliedIndex(),
		gapEngine.EncryptionScanner(),
	); err != nil {
		return pkgerrors.Wrapf(err,
			"encryption startup guard: sidecar=%q default_group=%d",
			sidecarPath, defaultGroup)
	}
	return nil
}

// chainEncryptionStartupGuard runs checkSidecarBehindRaftLog
// only if prevErr is nil, returning whichever error fires
// first. Lets the caller compose buildShardGroups + the Stage
// 6C-2d guard in a single conditional, keeping run()'s cyclop
// budget intact.
func chainEncryptionStartupGuard(
	prevErr error,
	runtimes []*raftGroupRuntime,
	defaultGroup uint64,
	sidecarPath string,
	encryptionEnabled bool,
) error {
	if prevErr != nil {
		return prevErr
	}
	return checkSidecarBehindRaftLog(runtimes, defaultGroup, sidecarPath, encryptionEnabled)
}

// findDefaultGroupRuntime returns the runtime whose spec.id
// matches defaultGroup, or nil if no such runtime is present in
// the supplied slice. Used by the startup-guard phase to scope
// the §9.1 gap check to the group whose Raft log advances the
// sidecar's raft_applied_index.
func findDefaultGroupRuntime(runtimes []*raftGroupRuntime, defaultGroup uint64) *raftGroupRuntime {
	for _, rt := range runtimes {
		if rt != nil && rt.spec.id == defaultGroup {
			return rt
		}
	}
	return nil
}
