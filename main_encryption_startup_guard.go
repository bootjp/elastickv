package main

import (
	"errors"
	"fmt"
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
		return nil
	}

	gapEngine, ok := engine.(encryptionGapEngine)
	if !ok {
		return fmt.Errorf("encryption startup guard: engine for default group %d does not implement encryptionGapEngine (missing AppliedIndex+EncryptionScanner)", defaultGroup)
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
