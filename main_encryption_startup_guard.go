package main

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
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

type restoredCutoverReader interface {
	RestoredCutover() uint64
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
//   - default-group runtime is nil: the runtime didn't construct
//     for this shard. The earlier startup-guard layer (6C-1/6C-2)
//     would have caught a fully-missing shard before this point;
//     this branch is the defensive return for misconfigured shard
//     maps that omit the default group entirely.
//
// A NON-nil runtime whose engine is nil is treated as a fatal
// initialisation failure rather than silently skipped — by this
// point buildShardGroups has already constructed the runtime, so
// a nil engine means an opener failed without surfacing an error,
// which would let the guard pass on a node that never finished
// coming up. Fail-closed protects the §9.1 invariant.
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
		return pkgerrors.Errorf(
			"encryption startup guard: engine for default group %d does not implement encryptionGapEngine (missing AppliedIndex+EncryptionScanner)",
			defaultGroup)
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
	// Legacy-sidecar compat gate. Fresh sidecars written by THIS
	// PR's applier (writeBootstrapSidecar / writeRotationSidecar
	// now advance sc.RaftAppliedIndex via advanceRaftAppliedIndex
	// inside the same crash-durable WriteSidecar fsync) will carry
	// a non-zero RaftAppliedIndex from the very first ApplyBootstrap,
	// so this branch is a no-op for any cluster bootstrapped on this
	// version or later.
	//
	// The gate exists to handle sidecars that bootstrapped on a
	// PRE-fix binary, where the applier never wrote
	// raft_applied_index. Those sidecars persist with
	// raft_applied_index=0 even after the cluster successfully
	// committed and applied an encryption-relevant entry; the guard
	// fired against (0, engine.applied] would falsely classify the
	// historical entry as "sidecar behind" and refuse startup on
	// every restart — turning the guard into a brick-the-cluster
	// regression instead of a safety net.
	//
	// Skip-when-zero is fail-OPEN by design for this transitional
	// window. The fail-OPEN posture matches the pre-6C-2d behaviour
	// (no guard at all) so this PR does not regress legacy
	// clusters. A WARN-level log line surfaces the transitional
	// state so an operator can see the legacy posture; running
	// `encryption resync-sidecar` (or any rotation) on the cluster
	// will then advance raft_applied_index and the gate becomes a
	// no-op on subsequent restarts.
	if sidecar.RaftAppliedIndex == 0 {
		slog.Warn("encryption startup guard skipped: sidecar.raft_applied_index=0 (legacy sidecar from a pre-Stage-6C-2d binary; run resync-sidecar or rotate to activate the §9.1 guard)",
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
	if err := checkSidecarBehindRaftLog(runtimes, defaultGroup, sidecarPath, encryptionEnabled); err != nil {
		return err
	}
	return checkEnvelopeCutoverDivergenceStartupGuard(runtimes, defaultGroup, sidecarPath, encryptionEnabled)
}

func checkEnvelopeCutoverDivergenceStartupGuard(
	runtimes []*raftGroupRuntime,
	defaultGroup uint64,
	sidecarPath string,
	encryptionEnabled bool,
) error {
	if !encryptionEnabled || sidecarPath == "" {
		return nil
	}
	sc, err := readExistingSidecarForStartupGuard(sidecarPath)
	if err != nil || sc == nil {
		return err
	}
	return checkEnvelopeCutoverDivergenceForRuntime(runtimes, defaultGroup, sc)
}

func checkEnvelopeCutoverDivergenceBeforeNonceBump(
	raftID string,
	raftDir string,
	groups []groupSpec,
	defaultGroup uint64,
	multi bool,
	sidecarPath string,
	encryptionEnabled bool,
) error {
	if !encryptionEnabled || sidecarPath == "" {
		return nil
	}
	sc, err := readExistingSidecarForStartupGuard(sidecarPath)
	if err != nil {
		return err
	}
	sidecarCutover := uint64(0)
	if sc != nil {
		sidecarCutover = sc.RaftEnvelopeCutoverIndex
	}
	dir := groupDataDir(raftDir, raftID, defaultGroup, multi)
	if !hasGroup(defaultGroup, groups) {
		return nil
	}
	payload, ok, err := etcdraftengine.OpenNewestFSMSnapshotPayload(dir)
	if err != nil || !ok {
		return pkgerrors.Wrap(err, "encryption startup guard: open newest FSM snapshot payload")
	}
	defer payload.Close()
	return checkEnvelopeCutoverDivergenceSnapshotPayload(payload, sidecarCutover, defaultGroup)
}

func checkEnvelopeCutoverDivergenceSnapshotPayload(payload io.Reader, sidecarCutover, defaultGroup uint64) error {
	_, snapshotCutover, err := kv.ReadSnapshotHeader(bufio.NewReader(payload))
	if err != nil {
		return pkgerrors.Wrap(err, "encryption startup guard: read restored snapshot header")
	}
	if snapshotCutover == 0 || snapshotCutover == sidecarCutover {
		return nil
	}
	return pkgerrors.Wrapf(encryption.ErrEnvelopeCutoverDivergence,
		"encryption startup guard: restored_snapshot_cutover=%d sidecar_raft_envelope_cutover=%d default_group=%d",
		snapshotCutover, sidecarCutover, defaultGroup)
}

func hasGroup(groupID uint64, groups []groupSpec) bool {
	for _, g := range groups {
		if g.id == groupID {
			return true
		}
	}
	return false
}

func readExistingSidecarForStartupGuard(sidecarPath string) (*encryption.Sidecar, error) {
	if _, err := os.Stat(sidecarPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, pkgerrors.Wrapf(err, "encryption startup guard: stat sidecar %q", sidecarPath)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "encryption startup guard: read sidecar %q", sidecarPath)
	}
	return sc, nil
}

func checkEnvelopeCutoverDivergenceForRuntime(
	runtimes []*raftGroupRuntime,
	defaultGroup uint64,
	sc *encryption.Sidecar,
) error {
	rt := findDefaultGroupRuntime(runtimes, defaultGroup)
	if rt == nil {
		return nil
	}
	if rt.stateMachine == nil {
		if sc.RaftEnvelopeCutoverIndex == 0 {
			return nil
		}
		return pkgerrors.Errorf(
			"encryption startup guard: state machine for default group %d is nil (cannot compare restored raft envelope cutover)",
			defaultGroup)
	}
	source, ok := rt.stateMachine.(restoredCutoverReader)
	if !ok {
		if sc.RaftEnvelopeCutoverIndex == 0 {
			return nil
		}
		return pkgerrors.Errorf(
			"encryption startup guard: state machine for default group %d cannot expose restored raft envelope cutover",
			defaultGroup)
	}
	snapshotCutover := source.RestoredCutover()
	if snapshotCutover == 0 {
		return nil
	}
	if snapshotCutover != sc.RaftEnvelopeCutoverIndex {
		return pkgerrors.Wrapf(encryption.ErrEnvelopeCutoverDivergence,
			"encryption startup guard: restored_snapshot_cutover=%d sidecar_raft_envelope_cutover=%d default_group=%d",
			snapshotCutover, sc.RaftEnvelopeCutoverIndex, defaultGroup)
	}
	return nil
}

type encryptionMembershipStartupGuardInput struct {
	raftID            string
	raftDir           string
	groups            []groupSpec
	multi             bool
	defaultGroup      uint64
	sidecarPath       string
	encryptionEnabled bool
	bootstrapServers  []raftengine.Server
}

// checkEncryptionMembershipStartupGuardsBeforeEngine implements the
// Stage 6C-3 startup-refusal phase before buildEncryptionWriteWiring
// bumps local_epoch and before buildShardGroups opens Raft engines:
//
//   - ErrNodeIDCollision: every voter/learner ID observed from the
//     persisted etcd-raft peers files (or bootstrap peer list on a
//     brand-new cluster) is narrowed with the same DeriveNodeID →
//     NodeID16 path used by the nonce factory.
//   - ErrLocalEpochRollback: the local sidecar's active storage DEK
//     local_epoch must not be behind this node's writer-registry
//     LastSeenLocalEpoch. Equality is allowed at this pre-bump phase:
//     a healthy restart has sidecar == registry from the previous
//     process load, and prepareStorageNonceEpoch will bump the sidecar
//     before any encrypted write path opens.
func checkEncryptionMembershipStartupGuardsBeforeEngine(in encryptionMembershipStartupGuardInput) error {
	if !in.encryptionEnabled || in.sidecarPath == "" {
		return nil
	}
	sc, err := readExistingSidecarForStartupGuard(in.sidecarPath)
	if err != nil || sc == nil {
		return err
	}
	if sc.Active.Storage == 0 {
		return nil
	}
	if err := checkNodeIDCollisionFromDisk(in.raftID, in.raftDir, in.groups, in.multi, in.bootstrapServers); err != nil {
		return err
	}
	return checkLocalEpochRollbackFromDisk(in.raftID, in.raftDir, in.defaultGroup, in.multi, sc)
}

func checkNodeIDCollisionFromDisk(
	raftID string,
	raftDir string,
	groups []groupSpec,
	multi bool,
	bootstrapServers []raftengine.Server,
) error {
	fullNodeIDs, err := memberFullNodeIDsFromDisk(raftID, raftDir, groups, multi, bootstrapServers)
	if err != nil {
		return err
	}
	if err := encryption.CheckNodeIDCollision(fullNodeIDs); err != nil {
		return pkgerrors.Wrap(err, "encryption startup guard: node_id collision")
	}
	return nil
}

func memberFullNodeIDsFromDisk(
	raftID string,
	raftDir string,
	groups []groupSpec,
	multi bool,
	bootstrapServers []raftengine.Server,
) ([]uint64, error) {
	seen := map[uint64]struct{}{}
	var out []uint64
	add := func(fullNodeID uint64) {
		if fullNodeID == 0 {
			return
		}
		if _, ok := seen[fullNodeID]; ok {
			return
		}
		seen[fullNodeID] = struct{}{}
		out = append(out, fullNodeID)
	}
	for _, g := range groups {
		dir := groupDataDir(raftDir, raftID, g.id, multi)
		peers, ok, err := etcdraftengine.LoadPersistedPeers(dir)
		if err != nil {
			return nil, pkgerrors.Wrapf(err,
				"encryption startup guard: read persisted peers for group %d",
				g.id)
		}
		if ok {
			for _, peer := range peers {
				add(fullNodeIDForPersistedPeer(peer))
			}
			continue
		}
		for _, srv := range bootstrapServers {
			add(etcdraftengine.DeriveNodeID(srv.ID))
		}
	}
	return out, nil
}

func fullNodeIDForPersistedPeer(peer etcdraftengine.Peer) uint64 {
	if peer.ID != "" {
		return etcdraftengine.DeriveNodeID(peer.ID)
	}
	return peer.NodeID
}

func checkLocalEpochRollbackFromDisk(
	raftID string,
	raftDir string,
	defaultGroup uint64,
	multi bool,
	sc *encryption.Sidecar,
) error {
	key, ok := sc.Keys[strconv.FormatUint(uint64(sc.Active.Storage), 10)]
	if !ok {
		return pkgerrors.Wrapf(encryption.ErrSidecarActiveKeyMissing,
			"encryption startup guard: active storage DEK %d missing from sidecar keys",
			sc.Active.Storage)
	}
	fullNodeID := etcdraftengine.DeriveNodeID(raftID)
	storePath := filepath.Join(groupDataDir(raftDir, raftID, defaultGroup, multi), "fsm.db")
	if _, err := os.Stat(storePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := checkLocalEpochRollbackBeforeBump(
				missingWriterRegistry{},
				fullNodeID,
				sc.Active.Storage,
				key.LocalEpoch,
				sc.StorageEnvelopeActive,
			); err != nil {
				return pkgerrors.Wrap(err, "encryption startup guard: local_epoch rollback")
			}
			return nil
		}
		return pkgerrors.Wrapf(err, "encryption startup guard: stat writer registry store %q", storePath)
	}
	st, err := store.NewPebbleStore(storePath)
	if err != nil {
		return pkgerrors.Wrapf(err, "encryption startup guard: open writer registry store %q", storePath)
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			slog.Warn("encryption startup guard: failed to close writer registry store",
				slog.String("store_path", storePath),
				slog.Any("err", closeErr))
		}
	}()
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		return pkgerrors.Wrap(err, "encryption startup guard: writer registry")
	}
	if err := checkLocalEpochRollbackBeforeBump(
		reg,
		fullNodeID,
		sc.Active.Storage,
		key.LocalEpoch,
		sc.StorageEnvelopeActive,
	); err != nil {
		return pkgerrors.Wrap(err, "encryption startup guard: local_epoch rollback")
	}
	return nil
}

func checkLocalEpochRollbackBeforeBump(
	registry encryption.WriterRegistryStore,
	fullNodeID uint64,
	activeStorageDEKID uint32,
	sidecarLocalEpoch uint16,
	storageEnvelopeActive bool,
) error {
	key := encryption.RegistryKey(activeStorageDEKID, encryption.NodeID16(fullNodeID))
	rawVal, ok, err := registry.GetRegistryRow(key)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: read writer-registry row for full_node_id=%#x dek_id=%d",
			fullNodeID, activeStorageDEKID)
	}
	if !ok {
		return missingRegistryRowBeforeBump(storageEnvelopeActive, fullNodeID, activeStorageDEKID)
	}
	registryRow, err := encryption.DecodeRegistryValue(rawVal)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: decode writer-registry row for full_node_id=%#x dek_id=%d",
			fullNodeID, activeStorageDEKID)
	}
	if registryRow.FullNodeID != fullNodeID {
		return foreignRegistryRowBeforeBump(registryRow.FullNodeID, fullNodeID, activeStorageDEKID)
	}
	if sidecarLocalEpoch < registryRow.LastSeenLocalEpoch {
		return pkgerrors.Wrapf(encryption.ErrLocalEpochRollback,
			"sidecar local_epoch=%d < registry last_seen_local_epoch=%d (full_node_id=%#x dek_id=%d) before startup bump; sidecar must be at least registry before BumpLocalEpoch advances it",
			sidecarLocalEpoch, registryRow.LastSeenLocalEpoch, fullNodeID, activeStorageDEKID)
	}
	return nil
}

func checkLocalEpochRollbackAfterReplay(
	raftID string,
	runtimes []*raftGroupRuntime,
	defaultGroup uint64,
	sidecarPath string,
	encryptionEnabled bool,
) error {
	if !encryptionEnabled || sidecarPath == "" {
		return nil
	}
	sc, key, ok, err := activeStorageKeyForStartupGuard(sidecarPath)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	reg, err := writerRegistryAfterReplay(runtimes, defaultGroup)
	if err != nil {
		return err
	}
	fullNodeID := etcdraftengine.DeriveNodeID(raftID)
	if err := checkLocalEpochRollbackAfterBump(
		reg,
		fullNodeID,
		sc.Active.Storage,
		key.LocalEpoch,
		sc.StorageEnvelopeActive,
	); err != nil {
		return pkgerrors.Wrap(err, "encryption startup guard: local_epoch rollback after raft replay")
	}
	return nil
}

func activeStorageKeyForStartupGuard(sidecarPath string) (*encryption.Sidecar, encryption.SidecarKey, bool, error) {
	sc, err := readExistingSidecarForStartupGuard(sidecarPath)
	if err != nil || sc == nil {
		return nil, encryption.SidecarKey{}, false, err
	}
	if sc.Active.Storage == 0 {
		return sc, encryption.SidecarKey{}, false, nil
	}
	key, ok := sc.Keys[strconv.FormatUint(uint64(sc.Active.Storage), 10)]
	if !ok {
		return sc, encryption.SidecarKey{}, false, pkgerrors.Wrapf(encryption.ErrSidecarActiveKeyMissing,
			"encryption startup guard: active storage DEK %d missing from sidecar keys after raft replay",
			sc.Active.Storage)
	}
	return sc, key, true, nil
}

func writerRegistryAfterReplay(runtimes []*raftGroupRuntime, defaultGroup uint64) (encryption.WriterRegistryStore, error) {
	defaultRuntime := findDefaultGroupRuntime(runtimes, defaultGroup)
	if defaultRuntime == nil || defaultRuntime.store == nil {
		return nil, pkgerrors.Errorf("encryption startup guard: default group %d runtime unavailable after raft replay", defaultGroup)
	}
	reg, err := store.WriterRegistryFor(defaultRuntime.store)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "encryption startup guard: writer registry after raft replay")
	}
	return reg, nil
}

func checkLocalEpochRollbackAfterBump(
	registry encryption.WriterRegistryStore,
	fullNodeID uint64,
	activeStorageDEKID uint32,
	sidecarLocalEpoch uint16,
	storageEnvelopeActive bool,
) error {
	key := encryption.RegistryKey(activeStorageDEKID, encryption.NodeID16(fullNodeID))
	rawVal, ok, err := registry.GetRegistryRow(key)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: read writer-registry row for full_node_id=%#x dek_id=%d after startup bump",
			fullNodeID, activeStorageDEKID)
	}
	if !ok {
		return missingRegistryRowBeforeBump(storageEnvelopeActive, fullNodeID, activeStorageDEKID)
	}
	registryRow, err := encryption.DecodeRegistryValue(rawVal)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: decode writer-registry row for full_node_id=%#x dek_id=%d after startup bump",
			fullNodeID, activeStorageDEKID)
	}
	if registryRow.FullNodeID != fullNodeID {
		return foreignRegistryRowBeforeBump(registryRow.FullNodeID, fullNodeID, activeStorageDEKID)
	}
	if sidecarLocalEpoch <= registryRow.LastSeenLocalEpoch {
		return pkgerrors.Wrapf(encryption.ErrLocalEpochRollback,
			"sidecar local_epoch=%d <= registry last_seen_local_epoch=%d (full_node_id=%#x dek_id=%d) after raft replay; startup bump must advance beyond every replayed registration",
			sidecarLocalEpoch, registryRow.LastSeenLocalEpoch, fullNodeID, activeStorageDEKID)
	}
	return nil
}

func missingRegistryRowBeforeBump(storageEnvelopeActive bool, fullNodeID uint64, activeStorageDEKID uint32) error {
	if storageEnvelopeActive {
		return pkgerrors.Wrapf(encryption.ErrLocalEpochRollback,
			"writer-registry has no row for full_node_id=%#x dek_id=%d but storage_envelope_active=true; cannot prove nonce monotonicity for the active DEK before startup bump",
			fullNodeID, activeStorageDEKID)
	}
	return nil
}

func foreignRegistryRowBeforeBump(registryFullNodeID, fullNodeID uint64, activeStorageDEKID uint32) error {
	return pkgerrors.Wrapf(encryption.ErrLocalEpochRollback,
		"writer-registry row for node_id16=%#x dek_id=%d belongs to full_node_id=%#x, not this full_node_id=%#x; refusing startup to avoid nonce-prefix collision",
		encryption.NodeID16(fullNodeID), activeStorageDEKID, registryFullNodeID, fullNodeID)
}

type missingWriterRegistry struct{}

func (missingWriterRegistry) GetRegistryRow([]byte) ([]byte, bool, error) { return nil, false, nil }

func (missingWriterRegistry) SetRegistryRow(_, _ []byte) error {
	return pkgerrors.New("missing writer registry is read-only")
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
