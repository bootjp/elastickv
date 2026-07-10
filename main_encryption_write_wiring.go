package main

import (
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	pkgerrors "github.com/cockroachdb/errors"
)

// buildShardGroupsWithEncryptionWiring assembles the storage-envelope
// write wiring from the process flags and then constructs the shard
// groups with it. The Stage 6C-3 membership guards run here before
// buildEncryptionWriteWiring can bump sidecar local_epoch and before
// buildShardGroups can replay committed Raft entries into encrypted
// stores. The helper keeps run() to a single startup-fault branch.
func buildShardGroupsWithEncryptionWiring(
	raftID string,
	raftDir string,
	groups []groupSpec,
	defaultGroup uint64,
	multi bool,
	bootstrap bool,
	bootstrapCfg raftBootstrapConfig,
	factory raftengine.Factory,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	clock *kv.HLC,
	kekWrapper kek.Wrapper,
	keystore *encryption.Keystore,
	sidecarPath string,
	encryptionEnabled bool,
	routeEngine *distribution.Engine,
	applyObserver kv.ApplyObserver,
) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, encryptionWriteWiring, error) {
	if guardErr := checkEncryptionMembershipStartupGuardsBeforeEngine(encryptionMembershipStartupGuardInput{
		raftID:            raftID,
		raftDir:           raftDir,
		groups:            groups,
		multi:             multi,
		defaultGroup:      defaultGroup,
		sidecarPath:       sidecarPath,
		encryptionEnabled: encryptionEnabled,
		bootstrapServers:  bootstrapCfg.adminSeed(defaultGroup),
	}); guardErr != nil {
		return nil, nil, encryptionWriteWiring{}, guardErr
	}
	encWiring, err := buildEncryptionWriteWiring(encryptionEnabled, raftID, sidecarPath, kekWrapper, keystore, groups)
	if err != nil {
		return nil, nil, encryptionWriteWiring{}, err
	}
	configureRaftEnvelopeFactory(factory, encWiring)
	runtimes, shardGroups, err := buildShardGroups(raftID, raftDir, groups, multi, bootstrap, bootstrapCfg,
		factory, proposalObserverForGroup, clock, kekWrapper, keystore, sidecarPath, encWiring, routeEngine, applyObserver)
	if err != nil {
		return runtimes, shardGroups, encWiring, err
	}
	if guardErr := checkLocalEpochRollbackAfterReplay(raftID, runtimes, defaultGroup, sidecarPath, encryptionEnabled); guardErr != nil {
		for _, rt := range runtimes {
			if rt != nil {
				rt.Close()
			}
		}
		return nil, nil, encryptionWriteWiring{}, guardErr
	}
	// Return the wiring (cache + bumped epoch) so run() can drive the
	// Stage 7a process-start registration after the shard stores open.
	return runtimes, shardGroups, encWiring, nil
}

type raftEnvelopeConfigurableFactory interface {
	SetRaftEnvelope(*encryption.Cipher, etcdraftengine.RaftCutoverIndex)
}

func configureRaftEnvelopeFactory(factory raftengine.Factory, w encryptionWriteWiring) {
	if factory == nil || w.raftEnvelope == nil {
		return
	}
	configurable, ok := factory.(raftEnvelopeConfigurableFactory)
	if !ok {
		return
	}
	configurable.SetRaftEnvelope(w.cipher, w.raftEnvelope.engineCutoverIndex)
}

func validateRaftEnvelopeStartupScope(cutoverIdx uint64, groups []groupSpec) error {
	if cutoverIdx == 0 {
		return nil
	}
	if len(groups) == 1 {
		return nil
	}
	return pkgerrors.Errorf(
		"encryption: active raft envelope sidecar requires exactly one shard group until per-group raft cutover indexes are implemented (got %d)",
		len(groups))
}

func encryptionApplierOptionsFor(kekWrapper encryption.KEKUnwrapper, keystore *encryption.Keystore, sidecarPath string, w encryptionWriteWiring) []encryption.ApplierOption {
	opts := append(applierOptionsFor(kekWrapper, keystore, sidecarPath),
		encryption.WithStateCache(w.cache),
		encryption.WithLocalEpoch(w.epoch),
		encryption.WithRaftLocalEpoch(w.raftEpoch))
	if w.raftEnvelope != nil {
		opts = append(opts, encryption.WithRaftCutoverWrapInstaller(w.raftEnvelope.installFromApply))
	}
	return opts
}

func (w encryptionWriteWiring) attachRaftEnvelopeGroup(groupID uint64, sg *kv.ShardGroup) {
	if w.raftEnvelope == nil {
		return
	}
	w.raftEnvelope.attachGroup(groupID, sg)
}

// encryptionWriteWiring bundles the Stage 6D-6c storage-envelope
// write-path dependencies threaded into buildShardGroups:
//
//   - cache is the process-shared StateCache (always non-nil). Every
//     per-shard Applier receives it via WithStateCache, and the
//     per-Put gate closures (cache.ActiveStorageKeyID /
//     cache.StorageEnvelopeActive) read from it, so an encryption
//     apply landing on one shard's FSM is visible to every shard's
//     storage layer (6D-6c-1's shared-cache invariant).
//   - cipher / nonceFactory are nil when encryption is not enabled
//     (no --encryption-enabled, no --kekFile, or no sidecar path). A
//     nil cipher leaves WithEncryption unwired, so the store stays in
//     legacy cleartext mode. When non-nil they are wired into every
//     shard's PebbleStore; the gate still keeps writes cleartext
//     until a Bootstrap + EnableStorageEnvelope has flipped the cache.
type encryptionWriteWiring struct {
	cache        *encryption.StateCache
	cipher       *encryption.Cipher
	nonceFactory store.NonceFactory
	// epoch is the §4.1 local_epoch this process load pinned into the
	// nonce factory (the value BumpLocalEpoch advanced to, or 0 in the
	// pre-bootstrap case). Stage 7a's process-start registration
	// proposes RegisterEncryptionWriter with this epoch so the writer
	// registry's last_seen advances in lockstep with the nonces this
	// load emits. Zero when encryption is off or pre-bootstrap.
	epoch uint16
	// raftEpoch is the §4.2 local_epoch this process load pins into
	// the raft-envelope nonce factory. It is independent from epoch
	// because storage and raft envelopes use separate DEKs and
	// write_count streams.
	raftEpoch uint16
	// raftNonceFactory emits §4.2 raft-envelope nonces. nil means
	// raft envelope wrapping is not configured for this process.
	raftNonceFactory store.NonceFactory
	// raftCutoverIndex is the process-shared source for the etcd
	// pre-apply unwrap hook. A zero value means inactive; the
	// callback exposed to etcd maps it to the inert max-uint sentinel.
	raftCutoverIndex *atomic.Uint64
	// raftEnvelope coordinates wrap publication across shard groups
	// and supplies the applier installer hook.
	raftEnvelope *raftEnvelopeRuntime
	// raftRegistration gates raft-envelope wrapping until this load has
	// committed a writer-registry row for (active raft DEK, raftEpoch).
	raftRegistration *raftRegistrationGate
}

// withDefaultedCache returns a copy of w with a non-nil StateCache.
// Zero-value wirings (the encryption-off test harnesses pass
// encryptionWriteWiring{}) carry a nil cache; defaulting it here keeps
// the "cache is always non-nil" contract that buildShardGroups'
// WithStateCache wiring depends on, without adding a branch to
// buildShardGroups itself.
func (w encryptionWriteWiring) withDefaultedCache() encryptionWriteWiring {
	if w.cache == nil {
		w.cache = encryption.NewStateCache()
	}
	return w
}

// pebbleOptions returns the PebbleStore options for the storage
// envelope. When the cipher is nil (encryption not enabled) it
// returns no options, leaving the store in cleartext mode. When set,
// it wires WithEncryption (cipher + nonce factory + active-key gate)
// and WithStorageEnvelopeGate (the §7.1 cutover gate); both read the
// shared cache so runtime Bootstrap / cutover applies take effect
// without a restart.
func (w encryptionWriteWiring) pebbleOptions() []store.PebbleStoreOption {
	if w.cipher == nil || w.nonceFactory == nil {
		return nil
	}
	return []store.PebbleStoreOption{
		store.WithEncryption(w.cipher, w.nonceFactory, w.cache.ActiveStorageKeyID),
		store.WithStorageEnvelopeGate(w.cache.StorageEnvelopeActive),
		// Stage 7a-2 §4.1 direct-path registration gate: the store
		// refuses to emit an encrypted envelope on a self-originated
		// write (catalog bootstrap Save) until this load's writer
		// registration has committed for the active storage DEK. Reads
		// Registered() (per-DEK, fail-closed) from the same shared cache
		// the registration paths MarkRegistered. Design §2.3 forbids any
		// fail-OPEN fallback, so unregistered loads are gated — see the
		// Registered() doc for the deferred runtime-registration cases.
		store.WithStorageRegistrationGate(w.cache.Registered),
	}
}

// buildEncryptionWriteWiring assembles the storage-envelope write-path
// wiring. It always returns a wiring with a non-nil StateCache (the
// per-shard appliers need it regardless of encryption state); the
// cipher + nonce factory are populated only when encryption is enabled
// (encryptionEnabled AND --kekFile AND a non-empty sidecarPath).
//
// Inputs are explicit parameters rather than the package-level flag
// globals so the nonce factory's node_id is provably derived from the
// same raftID the shard stores use, and so the helper is testable in
// isolation. run()'s orchestrator buildShardGroupsWithEncryptionWiring
// threads its own raftID / sidecarPath params plus *encryptionEnabled in.
//
// When a storage DEK is already active on disk (the restart path),
// the function hydrates the keystore from the sidecar so the cipher
// can decrypt pre-existing envelopes whose bootstrap entry may have
// been compacted, and bumps the §4.1 local_epoch so the per-load
// write_count reset stays nonce-safe. The raft DEK's local_epoch is bumped
// only after raft-envelope cutover is active; before cutover no raft-envelope
// nonces can be emitted, so consuming the finite epoch space gives no safety
// benefit. On a pre-bootstrap binary the epoch defaults to 0 — the value a
// future runtime Bootstrap assigns to the freshly minted DEK, which is that
// DEK's first-ever use and therefore nonce-safe; a later restart will then
// take the bump path.
func buildEncryptionWriteWiring(encryptionEnabled bool, raftID, sidecarPath string, kekWrapper encryption.KEKUnwrapper, keystore *encryption.Keystore, groups []groupSpec) (encryptionWriteWiring, error) {
	w := encryptionWriteWiring{cache: encryption.NewStateCache()}
	if !encryptionEnabled || kekWrapper == nil || sidecarPath == "" {
		return w, nil
	}
	// On error return w (with its non-nil cache) rather than a
	// zero-value wiring: the caller aborts startup on the error and
	// never consults the wiring, but keeping the cache non-nil on
	// every return path avoids a latent nil-cache footgun for any
	// future caller that reads w before checking err.
	cipher, err := encryption.NewCipher(keystore)
	if err != nil {
		return w, pkgerrors.Wrap(err, "build encryption write wiring: new cipher")
	}
	cutoverIdx, activeRaftDEKID, err := readRaftEnvelopeStartupState(sidecarPath)
	if err != nil {
		return w, err
	}
	if err := validateRaftEnvelopeStartupState(cutoverIdx, activeRaftDEKID, groups); err != nil {
		return w, err
	}
	epoch, err := prepareStorageNonceEpoch(sidecarPath, kekWrapper, keystore, w.cache)
	if err != nil {
		return w, err
	}
	raftEpoch, err := prepareRaftNonceEpoch(sidecarPath, kekWrapper, keystore, cutoverIdx)
	if err != nil {
		return w, err
	}
	fullNodeID := etcdraftengine.DeriveNodeID(raftID)
	nodeID := encryption.NodeID16(fullNodeID)
	w.cipher = cipher
	w.epoch = epoch
	w.raftEpoch = raftEpoch
	w.nonceFactory = encryption.NewDeterministicNonceFactory(nodeID, epoch)
	w.raftNonceFactory = encryption.NewDeterministicNonceFactory(nodeID, raftEpoch)
	w.raftCutoverIndex = &atomic.Uint64{}
	w.raftRegistration = &raftRegistrationGate{}
	runtime, err := newRaftEnvelopeRuntime(cipher, w.raftNonceFactory, cutoverIdx, activeRaftDEKID, w.raftCutoverIndex, w.raftEpoch, fullNodeID, w.raftRegistration)
	if err != nil {
		return w, err
	}
	w.raftEnvelope = runtime
	return w, nil
}

func readRaftEnvelopeStartupState(sidecarPath string) (cutoverIdx uint64, activeRaftDEKID uint32, err error) {
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		if encryption.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, pkgerrors.Wrap(err, "read raft envelope startup state")
	}
	return sc.RaftEnvelopeCutoverIndex, sc.Active.Raft, nil
}

func validateRaftEnvelopeStartupState(cutoverIdx uint64, activeRaftDEKID uint32, groups []groupSpec) error {
	if cutoverIdx == 0 {
		return nil
	}
	if activeRaftDEKID == 0 {
		return pkgerrors.New("encryption: active raft envelope sidecar requires active raft DEK")
	}
	return validateRaftEnvelopeStartupScope(cutoverIdx, groups)
}

func prepareRaftNonceEpoch(sidecarPath string, kekWrapper encryption.KEKUnwrapper, keystore *encryption.Keystore, cutoverIdx uint64) (uint16, error) {
	if cutoverIdx == 0 {
		return 0, nil
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		if encryption.IsNotExist(err) {
			return 0, nil
		}
		return 0, pkgerrors.Wrap(err, "prepare raft nonce epoch: read sidecar")
	}
	if sc.Active.Raft == 0 {
		return 0, pkgerrors.New("prepare raft nonce epoch: active cutover requires active raft DEK")
	}
	if err := encryption.HydrateKeystoreFromSidecar(keystore, kekWrapper, sc); err != nil {
		return 0, pkgerrors.Wrap(err, "prepare raft nonce epoch: hydrate keystore")
	}
	epoch, err := encryption.BumpLocalEpoch(sidecarPath, sc.Active.Raft)
	if err != nil {
		if errors.Is(err, encryption.ErrLocalEpochExhausted) {
			slog.Error("encryption write-path wiring refused: raft DEK local_epoch exhausted; rotate-dek required",
				slog.String("sidecar_path", sidecarPath),
				slog.Uint64("active_raft_dek", uint64(sc.Active.Raft)))
		}
		return 0, pkgerrors.Wrap(err, "prepare raft nonce epoch: bump local_epoch")
	}
	return epoch, nil
}

// prepareStorageNonceEpoch returns the §4.1 local_epoch the nonce
// factory should pin for this process load. It primes the shared
// StateCache from the sidecar so the storage gate is correct before
// the first store opens, and — when a storage DEK is active —
// hydrates the keystore and performs the durable local_epoch bump.
//
// Returns epoch 0 in the two pre-bootstrap cases (no sidecar file, or
// a sidecar with Active.Storage == 0): the cluster has no storage DEK
// yet, so there is no epoch to bump and no envelope will be emitted
// until a runtime Bootstrap installs one (with local_epoch 0, its
// first use).
func prepareStorageNonceEpoch(sidecarPath string, kekWrapper encryption.KEKUnwrapper, keystore *encryption.Keystore, cache *encryption.StateCache) (uint16, error) {
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		if encryption.IsNotExist(err) {
			return 0, nil
		}
		return 0, pkgerrors.Wrap(err, "prepare storage nonce epoch: read sidecar")
	}
	cache.RefreshFromSidecar(sc)
	if sc.Active.Storage == 0 {
		return 0, nil
	}
	if err := encryption.HydrateKeystoreFromSidecar(keystore, kekWrapper, sc); err != nil {
		return 0, pkgerrors.Wrap(err, "prepare storage nonce epoch: hydrate keystore")
	}
	epoch, err := encryption.BumpLocalEpoch(sidecarPath, sc.Active.Storage)
	if err != nil {
		// Surface the §9.1 exhaustion refusal verbatim so the operator
		// log line names the rotate-dek recovery path.
		if errors.Is(err, encryption.ErrLocalEpochExhausted) {
			slog.Error("encryption write-path wiring refused: storage DEK local_epoch exhausted; rotate-dek required",
				slog.String("sidecar_path", sidecarPath),
				slog.Uint64("active_storage_dek", uint64(sc.Active.Storage)))
		}
		return 0, pkgerrors.Wrap(err, "prepare storage nonce epoch: bump local_epoch")
	}
	return epoch, nil
}
