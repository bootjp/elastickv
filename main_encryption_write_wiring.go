package main

import (
	"errors"
	"log/slog"

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
	bootstrapServers []raftengine.Server,
	factory raftengine.Factory,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	clock *kv.HLC,
	kekWrapper kek.Wrapper,
	keystore *encryption.Keystore,
	sidecarPath string,
	encryptionEnabled bool,
	routeEngine *distribution.Engine,
) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, encryptionWriteWiring, error) {
	// Stage 6E-2c: refuse startup BEFORE buildEncryptionWriteWiring
	// runs (which calls prepareStorageNonceEpoch → BumpLocalEpoch
	// → durable sidecar write). If we ran the refusal after the
	// epoch bump, an orchestrator restart loop on a sidecar with
	// non-zero RaftEnvelopeCutoverIndex would consume one
	// local_epoch per boot and could hit ErrLocalEpochExhausted
	// before the operator notices (codex P2 round-1 r3). Running
	// it first keeps the sidecar untouched on the refusal path.
	//
	// This binary does not (yet) ship the cipher-aware wrap
	// closure factory (that lands in Stage 6E-2e/2f). With a
	// cutover recorded but no wrap installed, every fresh
	// proposal would land cleartext above the cutover index and
	// halt the apply loop on §6.3 strict-`>` unwrap — a
	// node-wide crash loop is worse than refusing to serve.
	// Fail-closed so the operator sees the refusal at boot,
	// before any traffic is admitted and before any sidecar
	// mutation (codex P1 round-1).
	//
	// In a healthy 6E-2c-only deployment this never fires:
	// raftEnvelopeWrapEnabled is still false in
	// adapter/encryption_admin.go (see Stage 6E-1b), so
	// EnableRaftEnvelope refuses to record a cutover index. The
	// refusal fires only on a sidecar restored from a future
	// deployment where 6E-2f flipped the gate, or hand-edited —
	// both cases the operator must intervene before serving
	// traffic.
	if cutoverErr := refuseStartupOnActiveRaftCutover(sidecarPath); cutoverErr != nil {
		return nil, nil, encryptionWriteWiring{}, cutoverErr
	}
	if guardErr := checkEncryptionMembershipStartupGuardsBeforeEngine(encryptionMembershipStartupGuardInput{
		raftID:            raftID,
		raftDir:           raftDir,
		groups:            groups,
		multi:             multi,
		defaultGroup:      defaultGroup,
		sidecarPath:       sidecarPath,
		encryptionEnabled: encryptionEnabled,
		bootstrapServers:  bootstrapServers,
	}); guardErr != nil {
		return nil, nil, encryptionWriteWiring{}, guardErr
	}
	encWiring, err := buildEncryptionWriteWiring(encryptionEnabled, raftID, sidecarPath, kekWrapper, keystore)
	if err != nil {
		return nil, nil, encryptionWriteWiring{}, err
	}
	runtimes, shardGroups, err := buildShardGroups(raftID, raftDir, groups, multi, bootstrap, bootstrapServers,
		factory, proposalObserverForGroup, clock, kekWrapper, keystore, sidecarPath, encWiring, routeEngine)
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

// refuseStartupOnActiveRaftCutover reads the sidecar at process
// start and returns a fail-closed error if RaftEnvelopeCutoverIndex
// is non-zero. Stage 6E-2c installs the dynamic wrap proposer
// chain on every shard group but leaves the wrap closure pointer
// nil — the cipher-aware factory ships in Stage 6E-2e/2f. Without
// the closure, a post-cutover write proposed under this binary
// would land cleartext above the cutover; the strict-`>` apply
// hook would then halt on unwrap failure across the cluster.
//
// Missing or unreadable sidecars are not this helper's concern —
// the Stage 6C-1 startup guard already refuses to boot when the
// sidecar is malformed or the KEK does not unwrap the recorded
// DEKs. A clean ReadSidecar with a zero RaftEnvelopeCutoverIndex
// is the Stage 3 default and returns nil.
//
// The error message names the recovery path explicitly so the
// operator log line is enough to act on without grepping source.
func refuseStartupOnActiveRaftCutover(sidecarPath string) error {
	if sidecarPath == "" {
		return nil
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		// Missing sidecar is the pre-bootstrap state — there is no
		// raft envelope cutover to refuse on. Other read failures
		// (malformed / KEK-mismatched) propagate so the caller's
		// startup chain sees them; mirroring the prepareStorageNonceEpoch
		// shape that lets the Stage 6C-1 startup guard surface the
		// same error through a different code path keeps the
		// failure mode consistent.
		if encryption.IsNotExist(err) {
			return nil
		}
		return pkgerrors.Wrap(err, "encryption: refuse startup on active raft cutover (read sidecar)")
	}
	if sc.RaftEnvelopeCutoverIndex == 0 {
		return nil
	}
	slog.Error("encryption: refusing to start — sidecar reports raft envelope cutover active but this binary does not install the wrap closure on shard groups; Stage 6E-2e/2f wires the cipher. Restore a sidecar with RaftEnvelopeCutoverIndex=0 (cutover not yet installed) or upgrade to a binary that ships the cipher factory.",
		slog.Uint64("cutover_index", sc.RaftEnvelopeCutoverIndex),
		slog.String("sidecar_path", sidecarPath))
	return pkgerrors.Errorf(
		"encryption: sidecar reports raft envelope cutover active at index %d but this binary does not install the wrap closure on shard groups; Stage 6E-2e/2f wires the cipher — restore a sidecar with RaftEnvelopeCutoverIndex=0 or upgrade",
		sc.RaftEnvelopeCutoverIndex)
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
// isolation (codex P2 on PR #826). run()'s orchestrator
// buildShardGroupsWithEncryptionWiring threads its own raftID /
// sidecarPath params plus *encryptionEnabled in.
//
// When a storage DEK is already active on disk (the restart path),
// the function hydrates the keystore from the sidecar so the cipher
// can decrypt pre-existing envelopes whose bootstrap entry may have
// been compacted, and bumps the §4.1 local_epoch so the per-load
// write_count reset stays nonce-safe. On a pre-bootstrap binary the
// epoch defaults to 0 — the value a future runtime Bootstrap assigns
// to the freshly minted DEK, which is that DEK's first-ever use and
// therefore nonce-safe; a later restart will then take the bump path.
func buildEncryptionWriteWiring(encryptionEnabled bool, raftID, sidecarPath string, kekWrapper encryption.KEKUnwrapper, keystore *encryption.Keystore) (encryptionWriteWiring, error) {
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
	epoch, err := prepareStorageNonceEpoch(sidecarPath, kekWrapper, keystore, w.cache)
	if err != nil {
		return w, err
	}
	w.cipher = cipher
	w.epoch = epoch
	w.nonceFactory = encryption.NewDeterministicNonceFactory(
		encryption.NodeID16(etcdraftengine.DeriveNodeID(raftID)), epoch)
	return w, nil
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
