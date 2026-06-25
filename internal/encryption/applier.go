package encryption

import (
	"bytes"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/cockroachdb/errors"
)

// WriterRegistryStore is the storage abstraction the Applier needs
// to read and write §4.1 writer-registry rows under the
// `!encryption|writers|<dek_id>|<uint16(node_id)>` Pebble prefix.
//
// The interface stays separate from store.MVCCStore because writer-
// registry rows live OUTSIDE the MVCC namespace — they are
// metadata about the cluster's encryption state, not user data.
// They carry no commit timestamps, no MVCC visibility, and no
// retention semantics; the Pebble row is the durable form and is
// replayed on FSM apply just like any other 0x03 entry.
//
// Implementations MUST make Set durable before returning (or the
// next FSM apply will see a stale read on Get and §4.1 case 2's
// monotonic-epoch check will incorrectly accept a rolled-back
// local_epoch). The main.go wiring satisfies this by routing
// through the FSM's `pebble.Sync` Set path; the in-memory
// MapWriterRegistryStore used in tests trivially satisfies it.
type WriterRegistryStore interface {
	// GetRegistryRow returns the value at the supplied registry
	// key. The boolean is true iff the row exists; on a missing
	// row both `value` and the boolean are zero. The error path
	// is reserved for storage faults (Pebble error, etc.); a
	// missing row is NOT an error.
	GetRegistryRow(key []byte) (value []byte, ok bool, err error)

	// SetRegistryRow writes a registry row durably. Overwrites
	// any existing value at the same key. Idempotent at the
	// (key, value) tuple level — writing the same value twice
	// has no observable effect.
	SetRegistryRow(key []byte, value []byte) error
}

// Applier is the §6.3 EncryptionApplier concrete implementation.
// It satisfies kv.EncryptionApplier and is wired at FSM
// construction via kv.NewKvFSMWithHLC(..., kv.WithEncryption(applier)).
//
// Stage 6A ships:
//
//   - ApplyRegistration — the §4.1 case 1 / case 2 dispatch.
//     For (dek_id, uint16(full_node_id)) keying:
//
//     case 1: no existing row → insert.
//     case 2 (strictly greater epoch, same full_node_id) → advance LastSeen.
//     case 2-idempotent (equal epoch, same full_node_id) → no-op (legit
//     Raft replay; rejecting it would halt-on-replay-loop after crash).
//     case 3 (strictly smaller epoch, same full_node_id) → halt apply
//     (rollback; recovered via §9.1).
//     case 4 (different full_node_id at same uint16 truncation) →
//     halt apply (§6.1 uniqueness invariant).
//
//   - ApplyBootstrap and ApplyRotation — return the defense-in-depth
//     ErrKEKNotConfigured marker. Stage 6B will swap these for the
//     real KEK-unwrap + sidecar mutate + keystore install path.
//
// Apart from the shared StateCache pointer (see below), the
// Applier carries no in-memory state of its own; durable state
// lives in the supplied WriterRegistryStore and the on-disk
// sidecar. The StateCache mirrors a small subset of sidecar
// fields the storage hot path consults on every Put — kept
// coherent by durable write-then-cache ordering inside each
// apply path.
type Applier struct {
	registry    WriterRegistryStore
	kek         KEKUnwrapper
	keystore    *Keystore
	sidecarPath string
	now         func() time.Time
	// stateCache is the process-shared mirror of the sidecar fields
	// the storage hot path consults on every Put. See StateCache for
	// the full contract; in short, a single instance is owned by
	// main.go (parallel to the shared *Keystore) and threaded into
	// every per-shard Applier via WithStateCache so that an apply
	// landing on shard A's FSM is immediately visible to shard B's
	// storage layer. Multi-group encryption applies always land on
	// exactly one shard's FSM (the one whose engine accepted the
	// proposal), so a per-Applier cache would leave the remaining
	// shards stuck with pre-apply atomic values.
	//
	// Never nil after NewApplier: when WithStateCache is omitted the
	// constructor installs a private instance so single-applier
	// callers and tests keep working unchanged.
	stateCache *StateCache
	// localEpoch is the §4.1 storage write-path local_epoch this
	// process load pinned its nonce factory to at startup (the value
	// `encryptionWriteWiring.epoch` holds). It is read ONLY by the
	// Stage 7b' rotation-case applyRotateDEK path when writing
	// `Keys[newDEK].LocalEpoch` for storage rotations — recording the
	// LOCAL node's highest-emitted local_epoch under the new DEK so the
	// §9.1 startup guard on next restart sees a monotone advance rather
	// than the cluster-wide brick scenario described in 7b' §1. Raft
	// rotations (PurposeRaft) continue to write `LocalEpoch: 0` (until
	// raft envelope support lands with its own per-purpose epoch
	// plumbing) per 7b' §3.1.1.
	//
	// Zero (the default when WithLocalEpoch is omitted) is correct for:
	// (a) the pre-bootstrap process load — no writes have been emitted
	//     under any DEK yet, so the sidecar's `0` accurately records this
	//     node's highest-emitted local_epoch (7b' §3.1.2);
	// (b) FSM-internal test harnesses that construct an Applier without
	//     write-path state — they have no nonce factory and accept the
	//     zero value as the "preserve today's behavior" fallback.
	localEpoch uint16
	// raftCutoverWrapInstaller is the Stage 6E-2e-1 hook that
	// applyEnableRaftEnvelope invokes on every replica's local FSM
	// apply of the cutover marker. Production wiring (6E-2e-3:
	// main.go) supplies a closure that publishes the wrap closure to
	// every participating kv.ShardGroup via SetRaftPayloadWrap, so a
	// follower that becomes leader post-cutover already has wrap
	// active without needing the EnableRaftEnvelope handler to
	// re-run (closes the BLOCKER (b) leader-failover hazard from
	// codex P1 round-3 on PR933).
	//
	// Called from the fresh-success branch AND the already-active
	// branch (FSM replay safety: a snapshot that excludes the
	// cutover entry replays it on restart; idempotent install is
	// expected). NOT called from the stale-DEKID benign-no-op
	// branch — that branch leaves RaftEnvelopeCutoverIndex at 0 and
	// the cutover has not taken effect.
	//
	// Errors from the installer halt apply: the sidecar already
	// records the cutover but the in-process wrap is missing, so a
	// subsequent USER proposal on this node would land cleartext at
	// `index > cutoverIdx` and brick the §6.3 strict-`>` unwrap
	// hook cluster-wide. Halting forces the operator to investigate
	// (typically a misconfigured cipher) before any further apply
	// runs.
	//
	// nil disables the hook — preserves the pre-6E-2e-1 test
	// surface (no behavior change for callers that don't opt in).
	raftCutoverWrapInstaller RaftCutoverWrapInstaller
}

// RaftCutoverWrapInstaller is the Stage 6E-2e-1 callback the Applier
// invokes on every replica's local FSM apply of the
// EnableRaftEnvelope cutover marker to publish the §4.2 raft envelope
// wrap closure on this node.
//
// Contract:
//   - cutoverIdx is the Raft index recorded in the sidecar
//     (sc.RaftEnvelopeCutoverIndex). Fresh-success apply passes the
//     just-stamped value; already-active apply passes the previously-
//     recorded value (idempotent re-install).
//   - activeRaftDEKID is the sidecar.Active.Raft value at apply time.
//     The installer constructs the wrap closure using this DEK so the
//     §6.3 strict-`>` apply hook on every replica unwraps with the
//     same key.
//   - Returns nil on success or an error that halts apply (see the
//     raftCutoverWrapInstaller field comment for the rationale).
//
// The installer MUST be idempotent: replayed FSM apply, snapshot
// restore, and the explicit EnableRaftEnvelope handler's InstallWrap
// call all converge on the same wrap closure publication.
type RaftCutoverWrapInstaller func(cutoverIdx uint64, activeRaftDEKID uint32) error

// StateCache mirrors the sidecar fields the storage hot path needs
// to consult on every Put. Two requirements drive its existence:
//
//  1. ReadSidecar-on-every-Put would serialise the hot path through
//     a JSON parse + fsync barrier. atomic.Uint32 / atomic.Bool give
//     a wait-free single-load read instead.
//
//  2. In a multi-group deployment, encryption FSM entries apply on
//     whichever shard's leader accepted the proposal — not on every
//     shard. The per-shard storage layers must still observe the
//     updated state, so the cache MUST be a process-shared singleton
//     rather than a per-Applier field. main.go constructs one
//     StateCache at startup (parallel to the shared *Keystore) and
//     threads it into every per-shard Applier via WithStateCache.
//
// Coherence with disk is maintained by **durable write-then-cache**
// ordering: NewApplier primes the cache from ReadSidecar, and every
// apply path calls RefreshFromSidecar AFTER WriteSidecar succeeds.
// A crash between fsync and atomic store is benign because the next
// process start re-primes from disk.
//
// Zero values match the pre-bootstrap posture (no active storage
// DEK, envelope gate off) so a freshly-constructed StateCache is
// safe to use before any apply or prime has run.
type StateCache struct {
	// activeStorageDEKID mirrors sidecar.Active.Storage. Zero means
	// "not bootstrapped"; readers surface (0, false) and the storage
	// layer writes cleartext.
	activeStorageDEKID atomic.Uint32
	// storageEnvelopeActive mirrors sidecar.StorageEnvelopeActive
	// for the §6.2 cutover gate. Lifecycle:
	//   - false at construction (or primed from disk if a previous
	//     cutover already fired).
	//   - flipped to true exactly once by applyEnableStorageEnvelope
	//     on a fresh-success apply, AFTER WriteSidecar succeeds.
	//   - never flipped back to false (the cutover is one-way per
	//     §7.1 Phase 1; rotate-dek under the active envelope keeps
	//     it true).
	storageEnvelopeActive atomic.Bool
	// registeredStorageDEKID is the §4.1 writer-registry DEK id this
	// process has confirmed its own registration for (0 = none). It is
	// NOT mirrored from the sidecar — it tracks a per-process-load
	// fact (this load's writer registration committed) rather than
	// durable cluster state. Stage 7a-2's direct-write gate consults
	// it via Registered(): a self-originated encrypted write is
	// refused until this load's registration is confirmed for the
	// currently-active storage DEK. Set by MarkRegistered from 7a's
	// registration paths (barrier-close and the already-registered
	// startup branch). A single uint32 suffices because exactly one
	// storage DEK is active at a time; 7b's rotate-dek re-points
	// activeStorageDEKID and re-registers, which Registered()'s
	// equality check handles without a reset.
	registeredStorageDEKID atomic.Uint32
}

// NewStateCache returns a zero-initialised StateCache. The
// pre-bootstrap posture (Active.Storage=0, StorageEnvelopeActive=false)
// is the correct initial state; RefreshFromSidecar advances it to the
// current sidecar values when one is supplied.
func NewStateCache() *StateCache { return &StateCache{} }

// RefreshFromSidecar copies the relevant fields out of sc into the
// atomic mirrors. Safe to call concurrently with reads; safe to
// call from multiple goroutines (writers race to the same atomic
// CAS path, but the only writer in production is the FSM apply
// goroutine of the shard that accepted the encryption proposal).
//
// nil sc is a no-op: matches the pre-bootstrap posture where
// ReadSidecar returns IsNotExist.
func (c *StateCache) RefreshFromSidecar(sc *Sidecar) {
	if c == nil || sc == nil {
		return
	}
	c.activeStorageDEKID.Store(sc.Active.Storage)
	c.storageEnvelopeActive.Store(sc.StorageEnvelopeActive)
}

// ActiveStorageKeyID returns the current sidecar.Active.Storage DEK
// id. Signature matches store.ActiveStorageKeyID so main.go can pass
// `cache.ActiveStorageKeyID` directly into `store.WithEncryption(...)`
// as the per-Put activeKeyID closure. A non-zero id with ok=true
// means the cluster has run BootstrapEncryption; zero with ok=false
// means the cluster is still pre-bootstrap and the storage layer
// should write cleartext.
func (c *StateCache) ActiveStorageKeyID() (uint32, bool) {
	if c == nil {
		return 0, false
	}
	id := c.activeStorageDEKID.Load()
	return id, id != 0
}

// StorageEnvelopeActive returns the in-memory mirror of
// sidecar.StorageEnvelopeActive. Signature matches
// store.StorageEnvelopeActive so main.go can pass
// `cache.StorageEnvelopeActive` directly into
// `store.WithStorageEnvelopeGate(...)` as the per-Put cutover gate.
// Once true, the storage layer wraps every new version in the §4.1
// envelope; flips exactly once per cluster lifetime when the §7.1
// Phase 1 cutover entry applies.
func (c *StateCache) StorageEnvelopeActive() bool {
	if c == nil {
		return false
	}
	return c.storageEnvelopeActive.Load()
}

// MarkRegistered records that this process load's §4.1 writer
// registration has committed for storage DEK dekID. Stage 7a's
// registration paths call it once their barrier closes (or in the
// already-registered startup branch). Idempotent; a zero dekID is a
// no-op so a not-bootstrapped caller cannot accidentally mark the
// "no DEK" sentinel as registered.
func (c *StateCache) MarkRegistered(dekID uint32) {
	if c == nil || dekID == 0 {
		return
	}
	c.registeredStorageDEKID.Store(dekID)
}

// Registered reports whether this process load has confirmed its §4.1
// writer registration for the currently-active storage DEK. It is the
// predicate Stage 7a-2's WithStorageRegistrationGate consults on the
// direct write path: a self-originated encrypted write is refused
// (ErrWriterNotRegistered) until Registered() is true.
//
// It is deliberately per-DEK and fail-closed (design §2.3 forbids any
// fail-OPEN fallback): a node that has not registered for the active DEK
// — including a freshly rotated DEK it has not yet re-registered under
// (7b) — is gated. The runtime cases where a node legitimately needs to
// (re)register before its first encrypted direct write (Phase-0 boot then
// runtime EnableStorageEnvelope; non-proposer node after a runtime
// RotateDEK) are the deferred runtime-registration follow-on; until it
// lands they fail closed, which is the safe posture. No real runtime
// direct-write caller exists today — the only direct ApplyMutations path
// is the startup catalog bootstrap Save, which is covered by
// retryUntilRegistered + the already-registered MarkRegistered seed.
//
// Lock-free: two atomic loads. Returns false when there is no active
// storage DEK (id == 0) so a pre-bootstrap process never claims to be
// registered; once a DEK is active, returns true only when this load
// has marked that exact id (so 7b's rotate-dek to a new id re-arms
// the gate until the post-rotation registration marks the new id).
func (c *StateCache) Registered() bool {
	if c == nil {
		return false
	}
	id := c.activeStorageDEKID.Load()
	return id != 0 && c.registeredStorageDEKID.Load() == id
}

// KEKUnwrapper is the abstraction the Applier uses to recover
// cleartext DEK bytes from the wrapped DEK material carried in
// BootstrapPayload / RotationPayload. The supplied implementation
// is exercised on every 0x04 / 0x05 apply, so it MUST be safe for
// concurrent use across replays.
//
// kek.Wrapper from internal/encryption/kek satisfies this interface
// structurally — the Applier carries its own local interface
// declaration so the encryption package does not pick up a
// transitive dependency on the kek package's import graph (which
// in turn lets the kek package import from internal/encryption
// without a cycle when KMS providers land in Stage 9).
type KEKUnwrapper interface {
	Unwrap(wrapped []byte) ([]byte, error)
}

// ApplierOption configures an Applier at construction. Stage 6A
// shipped with only the WriterRegistryStore required; Stage 6B
// adds the KEK / Keystore / SidecarPath plumbing that unlocks
// real ApplyBootstrap / ApplyRotation. The functional-option
// shape keeps the Stage 6A test surface (NewApplier(reg)) working
// byte-for-byte while letting production main.go layer in the
// new dependencies.
type ApplierOption func(*Applier)

// WithKEK wires the KEKUnwrapper used by ApplyBootstrap /
// ApplyRotation. Passing nil leaves the Applier in the Stage 6A
// posture where both paths short-circuit with ErrKEKNotConfigured.
func WithKEK(unwrap KEKUnwrapper) ApplierOption {
	return func(a *Applier) { a.kek = unwrap }
}

// WithKeystore wires the in-memory keystore the Applier mutates
// on Bootstrap / Rotation. The keystore lifetime spans the
// process — main.go passes the same instance the storage cipher
// is reading from.
func WithKeystore(ks *Keystore) ApplierOption {
	return func(a *Applier) { a.keystore = ks }
}

// WithSidecarPath wires the §5.1 keys.json path the Applier
// crash-durably mutates on Bootstrap / Rotation. Empty path
// disables sidecar mutation (Stage 6A posture).
func WithSidecarPath(path string) ApplierOption {
	return func(a *Applier) { a.sidecarPath = path }
}

// WithNowFunc overrides the wall-clock used for the sidecar's
// Created field. Tests pin this to a deterministic clock; the
// production default is time.Now. The Created field is diagnostic
// only — different replicas timestamp independently and that is
// fine (§5.1 does not require byte-equal sidecars).
func WithNowFunc(now func() time.Time) ApplierOption {
	return func(a *Applier) { a.now = now }
}

// WithStateCache installs a shared StateCache so that an apply
// landing on this Applier (typically the per-shard Applier whose
// FSM accepted the encryption proposal) updates atomics that every
// other Applier in the process reads. main.go owns one StateCache
// for the lifetime of the binary and threads the same pointer into
// every per-shard Applier and into the storage-layer per-Put
// closures.
//
// If WithStateCache is omitted, NewApplier installs a private
// instance — preserves the single-applier ergonomics that tests
// and pre-multi-shard callers rely on.
func WithStateCache(c *StateCache) ApplierOption {
	return func(a *Applier) { a.stateCache = c }
}

// WithLocalEpoch installs the §4.1 storage write-path `local_epoch`
// this process load pinned its nonce factory to. Threaded through
// from `encryptionWriteWiring.epoch` so Stage 7b' rotation applies
// can record THIS node's highest-emitted local_epoch under the new
// DEK (see the `localEpoch` field doc on Applier and 7b' §3.1).
//
// A static `uint16` rather than a `func() uint16` provider is
// deliberate (7b' §3.1): `BumpLocalEpoch` only runs at process start,
// so there is no runtime epoch-bump path that would require late
// binding. The option is omitted by FSM-internal test harnesses; the
// zero value preserves today's `LocalEpoch: 0` behaviour for them.
func WithLocalEpoch(epoch uint16) ApplierOption {
	return func(a *Applier) { a.localEpoch = epoch }
}

// WithRaftCutoverWrapInstaller installs the Stage 6E-2e-1 hook the
// Applier invokes from applyEnableRaftEnvelope to publish the wrap
// closure on this node. nil is a no-op (the option is omitted on the
// test surface and on the pre-6E-2e-1 production posture); a
// non-nil installer is invoked on both fresh-success and
// already-active apply paths but NOT on the stale-DEK no-op branch.
//
// See the RaftCutoverWrapInstaller type comment for the contract;
// production wiring lives in main.go (6E-2e-3).
func WithRaftCutoverWrapInstaller(installer RaftCutoverWrapInstaller) ApplierOption {
	return func(a *Applier) { a.raftCutoverWrapInstaller = installer }
}

// NewApplier wires an Applier against the supplied registry store
// plus optional KEK / Keystore / sidecar / clock dependencies.
// Returns an error if registry is nil so misconfiguration is caught
// at construction time rather than at first apply (the panic site
// is much harder to map back to a "you forgot to wire X" diagnosis
// when it fires deep inside a Raft apply loop).
//
// Without WithKEK / WithKeystore / WithSidecarPath, the Applier
// retains the Stage 6A behaviour — ApplyRegistration is fully
// functional, ApplyBootstrap and ApplyRotation return the typed
// ErrKEKNotConfigured marker. This is the test default and the
// pre-Stage-6B production posture.
func NewApplier(registry WriterRegistryStore, opts ...ApplierOption) (*Applier, error) {
	if registry == nil {
		return nil, errors.New("encryption: NewApplier: registry is nil")
	}
	a := &Applier{registry: registry, now: time.Now}
	for i, opt := range opts {
		if opt == nil {
			return nil, errors.Errorf("encryption: NewApplier: opts[%d] is nil", i)
		}
		opt(a)
	}
	// WithNowFunc(nil) would overwrite the default and later panic
	// at apply time when a.now() is invoked. Fail fast at
	// construction so the misconfiguration surfaces at startup
	// rather than deep inside a Raft apply loop.
	if a.now == nil {
		return nil, errors.New("encryption: NewApplier: WithNowFunc(nil) overwrote default time.Now")
	}
	// Install a private StateCache when WithStateCache was not
	// supplied so the apply paths and accessors always have a
	// non-nil target. Tests rely on this; production main.go is
	// expected to thread a shared instance in.
	if a.stateCache == nil {
		a.stateCache = NewStateCache()
	}
	// Prime the in-memory accessors from the on-disk sidecar
	// (best-effort: a missing sidecar is the pre-bootstrap
	// posture and leaves both atomics at their zero values,
	// which is correct). The storage-layer closures may query
	// these atomics before the FSM has replayed a single entry
	// after restart, so the priming must happen at construction
	// rather than lazily on first apply. A read error (corrupt
	// JSON, bad version) surfaces back to the caller so a
	// misconfigured node fails to start instead of silently
	// running with stale-zero state.
	if a.sidecarPath != "" {
		switch sc, err := ReadSidecar(a.sidecarPath); {
		case err == nil:
			a.stateCache.RefreshFromSidecar(sc)
		case IsNotExist(err):
			// Pre-bootstrap; leave atomics at zero.
		default:
			return nil, errors.Wrap(err, "encryption: NewApplier: prime in-memory state from sidecar")
		}
	}
	return a, nil
}

// StateCache returns the shared cache this Applier writes to on
// every apply path. main.go wires one StateCache across all
// per-shard Appliers via WithStateCache, but for callers that
// constructed an Applier without supplying one this accessor
// returns the privately-installed instance so tests can still
// reach the atomics directly.
func (a *Applier) StateCache() *StateCache { return a.stateCache }

// ActiveStorageKeyID delegates to the shared StateCache. Convenience
// for tests and single-applier callers; multi-shard wiring should
// prefer reading StateCache().ActiveStorageKeyID directly so the
// closure target is independent of which shard's Applier received
// the encryption apply.
func (a *Applier) ActiveStorageKeyID() (uint32, bool) {
	return a.stateCache.ActiveStorageKeyID()
}

// StorageEnvelopeActive delegates to the shared StateCache. Same
// rationale as ActiveStorageKeyID above.
func (a *Applier) StorageEnvelopeActive() bool {
	return a.stateCache.StorageEnvelopeActive()
}

// bootstrapAndRotationConfigured reports whether WithKEK,
// WithKeystore, and WithSidecarPath have all been supplied. The
// three are an indivisible quorum for ApplyBootstrap /
// ApplyRotation — KEK-unwrap without a keystore to install into
// would compute DEK bytes only to discard them; a keystore
// install without a sidecar write would not survive restart; a
// sidecar write without KEK would record wrapped DEKs the local
// node cannot decrypt. The check is on read so a partial wiring
// during a future refactor fails closed at apply time rather
// than mis-applying with one dep present.
func (a *Applier) bootstrapAndRotationConfigured() bool {
	return a.kek != nil && a.keystore != nil && a.sidecarPath != ""
}

// ApplyRegistration implements §4.1's writer-registry insert
// dispatch. The payload's (DEKID, FullNodeID, LocalEpoch) maps to
// the Pebble key RegistryKey(DEKID, uint16(FullNodeID)) and the
// value RegistryValue{FullNodeID, FirstSeenLocalEpoch,
// LastSeenLocalEpoch}.
//
// The four-case dispatch:
//
//   - case 1 (no existing row at this key): insert with
//     FirstSeen = LastSeen = payload.LocalEpoch.
//
//   - case 2 (existing row, same FullNodeID, payload.LocalEpoch >
//     existing.LastSeenLocalEpoch): update LastSeenLocalEpoch to
//     payload.LocalEpoch. FirstSeen is preserved as the original
//     first-registered value.
//
//   - case 2-idempotent (existing row, same FullNodeID,
//     payload.LocalEpoch == existing.LastSeenLocalEpoch): legitimate
//     Raft replay. Raft re-applies committed entries after restart
//     until the latest FSM snapshot, so a RegisterEncryptionWriter
//     entry can be applied again with the same
//     (dek_id, full_node_id, local_epoch). Returns nil with no row
//     change. Rejecting equal epochs as rollback would pin a node
//     in a halt-on-replay loop after any crash before snapshotting
//     this entry.
//
//   - case 3 (existing row, same FullNodeID, payload.LocalEpoch <
//     existing.LastSeenLocalEpoch — strictly less): epoch rollback.
//     Returns an error wrapped with ErrEncryptionApply so the kv
//     dispatch layer halts apply. The §9.1 ErrLocalEpochRollback
//     startup guard is what 6C ships to prevent this from being
//     reachable in production; until then the apply-time halt is
//     the load-bearing backstop.
//
//   - case 4 (existing row, DIFFERENT FullNodeID under the same
//     uint16 truncation): node-id collision per §6.1. Returns an
//     error wrapped with ErrEncryptionApply. The startup
//     ErrNodeIDCollision guard (6C) covers the cluster-wide check;
//     this apply-time halt is the per-node backstop.
//
// The fail-closed paths exist as defense-in-depth: PR760
// established that the gRPC-layer mutator gate
// (registerEncryptionAdminServer) is the primary safety boundary;
// the apply-time checks here exist so a malformed entry that
// somehow committed (e.g., during a future refactor that bypasses
// the gate, or in a forensic / corruption scenario) still halts
// rather than silently advancing setApplied.
func (a *Applier) ApplyRegistration(p fsmwire.RegistrationPayload) error {
	key := RegistryKey(p.DEKID, NodeID16(p.FullNodeID))
	existing, ok, err := a.registry.GetRegistryRow(key)
	if err != nil {
		return errors.Wrap(err, "applier: get registry row")
	}
	if !ok {
		// case 1: insert
		val := EncodeRegistryValue(RegistryValue{
			FullNodeID:          p.FullNodeID,
			FirstSeenLocalEpoch: p.LocalEpoch,
			LastSeenLocalEpoch:  p.LocalEpoch,
		})
		if err := a.registry.SetRegistryRow(key, val); err != nil {
			return errors.Wrap(err, "applier: insert registry row")
		}
		return nil
	}
	prev, err := DecodeRegistryValue(existing)
	if err != nil {
		return errors.Wrap(err, "applier: decode existing registry row")
	}
	if prev.FullNodeID != p.FullNodeID {
		// case 4: uint16 collision under different full ids
		return errors.Wrapf(ErrEncryptionApply,
			"applier: writer-registry uint16 collision at dek_id=%d (existing full_node_id=%#x, incoming=%#x)",
			p.DEKID, prev.FullNodeID, p.FullNodeID)
	}
	if p.LocalEpoch == prev.LastSeenLocalEpoch {
		// case 2-idempotent: legitimate Raft replay of an already-
		// applied registration entry. Raft replays committed entries
		// after restart until the latest FSM snapshot, so a
		// RegisterEncryptionWriter entry can be applied again with
		// the same (dek_id, full_node_id, local_epoch). Rejecting
		// equal epochs as rollback would halt apply on every legit
		// post-crash replay and pin the node in a restart loop.
		// The row already reflects this epoch — no-op return
		// preserves the §4.1 monotonicity invariant without
		// false-halting.
		return nil
	}
	if p.LocalEpoch < prev.LastSeenLocalEpoch {
		// case 3: epoch rollback / replay of a STALE epoch. Strictly
		// less-than is the rollback signal — see the equal-epoch
		// idempotent path above for why <= would be wrong.
		return errors.Wrapf(ErrEncryptionApply,
			"applier: writer-registry local_epoch rollback at dek_id=%d full_node_id=%#x (existing last_seen=%d, incoming=%d)",
			p.DEKID, p.FullNodeID, prev.LastSeenLocalEpoch, p.LocalEpoch)
	}
	// case 2: monotonic advance
	val := EncodeRegistryValue(RegistryValue{
		FullNodeID:          p.FullNodeID,
		FirstSeenLocalEpoch: prev.FirstSeenLocalEpoch,
		LastSeenLocalEpoch:  p.LocalEpoch,
	})
	if err := a.registry.SetRegistryRow(key, val); err != nil {
		return errors.Wrap(err, "applier: update registry row")
	}
	return nil
}

// ApplyBootstrap implements §5.6 step 1a's initial bootstrap apply:
//
//  1. KEK-unwrap the wrapped storage + raft DEK pair.
//  2. Install both into the in-memory Keystore.
//  3. Update the §5.1 sidecar — Active.{Storage,Raft} slots,
//     keys[] map for both DEK IDs — and crash-durably persist
//     via WriteSidecar.
//  4. Batch-insert every RegistrationPayload in BatchRegistry
//     as the cluster's initial writer-registry rows.
//
// Without the trio of WithKEK / WithKeystore / WithSidecarPath
// supplied at construction, returns ErrKEKNotConfigured wrapped
// for the HaltApply pipeline — the defense-in-depth marker that
// keeps the no-options posture consistent with the FSM contract.
//
// Ordering for crash recovery: Keystore.Set fires before
// WriteSidecar so an ErrKeyConflict from Set aborts the apply
// before any disk mutation. A crash before WriteSidecar loses
// the in-memory keystore on restart, but the entry stays in the
// Raft log unapplied — replay re-runs the full sequence. A crash
// after WriteSidecar but before batch insert is recovered by
// replay because §4.1 case-2-idempotent makes the per-row inserts
// no-op on the second pass.
//
// Keystore.Set is idempotent for matching DEK bytes (returns nil)
// and returns ErrKeyConflict only if the same key_id maps to
// different bytes — which means a buggy KEK-unwrap path produced
// different output for the same wrapped input. That's a halt
// condition; the wrapped output is propagated.
//
// Blocking behaviour: ApplyBootstrap performs synchronous IO for
// each step (KEK Unwrap may dial a KMS in production providers,
// WriteSidecar fsyncs to disk, every BatchRegistry row triggers a
// pebble.Sync). Stage 6A's main.go wiring invokes this from the
// FSM apply path, which is already serialised under the engine's
// applyMu, so the synchronous IO is part of the §6.3 contract —
// a slow Bootstrap blocks the apply loop until commit, which is
// the same shape every other 0x03/0x04/0x05 entry uses. The
// BatchRegistry insert is the most likely contributor to apply
// latency at scale (one pebble.Sync per cluster member); the
// §5.6 BootstrapBatchRowCap = 1<<14 keeps the worst case bounded.
// A future optimisation could replace the per-row Set with a
// pebble.Batch.Commit(pebble.Sync), but the current shape is
// correct and matches the Stage 6A ApplyRegistration semantics
// row-for-row.
func (a *Applier) ApplyBootstrap(raftIdx uint64, p fsmwire.BootstrapPayload) error {
	if err := a.validateBootstrap(p); err != nil {
		return err
	}
	storageDEK, err := a.kek.Unwrap(p.WrappedStorage)
	if err != nil {
		return errors.Wrap(err, "applier: kek-unwrap storage DEK")
	}
	raftDEK, err := a.kek.Unwrap(p.WrappedRaft)
	if err != nil {
		return errors.Wrap(err, "applier: kek-unwrap raft DEK")
	}
	if err := a.keystore.Set(p.StorageDEKID, storageDEK); err != nil {
		return errors.Wrap(err, "applier: keystore set storage DEK")
	}
	if err := a.keystore.Set(p.RaftDEKID, raftDEK); err != nil {
		return errors.Wrap(err, "applier: keystore set raft DEK")
	}
	if err := a.writeBootstrapSidecar(raftIdx, p); err != nil {
		return err
	}
	for i, reg := range p.BatchRegistry {
		if err := a.ApplyRegistration(reg); err != nil {
			return errors.Wrapf(err, "applier: bootstrap batch registry insert at index %d (dek_id=%d, full_node_id=%#x)",
				i, reg.DEKID, reg.FullNodeID)
		}
	}
	return nil
}

// validateBootstrap runs the three input invariants the bootstrap
// dispatch enforces before any state mutation:
//
//  1. WithKEK + WithKeystore + WithSidecarPath all supplied.
//  2. StorageDEKID and RaftDEKID distinct — equal IDs would cause
//     the second sc.Keys[...] assignment in writeBootstrapSidecar
//     to overwrite the first, silently mis-labelling the lone
//     surviving key's purpose.
//  3. Every BatchRegistry row targets one of the two bootstrap
//     DEKs — a row targeting a foreign DEK would persist
//     writer-registry state for an unrelated key while the
//     bootstrap installs only the declared pair, silently
//     breaking the §4.1 first-writer invariant on the next
//     post-bootstrap write under that foreign DEK.
//
// Extracted from ApplyBootstrap so the dispatch hot path stays
// below the cyclomatic complexity budget.
func (a *Applier) validateBootstrap(p fsmwire.BootstrapPayload) error {
	if !a.bootstrapAndRotationConfigured() {
		return errors.Wrap(ErrKEKNotConfigured, "applier: bootstrap requires WithKEK + WithKeystore + WithSidecarPath")
	}
	if p.StorageDEKID == p.RaftDEKID {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: bootstrap requires distinct storage and raft DEK IDs (got %d for both)", p.StorageDEKID)
	}
	for i, reg := range p.BatchRegistry {
		if reg.DEKID != p.StorageDEKID && reg.DEKID != p.RaftDEKID {
			return errors.Wrapf(ErrEncryptionApply,
				"applier: bootstrap BatchRegistry[%d].dek_id=%d does not match storage=%d or raft=%d",
				i, reg.DEKID, p.StorageDEKID, p.RaftDEKID)
		}
	}
	return a.checkBootstrapIdempotency(p)
}

// checkBootstrapIdempotency enforces the §5.6 one-time-bootstrap
// invariant. A second committed bootstrap entry would re-point
// Active.{Storage,Raft} or mutate writer-registry state outside
// the rotation path, leaving key-lifecycle semantics inconsistent
// with the bootstrap/rotation contract.
//
// Raft replay of the SAME bootstrap entry (e.g., crash after
// bootstrap apply but before snapshot) is idempotent and allowed
// — Raft entry bytes are deterministic across replicas, so a
// legitimate replay has the same DEK IDs AND the same wrapped
// DEK material as the original apply. The check compares both:
//
//  1. DEK IDs must match Active.{Storage,Raft} (otherwise the
//     second bootstrap is rerouting active keys outside the
//     rotation path).
//  2. Wrapped DEK bytes must match sc.Keys[id].Wrapped
//     (otherwise the second bootstrap is installing DIFFERENT
//     key material under the same id — a divergent BatchRegistry
//     would necessarily reach apply through such a payload
//     because Raft determinism couples wrapped bytes to the
//     entire entry hash).
//
// If both DEK IDs and wrapped bytes match, the BatchRegistry
// rows must also match by Raft determinism — replay through
// ApplyRegistration is then safe via the §4.1 case-2-idempotent
// path.
//
// Split out from validateBootstrap so the dispatch path stays
// below the cyclop complexity budget.
func (a *Applier) checkBootstrapIdempotency(p fsmwire.BootstrapPayload) error {
	sc, err := ReadSidecar(a.sidecarPath)
	if err != nil && !IsNotExist(err) {
		return errors.Wrap(err, "applier: read sidecar for bootstrap idempotency check")
	}
	if sc == nil {
		return nil
	}
	if sc.Active.Storage == 0 && sc.Active.Raft == 0 {
		return nil
	}
	if sc.Active.Storage != p.StorageDEKID || sc.Active.Raft != p.RaftDEKID {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: cluster already bootstrapped (Active.Storage=%d, Active.Raft=%d); cannot re-bootstrap to (%d, %d) — use rotate-dek",
			sc.Active.Storage, sc.Active.Raft, p.StorageDEKID, p.RaftDEKID)
	}
	if err := a.checkBootstrapWrappedMatches(sc, p); err != nil {
		return err
	}
	return nil
}

// checkBootstrapWrappedMatches compares the payload's wrapped DEK
// bytes against the existing sidecar entries at the same DEK IDs.
// A mismatch indicates a second bootstrap entry committed with
// the same DEK IDs but different wrapped material — Raft
// determinism couples wrapped bytes to the entry hash, so
// matching wrapped bytes is the stable proof of a legitimate
// idempotent replay.
//
// Split out from checkBootstrapIdempotency to keep that helper
// under the cyclop complexity budget.
func (a *Applier) checkBootstrapWrappedMatches(sc *Sidecar, p fsmwire.BootstrapPayload) error {
	storageKey, hasStorage := sc.Keys[strconv.FormatUint(uint64(p.StorageDEKID), 10)]
	raftKey, hasRaft := sc.Keys[strconv.FormatUint(uint64(p.RaftDEKID), 10)]
	if !hasStorage || !hasRaft {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: sidecar inconsistent — Active matches (%d, %d) but keys map missing one or both entries",
			p.StorageDEKID, p.RaftDEKID)
	}
	if !bytes.Equal(storageKey.Wrapped, p.WrappedStorage) || !bytes.Equal(raftKey.Wrapped, p.WrappedRaft) {
		return errors.Wrap(ErrEncryptionApply,
			"applier: cluster already bootstrapped with different wrapped DEK material at the same DEK IDs — use rotate-dek")
	}
	return nil
}

// writeBootstrapSidecar reads the existing sidecar (or starts a
// fresh one if absent), sets Active.Storage / Active.Raft, inserts
// both new wrapped DEKs into the keys[] map under the storage /
// raft purposes, and crash-durably writes the result.
//
// LocalEpoch for the freshly-bootstrapped keys is 0 — bootstrap
// is the cluster's first registration under each DEK, so the
// §4.1 case 1 first-seen invariant holds and the registry batch
// inserts will record FirstSeen = LastSeen = 0 for the
// proposing node.
func (a *Applier) writeBootstrapSidecar(raftIdx uint64, p fsmwire.BootstrapPayload) error {
	sc, err := ReadSidecar(a.sidecarPath)
	if err != nil && !IsNotExist(err) {
		return errors.Wrap(err, "applier: read sidecar for bootstrap")
	}
	if sc == nil {
		sc = &Sidecar{Version: SidecarVersion, Keys: map[string]SidecarKey{}}
	}
	if sc.Keys == nil {
		sc.Keys = map[string]SidecarKey{}
	}
	sc.Active.Storage = p.StorageDEKID
	sc.Active.Raft = p.RaftDEKID
	advanceRaftAppliedIndex(sc, raftIdx)
	createdAt := a.now().UTC().Format(time.RFC3339)
	sc.Keys[strconv.FormatUint(uint64(p.StorageDEKID), 10)] = SidecarKey{
		Purpose:    SidecarPurposeStorage,
		Wrapped:    append([]byte(nil), p.WrappedStorage...),
		Created:    createdAt,
		LocalEpoch: 0,
	}
	sc.Keys[strconv.FormatUint(uint64(p.RaftDEKID), 10)] = SidecarKey{
		Purpose:    SidecarPurposeRaft,
		Wrapped:    append([]byte(nil), p.WrappedRaft...),
		Created:    createdAt,
		LocalEpoch: 0,
	}
	if err := WriteSidecar(a.sidecarPath, sc); err != nil {
		return errors.Wrap(err, "applier: write sidecar for bootstrap")
	}
	a.stateCache.RefreshFromSidecar(sc)
	return nil
}

// advanceRaftAppliedIndex sets sc.RaftAppliedIndex to raftIdx when
// raftIdx is non-zero AND strictly greater than the current value.
//
// Why monotonic-and-skip-zero (instead of unconditional assignment):
//
//   - Zero means "caller did not supply a Raft entry index" (the
//     raftengine.ApplyIndexAware seam was not wired, e.g. unit
//     tests that drive ApplyBootstrap directly without setting
//     pendingApplyIdx). Overwriting a previously-good index with
//     zero would silently regress the sidecar's freshness marker
//     and the §9.1 ErrSidecarBehindRaftLog guard would over-fire
//     on the next restart. Skip-on-zero preserves Stage-6A
//     behavior for those callers.
//
//   - Monotonic guards against a malformed replay (or buggy engine)
//     handing a strictly-lower index after a successful Bootstrap.
//     The sidecar must only ever record "the last Raft index we
//     have observed an encryption-relevant entry for"; once
//     advanced, regression is a divergence signal we choose to
//     swallow rather than HaltApply (the apply itself is still
//     correct; only the sidecar's freshness annotation would be
//     wrong, and the guard treats sidecar < engine as the harmless
//     direction — it just runs the scanner instead of fast-pass).
func advanceRaftAppliedIndex(sc *Sidecar, raftIdx uint64) {
	if raftIdx == 0 {
		return
	}
	if raftIdx > sc.RaftAppliedIndex {
		sc.RaftAppliedIndex = raftIdx
	}
}

// ApplyRotation implements §5.2 / §5.4 rotation apply. The sub-tag
// dispatches the entry to the per-variant handler:
//
//   - RotateSubRotateDEK — install a new wrapped DEK and re-point
//     the Active slot for its Purpose (Stage 6B-1).
//   - RotateSubEnableStorageEnvelope — one-shot storage-layer
//     cutover (Stage 6D-4). Flips StorageEnvelopeActive and records
//     StorageEnvelopeCutoverIndex inside a single sidecar fsync.
//   - RotateSubEnableRaftEnvelope — one-shot raft-layer cutover
//     (Stage 6E-1). Records RaftEnvelopeCutoverIndex inside a
//     single sidecar fsync. The engine apply-hook installed by
//     6E-2 dispatches `entry.Index > sidecar.RaftEnvelopeCutoverIndex`
//     through the unwrap path; strict `>` makes the cutover entry
//     itself (at index == cutover) flow through unwrap-free, which
//     is the chicken/egg bootstrap.
//
// Other sub-tags (rewrap-deks, retire-dek) land in later stages
// and return ErrEncryptionApply so the HaltApply seam fires on an
// unrecognised sub-tag rather than silently advancing setApplied.
//
// Same WithKEK / WithKeystore / WithSidecarPath trio requirement
// as ApplyBootstrap; partial wiring returns ErrKEKNotConfigured
// at apply time.
func (a *Applier) ApplyRotation(raftIdx uint64, p fsmwire.RotationPayload) error {
	if !a.bootstrapAndRotationConfigured() {
		return errors.Wrap(ErrKEKNotConfigured, "applier: rotation requires WithKEK + WithKeystore + WithSidecarPath")
	}
	switch p.SubTag {
	case fsmwire.RotateSubRotateDEK:
		return a.applyRotateDEK(raftIdx, p)
	case fsmwire.RotateSubEnableStorageEnvelope:
		return a.applyEnableStorageEnvelope(raftIdx, p)
	case fsmwire.RotateSubEnableRaftEnvelope:
		return a.applyEnableRaftEnvelope(raftIdx, p)
	default:
		return errors.Wrapf(ErrEncryptionApply,
			"applier: rotation sub_tag %#x not recognised", p.SubTag)
	}
}

// applyRotateDEK handles the RotateSubRotateDEK variant:
//
//  1. KEK-unwrap the proposed wrapped DEK.
//  2. Install it into the keystore under p.DEKID.
//  3. Update the §5.1 sidecar — Active slot for the supplied
//     Purpose, keys[] map for the new DEK ID — and crash-durably
//     persist.
//  4. Insert the proposing node's ProposerRegistration row so the
//     §4.1 case-2 monotonicity check covers its first encrypted
//     write under the new DEK.
func (a *Applier) applyRotateDEK(raftIdx uint64, p fsmwire.RotationPayload) error {
	// The proposer-registration row MUST target the rotated DEK ID
	// — that is the whole point of including it atomically with the
	// rotate-dek entry (§5.2): cover the proposer's first encrypted
	// write under the NEW DEK with a §4.1 case-1 first-seen row.
	// Accepting a different DEK ID would mutate writer-registry
	// state for an unrelated DEK while leaving the rotated DEK
	// unregistered, silently breaking the rotate-dek invariant.
	if p.ProposerRegistration.DEKID != p.DEKID {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: rotation proposer_registration.dek_id=%d does not match rotation dek_id=%d",
			p.ProposerRegistration.DEKID, p.DEKID)
	}
	dek, err := a.kek.Unwrap(p.Wrapped)
	if err != nil {
		return errors.Wrap(err, "applier: kek-unwrap rotation DEK")
	}
	if err := a.keystore.Set(p.DEKID, dek); err != nil {
		return errors.Wrap(err, "applier: keystore set rotation DEK")
	}
	if err := a.writeRotationSidecar(raftIdx, p); err != nil {
		return err
	}
	if err := a.ApplyRegistration(p.ProposerRegistration); err != nil {
		return errors.Wrap(err, "applier: rotation proposer-registration insert")
	}
	return nil
}

// applyEnableStorageEnvelope handles the
// RotateSubEnableStorageEnvelope variant (§2.1, §6.4): the one-shot
// storage-layer cutover. Defense-in-depth re-validates the four
// §2.1 constraints on the apply side (the cutover RPC mutator,
// shipping in 6D-6, validates the same set before propose).
//
// Outcomes and their FSM-level treatment:
//
//   - Malformed proposal (Purpose != PurposeStorage, len(Wrapped)
//     != 0, or ProposerRegistration.DEKID mismatch) — halt apply
//     via ErrEncryptionApply. These cannot arise from a healthy
//     propose path and indicate either a future refactor that
//     bypassed the mutator gate or a forensic-grade corruption.
//
//   - Stale DEKID (a RotateDEK raced between propose and apply, so
//     sidecar.Active.Storage has advanced past p.DEKID) — benign
//     no-op. The entry is consumed (RaftAppliedIndex advances) so
//     it is not replayed, but StorageEnvelopeActive and
//     StorageEnvelopeCutoverIndex are NOT touched. The §2.1
//     constraint #3 race posture explicitly forbids halting on
//     this case: it is a legitimate admin race against RotateDEK,
//     not a malformed entry. 6D-6 will distinguish "fresh success"
//     from "stale-DEKID no-op" via a §6.4 response-detail
//     ride-along; for 6D-4 the FSM-level test asserts via the
//     post-apply sidecar (StorageEnvelopeActive still false).
//
//   - Already active (a duplicate cutover entry committed) —
//     idempotent. RaftAppliedIndex advances; StorageEnvelopeActive
//     stays true; StorageEnvelopeCutoverIndex is NOT overwritten
//     (the original first-apply value is the stable idempotency
//     token per §6.4). The 6D-6 retry path reads
//     sc.StorageEnvelopeCutoverIndex directly and surfaces it as
//     `applied_index` with `was_already_active = true`.
//
//   - Fresh success — sidecar.StorageEnvelopeActive flips to true,
//     sidecar.StorageEnvelopeCutoverIndex is set to raftIdx, and
//     RaftAppliedIndex advances. All three writes land inside a
//     single crash-durable WriteSidecar fsync (§6.4 atomicity).
//     The ProposerRegistration row is then inserted via the
//     standard §4.1 case 1 / case 2 dispatch so the proposer's
//     first encrypted write under the now-active envelope is
//     covered.
//
// 6D-4 deliberately does NOT wire the §6.2 storage-layer toggle —
// PutAt continues to read pre-cutover until 6D-5 lands the
// `StorageEnvelopeActive` consult. The cutover apply is
// operator-inert at this stage.
// validateEnableStorageEnvelopePayload enforces the §2.1
// payload-shape constraints that do NOT require sidecar state
// (purpose, empty-Wrapped, proposer DEK pinning). Sidecar-derived
// constraints (#3 stale DEKID, #4 idempotency) live in the caller
// so the no-op vs. fresh-success branches can read the sidecar
// exactly once.
func validateEnableStorageEnvelopePayload(p fsmwire.RotationPayload) error {
	// §2.1 constraint #1.
	if p.Purpose != fsmwire.PurposeStorage {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: enable-storage-envelope must carry Purpose=PurposeStorage, got %#x", byte(p.Purpose))
	}
	// §2.1 constraint #2 — length-based, NOT `Wrapped == nil`. The
	// wire decoder materialises a zero-length payload as an
	// allocated empty slice (`[]byte{}`); a `== nil` check would
	// reject every valid cutover entry and halt-apply the cluster.
	if len(p.Wrapped) != 0 {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: enable-storage-envelope must carry empty Wrapped, got %d bytes", len(p.Wrapped))
	}
	// Pin the proposer registration to the cutover's DEK — symmetric
	// with the rotate-dek invariant. Mismatch on an empty-Wrapped
	// cutover would otherwise silently insert a writer-registry row
	// against an unrelated DEK.
	if p.ProposerRegistration.DEKID != p.DEKID {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: enable-storage-envelope proposer_registration.dek_id=%d does not match rotation dek_id=%d",
			p.ProposerRegistration.DEKID, p.DEKID)
	}
	return nil
}

func (a *Applier) applyEnableStorageEnvelope(raftIdx uint64, p fsmwire.RotationPayload) error {
	if err := validateEnableStorageEnvelopePayload(p); err != nil {
		return err
	}
	sc, err := ReadSidecar(a.sidecarPath)
	if err != nil {
		return errors.Wrap(err, "applier: read sidecar for enable-storage-envelope")
	}
	// §2.1 constraint #3 — DEKID stale at apply (RotateDEK raced).
	// Benign no-op: consume the entry without halting and without
	// flipping the cutover fields.
	if p.DEKID != sc.Active.Storage {
		advanceRaftAppliedIndex(sc, raftIdx)
		if err := WriteSidecar(a.sidecarPath, sc); err != nil {
			return errors.Wrap(err, "applier: write sidecar for stale-dekid cutover no-op")
		}
		return nil
	}
	// §2.1 constraint #4 — idempotency. Preserve the original
	// StorageEnvelopeCutoverIndex; only advance the generic
	// RaftAppliedIndex so the duplicate entry is not replayed.
	//
	// Why this branch can safely skip ApplyRegistration: the
	// fresh-success branch below runs ApplyRegistration BEFORE
	// WriteSidecar, so the invariant
	//   sc.StorageEnvelopeActive == true ⇒ registration row already on disk
	// holds across crash-restart. No re-insert is needed (and the
	// duplicate entry's ProposerRegistration field is intentionally
	// discarded — §2.1 #4). This addresses Codex P1 on PR #804,
	// which was filed against commit 3eb4555c77 before the
	// registration-before-sidecar reordering landed in 74a504c8.
	if sc.StorageEnvelopeActive {
		advanceRaftAppliedIndex(sc, raftIdx)
		if err := WriteSidecar(a.sidecarPath, sc); err != nil {
			return errors.Wrap(err, "applier: write sidecar for already-active cutover no-op")
		}
		return nil
	}
	// Fresh successful apply.
	//
	// Crash-recovery ordering: ApplyRegistration runs BEFORE
	// WriteSidecar. The cutover sidecar flip is the LAST observable
	// side effect of the apply. If the process crashes between the
	// registration insert and the sidecar write, on restart:
	//
	//   - The sidecar is still pre-cutover
	//     (StorageEnvelopeActive == false), so FSM replay re-enters
	//     this fresh-success branch rather than short-circuiting
	//     on the §2.1 #4 already-active no-op above.
	//   - ApplyRegistration re-runs with the same
	//     (DEKID, FullNodeID, LocalEpoch) and hits the §4.1
	//     case 2-idempotent path, returning nil without mutating
	//     the row.
	//   - WriteSidecar then lands cleanly.
	//
	// The reverse ordering (sidecar write first) would leave the
	// cluster in a state where StorageEnvelopeActive == true but
	// the proposer's writer-registry row is missing — the §5.2
	// startup guard would refuse boot, and FSM replay would
	// short-circuit on already-active and skip the registry insert
	// permanently. This was gemini-code-assist medium #1 on PR804.
	//
	// §6.4 atomicity still holds for the three sidecar fields:
	// `StorageEnvelopeActive`, `StorageEnvelopeCutoverIndex`, and
	// the `RaftAppliedIndex` bump all land inside one WriteSidecar
	// fsync.
	if err := a.ApplyRegistration(p.ProposerRegistration); err != nil {
		return errors.Wrap(err, "applier: cutover proposer-registration insert")
	}
	sc.StorageEnvelopeActive = true
	sc.StorageEnvelopeCutoverIndex = raftIdx
	advanceRaftAppliedIndex(sc, raftIdx)
	if err := WriteSidecar(a.sidecarPath, sc); err != nil {
		return errors.Wrap(err, "applier: write sidecar for cutover")
	}
	a.stateCache.RefreshFromSidecar(sc)
	return nil
}

// validateEnableRaftEnvelopePayload enforces the Stage 6E-1
// payload-shape constraints that do NOT require sidecar state
// (purpose, empty-Wrapped, proposer-DEK pinning). The shape
// mirrors validateEnableStorageEnvelopePayload but targets the
// raft slot:
//
//   - Purpose MUST be PurposeRaft
//   - Wrapped MUST be empty (length-based check, NOT == nil; the
//     wire decoder materialises zero-length payloads as
//     allocated empty slices)
//   - ProposerRegistration.DEKID MUST equal payload DEKID
//
// Sidecar-derived constraints (#3 stale DEKID, #4 idempotency)
// live in the caller so the no-op vs. fresh-success branches
// can read the sidecar exactly once.
func validateEnableRaftEnvelopePayload(p fsmwire.RotationPayload) error {
	if p.Purpose != fsmwire.PurposeRaft {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: enable-raft-envelope must carry Purpose=PurposeRaft, got %#x", byte(p.Purpose))
	}
	if len(p.Wrapped) != 0 {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: enable-raft-envelope must carry empty Wrapped, got %d bytes", len(p.Wrapped))
	}
	if p.ProposerRegistration.DEKID != p.DEKID {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: enable-raft-envelope proposer_registration.dek_id=%d does not match rotation dek_id=%d",
			p.ProposerRegistration.DEKID, p.DEKID)
	}
	return nil
}

// applyEnableRaftEnvelope handles the RotateSubEnableRaftEnvelope
// variant (Stage 6E-1 §3.1): the one-shot raft-layer cutover.
// Structural mirror of applyEnableStorageEnvelope; the differences
// from the storage variant are:
//
//   - Compares DEKID against sidecar.Active.Raft (not .Storage).
//   - Records the cutover via sidecar.RaftEnvelopeCutoverIndex (a
//     uint64 index — non-zero means "Phase-2 active"), as opposed
//     to a separate bool flag. The same field doubles as the
//     idempotency token for replay (constraint #4).
//   - The proposer-registration row registers against the active
//     raft DEK (PurposeRaft), not the storage DEK. The §4.1
//     writer-registry layout is per-(DEK_id, NodeID) so storage
//     and raft registrations are independent rows.
//
// Outcomes and FSM-level treatment match the storage variant.
// The CHECK ORDER differs from the storage variant though
// (codex P1 round-1 on PR944): the raft path tests
// RaftEnvelopeCutoverIndex != 0 BEFORE the stale-DEK check so
// FSM replay of the original cutover marker after a later
// RotateDEK lands on the already-active branch (which republishes
// the wrap closure via raftCutoverWrapInstaller) rather than the
// stale-DEK no-op (which does NOT install). The storage variant
// has no installer hook so the order swap is unnecessary there.
//
//   - Malformed payload — halt-apply via ErrEncryptionApply.
//   - Already active (duplicate cutover entry, OR FSM replay of
//     the original marker after RotateDEK advanced Active.Raft) —
//     idempotent; preserve original RaftEnvelopeCutoverIndex,
//     advance RaftAppliedIndex only, invoke installer with
//     sc.Active.Raft so the wrap is keyed to the current DEK on
//     every replica.
//   - Stale DEKID (RotateDEK raced between propose and apply AND
//     RaftEnvelopeCutoverIndex == 0, i.e. cutover never took
//     effect) — benign no-op, advance RaftAppliedIndex only.
//     Installer is NOT invoked: no cutover took effect, so
//     publishing a wrap closure would be incorrect.
//   - Fresh success — register proposer FIRST, then set
//     RaftEnvelopeCutoverIndex and advance RaftAppliedIndex
//     inside one WriteSidecar fsync. The registration-before-
//     sidecar ordering matches the storage variant's
//     crash-recovery invariant (§4.1 case 2-idempotent re-runs
//     are safe; the sidecar flip is the last observable
//     side-effect). Installer is invoked with sc.Active.Raft
//     after the sidecar write completes.
//
// Stage 6E-1 deliberately does NOT activate the §6.3 engine
// apply-hook unwrap or the coordinator wrap-on-propose switch
// — those land atomically in 6E-2. With only 6E-1 deployed, the
// sidecar cutover advances on apply but no entry is wrapped or
// unwrapped, so the apply is operator-inert (matches the design
// doc's "no behavior change" guarantee for 6E-1).
func (a *Applier) applyEnableRaftEnvelope(raftIdx uint64, p fsmwire.RotationPayload) error {
	// Fail-closed: RaftEnvelopeCutoverIndex != 0 is the sole
	// "Phase-2 active" sentinel for the raft variant (storage
	// variant uses a separate bool). A raftIdx of 0 — the "no
	// index supplied" sentinel from the index-aware apply seam —
	// would let the fresh-success branch register the proposer
	// while leaving RaftEnvelopeCutoverIndex at 0, so the cutover
	// would silently fail to activate AND a replay would re-enter
	// the fresh-success branch (the already-active short-circuit
	// only triggers on != 0). Reject before any sidecar mutation.
	if raftIdx == 0 {
		return errors.Wrap(ErrEncryptionApply,
			"applier: enable-raft-envelope requires a non-zero raft index")
	}
	if err := validateEnableRaftEnvelopePayload(p); err != nil {
		return err
	}
	sc, err := ReadSidecar(a.sidecarPath)
	if err != nil {
		return errors.Wrap(err, "applier: read sidecar for enable-raft-envelope")
	}
	// Stage 6E-1 constraint #4 (idempotency) — ordered BEFORE the
	// stale-DEK check so an FSM replay of the original cutover
	// marker after a successful cutover + later RotateDEK reaches
	// the already-active branch and republishes the wrap. With the
	// reverse order, a replayed payload whose p.DEKID predates the
	// rotation would be treated as a stale-DEK no-op, the wrap
	// would never be installed in this process, and a freshly-
	// elected leader on this node would admit cleartext writes
	// above the cutover index (codex P1 round-1 on PR944).
	//
	// The already-active branch preserves the original
	// RaftEnvelopeCutoverIndex; only RaftAppliedIndex advances so
	// the duplicate entry is not replayed again. Non-zero
	// RaftEnvelopeCutoverIndex IS the "already-active" signal (no
	// separate bool flag).
	if sc.RaftEnvelopeCutoverIndex != 0 {
		advanceRaftAppliedIndex(sc, raftIdx)
		if err := WriteSidecar(a.sidecarPath, sc); err != nil {
			return errors.Wrap(err, "applier: write sidecar for already-active raft-cutover no-op")
		}
		// Installer takes the CURRENT sc.Active.Raft, NOT the
		// replayed p.DEKID — the wrap closure must key to the
		// active DEK on this node so the §6.3 strict-`>` hook
		// unwraps with the same key on every replica (gemini
		// medium #1 on PR944).
		return a.invokeRaftCutoverWrapInstaller(sc.RaftEnvelopeCutoverIndex, sc.Active.Raft, "already-active replay")
	}
	// Stage 6E-1 constraint #3 — DEKID stale at apply (RotateDEK
	// raced between propose and apply, AND the cutover never
	// took effect — RaftEnvelopeCutoverIndex==0 is the gate above
	// that distinguishes the genuine race from a replay). Benign
	// no-op: consume the entry without halting and without
	// flipping the cutover field.
	if p.DEKID != sc.Active.Raft {
		advanceRaftAppliedIndex(sc, raftIdx)
		if err := WriteSidecar(a.sidecarPath, sc); err != nil {
			return errors.Wrap(err, "applier: write sidecar for stale-dekid raft-cutover no-op")
		}
		return nil
	}
	// Fresh successful apply. Crash-recovery ordering follows
	// the storage variant: ApplyRegistration runs BEFORE
	// WriteSidecar so the cutover sidecar write is the last
	// observable side effect. If the process crashes between the
	// registration insert and the sidecar write, on restart the
	// sidecar is still pre-cutover (RaftEnvelopeCutoverIndex ==
	// 0), FSM replay re-enters this branch, ApplyRegistration
	// re-runs and hits §4.1 case 2-idempotent (no-op), then
	// WriteSidecar lands cleanly.
	if err := a.ApplyRegistration(p.ProposerRegistration); err != nil {
		return errors.Wrap(err, "applier: raft-cutover proposer-registration insert")
	}
	sc.RaftEnvelopeCutoverIndex = raftIdx
	advanceRaftAppliedIndex(sc, raftIdx)
	if err := WriteSidecar(a.sidecarPath, sc); err != nil {
		return errors.Wrap(err, "applier: write sidecar for raft cutover")
	}
	a.stateCache.RefreshFromSidecar(sc)
	// Stage 6E-2e-1 BLOCKER (b) — publish the wrap closure on every
	// replica's local FSM apply so a follower that becomes leader
	// post-cutover already has wrap active without needing the
	// EnableRaftEnvelope handler to re-run. Without this hook, the
	// per-leader InstallWrap call in adapter/encryption_admin.go's
	// runRaftEnvelopeCutoverBarrier is the only path that installs
	// the wrap — a leader failover between cutover commit and
	// InstallWrap would let the new leader admit cleartext writes at
	// indexes > cutoverIdx and brick the §6.3 strict-`>` apply hook
	// cluster-wide (codex P1 round-3 on PR933).
	//
	// Ordered AFTER WriteSidecar so a crash between sidecar fsync
	// and installer invocation is recoverable: process restart sees
	// RaftEnvelopeCutoverIndex != 0 in the sidecar, the startup-time
	// install (6E-2e-3 main.go wiring) republishes the wrap, and
	// the next apply hits the already-active branch where the
	// installer is idempotent. The reverse ordering (installer
	// first) would leave the wrap published but the sidecar
	// pre-cutover on crash, breaking the equality the §6.3 hook
	// relies on cluster-wide.
	// Installer takes sc.Active.Raft (which equals p.DEKID here
	// because the stale-DEK check above passed) for documentation
	// clarity and to match the already-active branch's argument
	// shape (gemini medium #2 on PR944).
	return a.invokeRaftCutoverWrapInstaller(raftIdx, sc.Active.Raft, "fresh-success apply")
}

// invokeRaftCutoverWrapInstaller is the Stage 6E-2e-1 dispatch
// shared between fresh-success and already-active branches of
// applyEnableRaftEnvelope. nil installer is a no-op (preserves the
// pre-6E-2e-1 test surface); a non-nil installer's error is wrapped
// with the branch tag so operator logs distinguish a failure on
// fresh apply from one on FSM replay.
func (a *Applier) invokeRaftCutoverWrapInstaller(cutoverIdx uint64, activeRaftDEKID uint32, branchTag string) error {
	if a.raftCutoverWrapInstaller == nil {
		return nil
	}
	if err := a.raftCutoverWrapInstaller(cutoverIdx, activeRaftDEKID); err != nil {
		return errors.Wrapf(err, "applier: install raft-cutover wrap on %s", branchTag)
	}
	return nil
}

// writeRotationSidecar mutates the Active slot for the supplied
// Purpose and inserts the new wrapped DEK into the keys[] map at
// p.DEKID, then crash-durably writes. Existing keys[] entries are
// preserved (rotation does not retire old DEKs — that is a
// separate sub-tag in Stage 6E).
func (a *Applier) writeRotationSidecar(raftIdx uint64, p fsmwire.RotationPayload) error {
	sc, err := ReadSidecar(a.sidecarPath)
	if err != nil {
		return errors.Wrap(err, "applier: read sidecar for rotation")
	}
	if sc.Keys == nil {
		sc.Keys = map[string]SidecarKey{}
	}
	purpose, err := sidecarPurposeFor(p.Purpose)
	if err != nil {
		return err
	}
	// Stage 7b' §3.1: write THIS node's pinned storage local_epoch
	// into Keys[newDEK].LocalEpoch for storage rotations so the §9.1
	// startup guard on next restart sees a monotone advance from
	// prior.w.epoch → prior.w.epoch+1. Raft rotations continue to
	// write `LocalEpoch: 0` exactly as before (raft envelope support
	// isn't implemented yet; cross-applying the storage nonce
	// factory's epoch to a raft DEK would corrupt the raft counter
	// before raft envelope ships — 7b' §3.1.1).
	var keyLocalEpoch uint16
	switch p.Purpose {
	case fsmwire.PurposeStorage:
		sc.Active.Storage = p.DEKID
		keyLocalEpoch = a.localEpoch
	case fsmwire.PurposeRaft:
		sc.Active.Raft = p.DEKID
		keyLocalEpoch = 0
	}
	advanceRaftAppliedIndex(sc, raftIdx)
	sc.Keys[strconv.FormatUint(uint64(p.DEKID), 10)] = SidecarKey{
		Purpose:    purpose,
		Wrapped:    append([]byte(nil), p.Wrapped...),
		Created:    a.now().UTC().Format(time.RFC3339),
		LocalEpoch: keyLocalEpoch,
	}
	if err := WriteSidecar(a.sidecarPath, sc); err != nil {
		return errors.Wrap(err, "applier: write sidecar for rotation")
	}
	a.stateCache.RefreshFromSidecar(sc)
	return nil
}

// sidecarPurposeFor maps the fsmwire.Purpose enum to the
// sidecar's purpose string (SidecarPurposeStorage /
// SidecarPurposeRaft). An unrecognised purpose halts apply
// with ErrEncryptionApply so a future wire-format extension
// cannot silently mis-label sidecar entries.
func sidecarPurposeFor(p fsmwire.Purpose) (string, error) {
	switch p {
	case fsmwire.PurposeStorage:
		return SidecarPurposeStorage, nil
	case fsmwire.PurposeRaft:
		return SidecarPurposeRaft, nil
	default:
		return "", errors.Wrapf(ErrEncryptionApply,
			"applier: rotation purpose %#x not recognised", byte(p))
	}
}

// nodeIDMask narrows a uint64 full_node_id to its low 16 bits to
// match the §4.1 writer-registry key shape
// (`!encryption|writers|<dek_id>|<be2 uint16(node_id)>`). The
// mask happens to share the value 0xFFFF with the unrelated
// local_epoch field width, but the name distinguishes this
// FullNodeID-truncation use from the local_epoch masking in
// adapter/encryption_admin.go. Untyped so the cast at the call
// site is explicit (no implicit uint64 promotion).
const nodeIDMask = 0xFFFF
