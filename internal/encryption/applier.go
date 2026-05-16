package encryption

import (
	"bytes"
	"strconv"
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
// The Applier carries no in-memory state of its own; all state
// lives in the supplied WriterRegistryStore. This keeps it safe
// to construct once at FSM startup and share across the lifetime
// of the process — no per-apply allocation, no locks, no leak
// path for stale state across snapshot restore.
type Applier struct {
	registry    WriterRegistryStore
	kek         KEKUnwrapper
	keystore    *Keystore
	sidecarPath string
	now         func() time.Time
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
	return a, nil
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
	key := RegistryKey(p.DEKID, uint16(p.FullNodeID&nodeIDMask)) //nolint:gosec // masked to 16 bits; G115 cannot trace the bitwise narrowing
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
func (a *Applier) ApplyBootstrap(p fsmwire.BootstrapPayload) error {
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
	if err := a.writeBootstrapSidecar(p); err != nil {
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
func (a *Applier) writeBootstrapSidecar(p fsmwire.BootstrapPayload) error {
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
	return nil
}

// ApplyRotation implements §5.2 / §5.4 rotation apply. Stage 6B-1
// handles only the RotateSubRotateDEK sub-tag (rotate the storage
// or raft DEK to a new key_id). Other sub-tags (rewrap-deks,
// retire-dek, enable-storage-envelope, enable-raft-envelope) land
// in later stages and return ErrEncryptionApply for now so the
// HaltApply seam fires on an unrecognised sub-tag rather than
// silently advancing setApplied.
//
// For RotateSubRotateDEK:
//
//  1. KEK-unwrap the proposed wrapped DEK.
//  2. Install it into the keystore under p.DEKID.
//  3. Update the §5.1 sidecar — Active slot for the supplied
//     Purpose, keys[] map for the new DEK ID — and crash-durably
//     persist.
//  4. Insert the proposing node's ProposerRegistration row so the
//     §4.1 case-2 monotonicity check covers its first encrypted
//     write under the new DEK.
//
// Same WithKEK / WithKeystore / WithSidecarPath trio requirement
// as ApplyBootstrap; partial wiring returns ErrKEKNotConfigured
// at apply time.
func (a *Applier) ApplyRotation(p fsmwire.RotationPayload) error {
	if !a.bootstrapAndRotationConfigured() {
		return errors.Wrap(ErrKEKNotConfigured, "applier: rotation requires WithKEK + WithKeystore + WithSidecarPath")
	}
	if p.SubTag != fsmwire.RotateSubRotateDEK {
		return errors.Wrapf(ErrEncryptionApply,
			"applier: rotation sub_tag %#x not implemented in Stage 6B-1", p.SubTag)
	}
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
	if err := a.writeRotationSidecar(p); err != nil {
		return err
	}
	if err := a.ApplyRegistration(p.ProposerRegistration); err != nil {
		return errors.Wrap(err, "applier: rotation proposer-registration insert")
	}
	return nil
}

// writeRotationSidecar mutates the Active slot for the supplied
// Purpose and inserts the new wrapped DEK into the keys[] map at
// p.DEKID, then crash-durably writes. Existing keys[] entries are
// preserved (rotation does not retire old DEKs — that is a
// separate sub-tag in Stage 6E).
func (a *Applier) writeRotationSidecar(p fsmwire.RotationPayload) error {
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
	switch p.Purpose {
	case fsmwire.PurposeStorage:
		sc.Active.Storage = p.DEKID
	case fsmwire.PurposeRaft:
		sc.Active.Raft = p.DEKID
	}
	sc.Keys[strconv.FormatUint(uint64(p.DEKID), 10)] = SidecarKey{
		Purpose:    purpose,
		Wrapped:    append([]byte(nil), p.Wrapped...),
		Created:    a.now().UTC().Format(time.RFC3339),
		LocalEpoch: 0,
	}
	if err := WriteSidecar(a.sidecarPath, sc); err != nil {
		return errors.Wrap(err, "applier: write sidecar for rotation")
	}
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
