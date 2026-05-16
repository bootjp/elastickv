package encryption

import (
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
	registry WriterRegistryStore
}

// NewApplier wires an Applier against the supplied registry store.
// Returns an error if registry is nil so misconfiguration is caught
// at construction time rather than at first apply (the panic site
// is much harder to map back to a "you forgot to wire X" diagnosis
// when it fires deep inside a Raft apply loop).
func NewApplier(registry WriterRegistryStore) (*Applier, error) {
	if registry == nil {
		return nil, errors.New("encryption: NewApplier: registry is nil")
	}
	return &Applier{registry: registry}, nil
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

// ApplyBootstrap returns ErrKEKNotConfigured as the Stage 6A
// defense-in-depth marker. Real bootstrap apply — KEK-unwrap the
// wrapped DEK pair, install both into the keystore, batch-insert
// every RegistrationPayload in BatchRegistry as the initial
// writer-registry rows, and crash-durably persist the sidecar
// Active.{Storage,Raft} + keys map — lands in Stage 6B alongside
// the KEK plumbing.
func (a *Applier) ApplyBootstrap(_ fsmwire.BootstrapPayload) error {
	return errors.Wrap(ErrKEKNotConfigured, "applier: bootstrap requires KEK unwrapper (Stage 6B)")
}

// ApplyRotation returns ErrKEKNotConfigured as the Stage 6A
// defense-in-depth marker. Real rotation apply — KEK-unwrap the
// proposed DEK, install it under the supplied DEKID, update the
// sidecar's Active slot for the given Purpose, insert the
// ProposerRegistration row, and crash-durably persist — lands in
// Stage 6B.
func (a *Applier) ApplyRotation(_ fsmwire.RotationPayload) error {
	return errors.Wrap(ErrKEKNotConfigured, "applier: rotation requires KEK unwrapper (Stage 6B)")
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
