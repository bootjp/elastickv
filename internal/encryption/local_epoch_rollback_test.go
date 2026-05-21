package encryption_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
)

// stubRegistryStore is an in-memory WriterRegistryStore for the
// rollback-guard tests. Keyed by string so the test can populate
// rows by RegistryKey output without exporting unexported state.
type stubRegistryStore struct {
	rows   map[string][]byte
	getErr error
}

func (s *stubRegistryStore) GetRegistryRow(key []byte) ([]byte, bool, error) {
	if s.getErr != nil {
		return nil, false, s.getErr
	}
	v, ok := s.rows[string(key)]
	return v, ok, nil
}

func (s *stubRegistryStore) SetRegistryRow(key []byte, value []byte) error {
	if s.rows == nil {
		s.rows = map[string][]byte{}
	}
	s.rows[string(key)] = value
	return nil
}

// seedRegistry constructs a stub registry pre-populated with one
// row at the registry key derived from `full` (narrowed to its
// low 16 bits) under DEK id 1. All current tests probe the same
// active-DEK because the rollback contract is per-DEK and the
// table-of-DEKs case is exercised by `applier_test.go`'s
// case-1/2/3 dispatch tests. Future tests that need a
// different DEK id should accept a dekID parameter — for now
// hardcoding keeps the test fixtures tight.
func seedRegistry(t *testing.T, full uint64, lastSeen uint16) *stubRegistryStore {
	t.Helper()
	const seededDEKID uint32 = 1
	reg := &stubRegistryStore{rows: map[string][]byte{}}
	key := encryption.RegistryKey(seededDEKID, uint16(full&0xFFFF)) //nolint:gosec // masked to 16 bits
	row := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID:          full,
		FirstSeenLocalEpoch: 0,
		LastSeenLocalEpoch:  lastSeen,
	})
	if err := reg.SetRegistryRow(key, row); err != nil {
		t.Fatalf("seedRegistry: %v", err)
	}
	return reg
}

// TestCheckLocalEpochRollback_NoRow_PreCutover pins the
// freshly-joined-learner skip per §5.2 of the 6D design doc:
// with storageEnvelopeActive=false (pre-cutover) AND no
// registry row, the primitive returns nil. The §4.1 case-1
// first-seen branch will create the row on the next
// encrypted write.
func TestCheckLocalEpochRollback_NoRow_PreCutover(t *testing.T) {
	t.Parallel()
	reg := &stubRegistryStore{rows: map[string][]byte{}}
	err := encryption.CheckLocalEpochRollback(reg, 0xAAAA, 1, 42, false)
	if err != nil {
		t.Errorf("no-row + pre-cutover: want nil, got %v", err)
	}
}

// TestCheckLocalEpochRollback_NoRow_PostCutover pins the
// post-cutover refusal per §5.2 of the 6D design doc: with
// storageEnvelopeActive=true (post-cutover) AND no registry
// row, encrypted writes are happening cluster-wide but this
// node has no rollback anchor to compare its sidecar against.
// The primitive returns ErrLocalEpochRollback wrapped with a
// missing-row diagnostic.
//
// Codex r1 P2 specifically flagged that collapsing the
// missing-row case to nil unconditionally would let a node
// start without a nonce-reuse guardrail when the cutover is
// active. This test pins the fix.
func TestCheckLocalEpochRollback_NoRow_PostCutover(t *testing.T) {
	t.Parallel()
	reg := &stubRegistryStore{rows: map[string][]byte{}}
	err := encryption.CheckLocalEpochRollback(reg, 0xAAAA, 1, 42, true)
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("no-row + post-cutover: want ErrLocalEpochRollback, got %v", err)
	}
	if !strings.Contains(err.Error(), "storage_envelope_active=true") {
		t.Errorf("error must mention storage_envelope_active=true context: %v", err)
	}
}

// TestCheckLocalEpochRollback_SidecarStrictlyAhead pins the
// healthy case: sidecar > registry (the strict-ahead invariant)
// returns nil.
func TestCheckLocalEpochRollback_SidecarStrictlyAhead(t *testing.T) {
	t.Parallel()
	reg := seedRegistry(t, 0xAAAA, 10)
	if err := encryption.CheckLocalEpochRollback(reg, 0xAAAA, 1, 11, false); err != nil {
		t.Errorf("sidecar=11 > registry=10: want nil, got %v", err)
	}
}

// TestCheckLocalEpochRollback_SidecarLessThan pins the classic
// rollback case (sidecar restored from old backup): sidecar <
// registry returns ErrLocalEpochRollback.
func TestCheckLocalEpochRollback_SidecarLessThan(t *testing.T) {
	t.Parallel()
	reg := seedRegistry(t, 0xAAAA, 42)
	err := encryption.CheckLocalEpochRollback(reg, 0xAAAA, 1, 10, false)
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("sidecar=10 < registry=42: want ErrLocalEpochRollback, got %v", err)
	}
}

// TestCheckLocalEpochRollback_SidecarEqual pins the equality
// case: the strict-ahead invariant requires sidecar > registry,
// so equality is also a rollback (the SAME (node_id,
// local_epoch) prefix would be replayed under the same DEK).
// This is the case codex r6 flagged as a load-bearing edge.
func TestCheckLocalEpochRollback_SidecarEqual(t *testing.T) {
	t.Parallel()
	reg := seedRegistry(t, 0xAAAA, 42)
	err := encryption.CheckLocalEpochRollback(reg, 0xAAAA, 1, 42, false)
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("sidecar=42 == registry=42: want ErrLocalEpochRollback, got %v", err)
	}
}

// TestCheckLocalEpochRollback_PebbleError pins the I/O-error
// propagation path: a Pebble error from GetRegistryRow MUST NOT
// be classified as ErrLocalEpochRollback (the operator triages
// transport failure separately from a real rollback).
func TestCheckLocalEpochRollback_PebbleError(t *testing.T) {
	t.Parallel()
	pebbleErr := errors.New("simulated pebble corruption")
	reg := &stubRegistryStore{getErr: pebbleErr}
	err := encryption.CheckLocalEpochRollback(reg, 0xAAAA, 1, 10, false)
	if err == nil {
		t.Fatal("pebble error: want error, got nil")
	}
	if errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Errorf("pebble error must NOT be classified as ErrLocalEpochRollback: %v", err)
	}
	if !errors.Is(err, pebbleErr) {
		t.Errorf("underlying pebble error must be in chain: %v", err)
	}
}

// TestCheckLocalEpochRollback_ForeignRowPreCutover pins the
// defense-in-depth FullNodeID-mismatch case for pre-cutover:
// a registry row exists at the 16-bit-narrowed key but belongs
// to a DIFFERENT writer (historical occupant). Pre-cutover
// this is the freshly-joined-learner-with-historical-collision
// case — the next OpRegistration will overwrite the foreign
// row. Allow startup.
//
// Codex r3 P2 flagged that without this check, the comparison
// would happen against the FOREIGN writer's LastSeenLocalEpoch,
// potentially returning nil (sidecar > foreign) when in fact
// THIS node has no rollback anchor at all.
func TestCheckLocalEpochRollback_ForeignRowPreCutover(t *testing.T) {
	t.Parallel()
	// Seed with full_node_id 0xDEADBEEF_0000AAAA (narrowed=0xAAAA).
	reg := seedRegistry(t, 0xDEADBEEF_0000AAAA, 99)
	// Look up with a DIFFERENT full_node_id whose low 16 bits
	// ALSO narrow to 0xAAAA. Pre-cutover: the foreign row is
	// not THIS node's anchor; treat as no-row + allow startup.
	err := encryption.CheckLocalEpochRollback(reg, 0xCAFEBABE_0000AAAA, 1, 10, false)
	if err != nil {
		t.Errorf("foreign-row + pre-cutover: want nil, got %v", err)
	}
}

// TestCheckLocalEpochRollback_ForeignRowPostCutover pins the
// post-cutover refusal for the FullNodeID-mismatch case: a
// foreign row at the same 16-bit-narrowed key MUST NOT be
// treated as this node's rollback anchor. With
// storage_envelope_active=true, the absence of THIS node's
// own row means no rollback anchor exists and startup must
// refuse — same as the no-row + post-cutover case.
//
// Without the FullNodeID check, the function would compare
// sidecarLocalEpoch against the foreign writer's
// LastSeenLocalEpoch and (when sidecar happens to be ahead)
// return nil, allowing the node to start without an anchor.
func TestCheckLocalEpochRollback_ForeignRowPostCutover(t *testing.T) {
	t.Parallel()
	// Seed with full_node_id 0xDEADBEEF_0000AAAA (narrowed=0xAAAA),
	// large LastSeenLocalEpoch so sidecar=10 would be BELOW it
	// (the "natural" rollback scenario) — but the foreign-row
	// check should fire FIRST and route through the
	// missing-row-post-cutover branch.
	reg := seedRegistry(t, 0xDEADBEEF_0000AAAA, 99)
	err := encryption.CheckLocalEpochRollback(reg, 0xCAFEBABE_0000AAAA, 1, 10, true)
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("foreign-row + post-cutover: want ErrLocalEpochRollback, got %v", err)
	}
	if !strings.Contains(err.Error(), "storage_envelope_active=true") {
		t.Errorf("must use the missing-row-post-cutover message (storage_envelope_active=true): %v", err)
	}

	// Even when sidecar > foreign's LastSeenLocalEpoch (the
	// case that would have masked the missing anchor without
	// the FullNodeID check), the refusal must still fire.
	reg2 := seedRegistry(t, 0xDEADBEEF_0000AAAA, 5)
	err2 := encryption.CheckLocalEpochRollback(reg2, 0xCAFEBABE_0000AAAA, 1, 100, true)
	if !errors.Is(err2, encryption.ErrLocalEpochRollback) {
		t.Fatalf("foreign-row + post-cutover + sidecar>foreign.LastSeen: want ErrLocalEpochRollback, got %v", err2)
	}
}

// TestCheckLocalEpochRollback_NarrowingMatchesShippedConvention
// pins the load-bearing 16-bit narrowing: the primitive must
// look up the registry row at RegistryKey(dekID,
// uint16(full_node_id & 0xFFFF)), matching applier.go:230's
// shipped convention. A 6D-2 implementor that derives the
// registry key via xxhash (or any other 16-bit projection)
// would lookup at a different key and the no-row branch would
// fire spuriously, masking a real rollback.
//
// Verify by seeding the row at uint16-narrowed key with a
// full_node_id that has non-trivial high bits and looking up
// with the SAME full_node_id — confirming that the key
// derivation handles high-bit-aware values correctly. (The
// foreign-row case, where two distinct full_node_ids share the
// same 16-bit slice, is covered by the dedicated
// `ForeignRowPreCutover` / `ForeignRowPostCutover` tests
// above.)
func TestCheckLocalEpochRollback_NarrowingMatchesShippedConvention(t *testing.T) {
	t.Parallel()
	// Seed with full_node_id 0xDEADBEEF_0000AAAA (narrowed=0xAAAA).
	const fullID uint64 = 0xDEADBEEF_0000AAAA
	reg := seedRegistry(t, fullID, 50)
	// Look up with the SAME full_node_id. The key derivation
	// must use uint16(fullID & 0xFFFF) and find the row.
	// sidecar=10 < registry=50 fires ErrLocalEpochRollback.
	err := encryption.CheckLocalEpochRollback(reg, fullID, 1, 10, false)
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("narrowing-match: want ErrLocalEpochRollback, got %v", err)
	}
}
