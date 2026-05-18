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

// TestCheckLocalEpochRollback_NarrowingMatchesShippedConvention
// pins the load-bearing 16-bit narrowing: the primitive must
// look up the registry row at RegistryKey(dekID,
// uint16(full_node_id & 0xFFFF)), matching applier.go:230's
// shipped convention. A 6D-2 implementor that derives the
// registry key via xxhash (or any other 16-bit projection)
// would lookup at a different key and the no-row branch would
// fire spuriously, masking a real rollback.
//
// Verify by seeding the row at uint16-narrowed key and looking
// up with a full_node_id whose HIGH bits differ — narrowing
// must hit the same row.
func TestCheckLocalEpochRollback_NarrowingMatchesShippedConvention(t *testing.T) {
	t.Parallel()
	// Seed with full_node_id 0xDEADBEEF_0000AAAA (narrowed=0xAAAA).
	reg := seedRegistry(t, 0xDEADBEEF_0000AAAA, 50)
	// Look up with a different full_node_id that ALSO narrows
	// to 0xAAAA. Same registry row should be hit; sidecar=10 <
	// registry=50 should fire ErrLocalEpochRollback.
	err := encryption.CheckLocalEpochRollback(reg, 0xCAFEBABE_0000AAAA, 1, 10, false)
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("narrowing-match: want ErrLocalEpochRollback, got %v", err)
	}
}
